"""Pulsar to WebDAV ingestion: acknowledge only verified, atomically named files."""
import base64
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import ssl
import threading
import time
import urllib.error
import urllib.request
import uuid

CONFIG = Path('/etc/data-platform-ingest/config.json')
STATUS = Path('/var/lib/data-platform-ingest/status.json')
LOG = logging.getLogger('data-platform-ingest')


class ContentConflict(Exception):
    pass


def object_key(topic, properties, message_id):
    event = properties.get('event_id')
    identity = b'event:'+event.encode() if isinstance(event, str) and event else b'message:'+message_id
    return hashlib.sha256(topic.encode()+b'\0'+identity).hexdigest()+'.bin'


class WebDAV:
    def __init__(self, config):
        self.base = config['dcache_url'].rstrip('/')
        if not self.base.startswith('https://'):
            raise ValueError('Verified HTTPS storage is required')
        self.context = ssl.create_default_context(cafile=config['dcache_ca'])
        credential = (config['dcache_user']+':'+config['dcache_password']).encode()
        self.authorization = 'Basic '+base64.b64encode(credential).decode()

    def request(self, method, key, body=None, headers=None):
        values = {'Authorization': self.authorization}
        values.update(headers or {})
        req = urllib.request.Request(self.base+'/'+key, data=body, headers=values, method=method)
        with urllib.request.urlopen(req, context=self.context, timeout=20) as response:
            return response.read()

    def get(self, key):
        try:
            return self.request('GET', key)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            raise

    def delete(self, key):
        try:
            self.request('DELETE', key)
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise

    def store(self, key, payload):
        existing = self.get(key)
        if existing is not None:
            if existing != payload:
                raise ContentConflict('Existing event has different contents')
            return
        temporary = '.'+key+'.'+uuid.uuid4().hex+'.part'
        try:
            self.request('PUT', temporary, payload, {'Content-Type': 'application/octet-stream'})
            if self.get(temporary) != payload:
                raise ContentConflict('Staged bytes differ from the message')
            try:
                self.request('MOVE', temporary, headers={'Destination': self.base+'/'+key, 'Overwrite': 'F'})
            except urllib.error.HTTPError as error:
                if error.code != 412 or self.get(key) != payload:
                    raise
            if self.get(key) != payload:
                raise ContentConflict('Stored bytes differ from the message')
        finally:
            try:
                self.delete(temporary)
            except Exception:
                LOG.warning('temporary_cleanup_failed key=%s', key)


def process_message(consumer, message, storage):
    key = object_key(message.topic_name(), message.properties(), message.message_id().serialize())
    try:
        storage.store(key, message.data())
    except Exception:
        consumer.negative_acknowledge(message)
        raise
    consumer.acknowledge(message)
    return key


def open_client(config):
    import pulsar
    token = Path(config['pulsar_token_file']).read_text().strip()
    return pulsar.Client(config['broker_url'], authentication=pulsar.AuthenticationToken(token),
                         tls_trust_certs_file_path=config['pulsar_ca'],
                         tls_allow_insecure_connection=False, tls_validate_hostname=True,
                         operation_timeout_seconds=15, connection_timeout_ms=5000,
                         logger=logging.getLogger('pulsar-client'))


def write_status(status):
    status['updated'] = time.time()
    temporary = STATUS.with_name('.status-'+uuid.uuid4().hex)
    temporary.write_text(json.dumps(status))
    temporary.chmod(0o640)
    os.replace(temporary, STATUS)


def run():
    import pulsar
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s %(message)s')
    config = json.loads(CONFIG.read_text())
    storage = WebDAV(config)
    try:
        status = json.loads(STATUS.read_text())
    except (OSError, ValueError):
        status = {'processed': 0, 'failures': 0}
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    client = open_client(config)
    try:
        consumer = client.subscribe(config['topic'], config['subscription'],
                                    consumer_type=pulsar.ConsumerType.Exclusive,
                                    initial_position=pulsar.InitialPosition.Earliest,
                                    receiver_queue_size=100,
                                    negative_ack_redelivery_delay_ms=5000)
        status['state'] = 'ready'
        write_status(status)
        while not stop.is_set():
            try:
                message = consumer.receive(timeout_millis=1000)
            except pulsar.Timeout:
                continue
            key = object_key(message.topic_name(), message.properties(), message.message_id().serialize())
            try:
                process_message(consumer, message, storage)
            except Exception as error:
                status.update(state='retrying', last_failure_key=key,
                              failures=status.get('failures', 0)+1)
                write_status(status)
                LOG.warning('message_retry key=%s reason=%s', key, type(error).__name__)
                time.sleep(1)
                continue
            status.update(state='ready', last_key=key, processed=status.get('processed', 0)+1)
            write_status(status)
    finally:
        client.close()


if __name__ == '__main__':
    run()
