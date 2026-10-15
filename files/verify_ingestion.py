"""Root-only functional checks; credentials remain on the deployment VM."""
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import time
import urllib.request
import uuid

sys.path.insert(0, '/usr/local/lib/data-platform-ingest')
import consumer


def wait_for(test, label, timeout=180):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        try:
            value = test()
            if value:
                return value
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError('Timed out: '+label)


def service(name, action):
    subprocess.run(['systemctl', action, name], check=True, capture_output=True)


def main():
    config = json.loads(consumer.CONFIG.read_text())
    storage = consumer.WebDAV(config)
    token = Path('/etc/apache-pulsar/admin.token').read_text().strip()
    context = ssl.create_default_context(cafile=config['pulsar_ca'])
    topic_path = config['topic'].removeprefix('persistent://')

    def stats():
        req = urllib.request.Request(config['admin_url']+'/admin/v2/persistent/'+topic_path+'/stats',
                                     headers={'Authorization': 'Bearer '+token})
        with urllib.request.urlopen(req, context=context, timeout=10) as response:
            return json.load(response)['subscriptions'][config['subscription']]

    def status():
        return json.loads(consumer.STATUS.read_text())

    def publish(event, payload):
        client = consumer.open_client(config)
        try:
            producer = client.create_producer(config['topic'], batching_enabled=False)
            producer.send(payload, properties={'event_id': event})
        finally:
            client.close()

    def drained():
        value = stats()
        return value['msgBacklog'] == 0 and value.get('unackedMessages', 0) == 0

    def delivered(key, payload, previous):
        return (storage.get(key) == payload and status().get('processed', 0) > previous
                and status().get('last_key') == key and drained())

    wait_for(lambda: status()['state'] == 'ready' and stats(), 'consumer subscription')
    wait_for(drained, 'empty test subscription before checks')
    keys = []
    try:
        event = uuid.uuid4().hex
        payload = b'Data platform binary event\x00\xff'+os.urandom(1024)
        key = consumer.object_key(config['topic'], {'event_id': event}, b'')
        keys.append(key)
        previous = status().get('processed', 0)
        publish(event, payload)
        wait_for(lambda: delivered(key, payload, previous), 'verified bytes and acknowledgement')
        previous = status()['processed']
        publish(event, payload)
        wait_for(lambda: delivered(key, payload, previous), 'duplicate event uses the same object')
        print('DATA_PLATFORM_INGESTION_BYTES_AND_DUPLICATE_OK', flush=True)
        if sys.argv[1] != 'recovery':
            return

        # Only the bridge credential is changed; the storage server is untouched.
        original = consumer.CONFIG.read_bytes()
        metadata = consumer.CONFIG.stat()

        def replace_config(content):
            temporary = consumer.CONFIG.with_name('.recovery-'+uuid.uuid4().hex)
            temporary.write_bytes(content)
            os.chown(temporary, metadata.st_uid, metadata.st_gid)
            temporary.chmod(metadata.st_mode & 0o777)
            os.replace(temporary, consumer.CONFIG)

        event = uuid.uuid4().hex
        payload = b'Storage retry\x00'+os.urandom(256)
        key = consumer.object_key(config['topic'], {'event_id': event}, b'')
        keys.append(key)
        previous = status()['processed']
        failed_before = status().get('failures', 0)
        try:
            bad = dict(config, dcache_password='invalid-'+uuid.uuid4().hex)
            replace_config(json.dumps(bad).encode())
            service('data-platform-ingest', 'restart')
            publish(event, payload)
            wait_for(lambda: status().get('last_failure_key') == key
                     and status().get('failures', 0) > failed_before, 'failed write retried')
            if storage.get(key) is not None or stats()['msgBacklog'] < 1:
                raise RuntimeError('Failed storage write was not retained in Pulsar')
        finally:
            replace_config(original)
            service('data-platform-ingest', 'restart')
        wait_for(lambda: delivered(key, payload, previous), 'storage credential recovery')
        print('DATA_PLATFORM_STORAGE_RETRY_OK', flush=True)

        event = uuid.uuid4().hex
        payload = b'Broker recovery\x00'+os.urandom(256)
        key = consumer.object_key(config['topic'], {'event_id': event}, b'')
        keys.append(key)
        previous = status()['processed']
        service('data-platform-ingest', 'stop')
        try:
            publish(event, payload)
            wait_for(lambda: stats()['msgBacklog'] >= 1, 'pending durable event')
            service('apache-pulsar', 'restart')
            wait_for(lambda: stats()['msgBacklog'] >= 1, 'pending event after broker restart')
        finally:
            service('data-platform-ingest', 'start')
        wait_for(lambda: delivered(key, payload, previous), 'consumer delivery after broker restart')
        print('DATA_PLATFORM_BROKER_RESTART_FLOW_OK', flush=True)
    finally:
        # Never remove an object while its acknowledgement is still pending.
        if wait_for(drained, 'test messages acknowledged before cleanup'):
            for key in keys:
                storage.delete(key)
                if storage.get(key) is not None:
                    raise RuntimeError('Test object cleanup failed')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Do not expose HTTP authentication headers, configuration or JWTs.
        label = str(error) if type(error) is RuntimeError else type(error).__name__
        print('Integration verification failed: '+label, file=sys.stderr)
        raise SystemExit(1)
