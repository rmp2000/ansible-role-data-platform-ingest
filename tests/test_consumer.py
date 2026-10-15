import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock
import urllib.error

spec = importlib.util.spec_from_file_location('consumer', Path(__file__).parents[1]/'files/consumer.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)


class MemoryStorage(c.WebDAV):
    def __init__(self):
        self.base = 'https://storage/data/events'
        self.objects = {}
        self.operations = []
        self.corrupt = False

    def request(self, method, key, body=None, headers=None):
        self.operations.append((method, key))
        if method == 'GET':
            if key not in self.objects:
                raise urllib.error.HTTPError(key, 404, '', {}, None)
            return b'corrupt' if self.corrupt else self.objects[key]
        if method == 'PUT':
            self.objects[key] = body
        if method == 'MOVE':
            destination = headers['Destination'].rsplit('/', 1)[1]
            if headers['Overwrite'] != 'F' or destination in self.objects:
                raise AssertionError('Must never overwrite a completed event')
            self.objects[destination] = self.objects.pop(key)
        if method == 'DELETE':
            self.objects.pop(key, None)
        return b''


class ConsumerTests(unittest.TestCase):
    def test_staging_and_duplicate(self):
        storage = MemoryStorage()
        payload = b'\x00\xffbinary'
        storage.store('event.bin', payload)
        self.assertEqual(storage.objects, {'event.bin': payload})
        self.assertEqual([x[0] for x in storage.operations], ['GET', 'PUT', 'GET', 'MOVE', 'GET', 'DELETE'])
        storage.operations.clear()
        storage.store('event.bin', payload)
        self.assertEqual(storage.operations, [('GET', 'event.bin')])

    def test_conflicting_event_is_preserved(self):
        storage = MemoryStorage()
        storage.objects['event.bin'] = b'original'
        with self.assertRaises(c.ContentConflict):
            storage.store('event.bin', b'conflict')
        self.assertEqual(storage.objects, {'event.bin': b'original'})

    def test_corrupt_staging_never_creates_final_file(self):
        storage = MemoryStorage()
        storage.corrupt = True
        with self.assertRaises(c.ContentConflict):
            storage.store('event.bin', b'expected')
        self.assertEqual(storage.objects, {})

    def test_acknowledge_after_storage_success(self):
        order = []
        message = Mock()
        message.topic_name.return_value = 'persistent://platform/events/data-platform'
        message.properties.return_value = {'event_id': 'one'}
        message.message_id().serialize.return_value = b'id'
        store = Mock()
        store.store.side_effect = lambda *_: order.append('stored')
        broker = Mock()
        broker.acknowledge.side_effect = lambda *_: order.append('acknowledged')
        c.process_message(broker, message, store)
        self.assertEqual(order, ['stored', 'acknowledged'])
        broker.negative_acknowledge.assert_not_called()

    def test_failed_write_is_not_acknowledged(self):
        message = Mock()
        message.topic_name.return_value = 'topic'
        message.properties.return_value = {}
        message.message_id().serialize.return_value = b'id'
        store = Mock()
        store.store.side_effect = OSError('storage unavailable')
        broker = Mock()
        with self.assertRaises(OSError):
            c.process_message(broker, message, store)
        broker.acknowledge.assert_not_called()
        broker.negative_acknowledge.assert_called_once_with(message)

    def test_event_identity_is_stable_and_scoped(self):
        self.assertEqual(c.object_key('a', {'event_id': 'same'}, b'1'),
                         c.object_key('a', {'event_id': 'same'}, b'2'))
        self.assertNotEqual(c.object_key('a', {'event_id': 'same'}, b'1'),
                            c.object_key('b', {'event_id': 'same'}, b'1'))
        self.assertNotEqual(c.object_key('a', {}, b'1'), c.object_key('a', {}, b'2'))


if __name__ == '__main__':
    unittest.main()
