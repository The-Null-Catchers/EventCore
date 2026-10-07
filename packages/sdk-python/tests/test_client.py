import unittest
from unittest.mock import patch
import urllib.error
from eventcore import EventCore, Delivery

class ClientTests(unittest.TestCase):
    def test_publish_does_not_retry(self):
        client = EventCore('https://example.com', 'key')
        with patch('urllib.request.urlopen', side_effect=urllib.error.URLError('ambiguous')) as call:
            with self.assertRaises(urllib.error.URLError):
                client.publish('orders', 'created', {})
            self.assertEqual(call.call_count, 1)

    def test_delivery_commit_is_fenced(self):
        client = EventCore('https://example.com', 'key')
        delivery = Delivery(client, 'orders', 'billing', 'a',
                            {'partition': 0, 'epoch': 4, 'token': 'lease', 'events': []})
        with patch.object(client, 'request') as call:
            delivery.ack()
            self.assertEqual(call.call_args.args[2],
                             {'member': 'a', 'epoch': 4, 'partition': 0, 'token': 'lease'})

    def test_partial_commit_failure_is_not_retried(self):
        client = EventCore('https://example.com', 'key')
        delivery = Delivery(client, 'orders', 'billing', 'a',
                            {'partition': 0, 'epoch': 4, 'token': 'lease', 'events': []})
        with patch('urllib.request.urlopen', side_effect=urllib.error.URLError('ambiguous')) as call:
            with self.assertRaises(urllib.error.URLError):
                delivery.commit(3)
            self.assertEqual(call.call_count, 1)
            req = call.call_args.args[0]
            import json
            self.assertEqual(json.loads(req.data),
                             {'member': 'a', 'epoch': 4, 'partition': 0, 'token': 'lease', 'next_offset': 3})
            self.assertTrue(req.full_url.endswith('/commit'))

    def test_publish_passes_idempotency_key_without_retries(self):
        client = EventCore('https://example.com', 'key')
        with patch.object(client, 'request', return_value={'id': 'same', 'deduplicated': True}) as call:
            event = client.publish('orders', 'created', {'order': '123'}, idempotency_key='req-123')
            self.assertTrue(event['deduplicated'])
            self.assertEqual(call.call_count, 1)
            self.assertEqual(call.call_args.args[2]['idempotency_key'], 'req-123')

    def test_scan_and_replay_use_fixed_cursor(self):
        client = EventCore('https://example.com', 'key')
        with patch.object(client, 'request', return_value={'next_offset': 20}) as call:
            client.scan('orders', 1, 0, end_offset=50, type='order.created')
            self.assertIn('end_offset=50', call.call_args.args[1])
            client.replay('orders', 'archive', 'run-1', 1, 0, 50)
            self.assertEqual(call.call_args.args[2]['replay_id'], 'run-1')
            self.assertEqual(call.call_args.args[2]['end_offset'], 50)
            self.assertTrue(call.call_args.args[2]['confirm'])

    def test_replay_failure_exposes_progress_without_retry(self):
        import io
        from eventcore import EventCoreError
        client = EventCore('https://example.com', 'key')
        error = urllib.error.HTTPError('https://example.com', 503, 'failure', {},
                    io.BytesIO(b'{"error":"disk pressure","next_offset":6,"receipts":[],"done":false}'))
        with patch('urllib.request.urlopen', side_effect=error) as call:
            with self.assertRaises(EventCoreError) as raised:
                client.replay('orders', 'archive', 'run', 0, 5, 20)
            self.assertEqual(raised.exception.details['next_offset'], 6)
            self.assertEqual(call.call_count, 1)

if __name__ == '__main__':
    unittest.main()
