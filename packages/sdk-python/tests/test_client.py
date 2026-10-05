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

if __name__ == '__main__':
    unittest.main()
