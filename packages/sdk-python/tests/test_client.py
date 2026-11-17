import json
import unittest
from unittest.mock import Mock, patch
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from eventcore import EventCore, Delivery, EventCoreError

class ClientTests(unittest.TestCase):
    def test_subscription_still_recovers_pull_transport_failures(self):
        for error, joins in ((EventCoreError(409, 'stale epoch'), 2),
                             (EventCoreError(503, 'unavailable'), 1),
                             (urllib.error.URLError('offline'), 2),
                             (TimeoutError('pull timeout'), 2)):
            with self.subTest(error=error):
                client = EventCore('https://example.com', 'key')
                stopped = False
                delivery = object()
                def callback(item):
                    nonlocal stopped
                    self.assertIs(item, delivery)
                    stopped = True
                with patch.object(client, 'join', return_value={'epoch': 1}) as join, \
                     patch.object(client, 'pull', side_effect=[error, [delivery]]) as pull, \
                     patch.object(client, 'request') as leave, patch('time.sleep'):
                    client.subscribe('orders', 'billing', 'member', callback, stop=lambda: stopped)
                    self.assertEqual(pull.call_count, 2)
                    self.assertEqual(join.call_count, joins)
                    leave.assert_called_once_with('POST', '/v1/topics/orders/groups/billing/leave', {'member': 'member'})

    def test_subscription_callback_errors_are_not_retried_or_swallowed(self):
        for error in (EventCoreError(409, 'processing conflict'), EventCoreError(503, 'dependency down'),
                      urllib.error.URLError('callback network error'), TimeoutError('callback timeout')):
            with self.subTest(error=error):
                client = EventCore('https://example.com', 'key')
                callback = Mock(side_effect=error)
                with patch.object(client, 'join', return_value={'epoch': 1}) as join, \
                     patch.object(client, 'pull', return_value=[object()]) as pull, \
                     patch.object(client, 'request') as leave, patch('time.sleep'):
                    with self.assertRaises(type(error)) as raised:
                        client.subscribe('orders', 'billing', 'member', callback,
                                         stop=Mock(side_effect=[False, True]))
                    self.assertIs(raised.exception, error)
                    self.assertEqual(callback.call_count, 1)
                    self.assertEqual(pull.call_count, 1)
                    self.assertEqual(join.call_count, 1)
                    leave.assert_called_once_with('POST', '/v1/topics/orders/groups/billing/leave', {'member': 'member'})

    def test_subscription_cleanup_does_not_replace_processing_failure(self):
        client = EventCore('https://example.com', 'key')
        failure = RuntimeError('processing failed')
        with patch.object(client, 'join', return_value={'epoch': 1}), \
             patch.object(client, 'pull', return_value=[object()]), \
             patch.object(client, 'request', side_effect=OSError('leave failed')):
            with self.assertRaises(RuntimeError) as raised:
                client.subscribe('orders', 'billing', 'member', Mock(side_effect=failure))
            self.assertIs(raised.exception, failure)

    def test_subscription_cleanup_failure_is_reported_after_normal_stop(self):
        client = EventCore('https://example.com', 'key')
        with patch.object(client, 'join', return_value={'epoch': 1}), \
             patch.object(client, 'request', side_effect=OSError('leave failed')):
            with self.assertRaises(OSError):
                client.subscribe('orders', 'billing', 'member', Mock(), stop=lambda: True)

    def test_redirects_never_forward_credentials_or_replay_requests(self):
        received = []
        source_calls = []

        class Target(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append((self.command, self.headers.get('Authorization')))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{}')
            do_POST = do_GET
            def log_message(self, *args):
                pass

        target = ThreadingHTTPServer(('127.0.0.1', 0), Target)

        class Redirect(BaseHTTPRequestHandler):
            def do_GET(self):
                source_calls.append(self.command)
                self.send_response(int(self.path.rsplit('/', 1)[-1]))
                self.send_header('Location', f'http://127.0.0.1:{target.server_port}/target')
                self.end_headers()
            do_POST = do_GET
            def log_message(self, *args):
                pass

        source = ThreadingHTTPServer(('127.0.0.1', 0), Redirect)
        threads = [Thread(target=server.serve_forever, daemon=True) for server in (source, target)]
        for thread in threads:
            thread.start()
        try:
            client = EventCore(f'http://127.0.0.1:{source.server_port}', 'private-api-key')
            for method in ('GET', 'POST'):
                for status in (301, 302, 303, 307, 308):
                    with self.subTest(method=method, status=status):
                        with self.assertRaises(EventCoreError) as raised:
                            client.request(method, f'/redirect/{status}', {} if method == 'POST' else None)
                        self.assertEqual(raised.exception.status, status)
            self.assertEqual(received, [])
            self.assertEqual(len(source_calls), 10)
            direct = EventCore(f'http://127.0.0.1:{target.server_port}', 'private-api-key')
            self.assertEqual(direct.request('GET', '/target'), {})
            self.assertEqual(direct.request('POST', '/target', {}), {})
            self.assertEqual(received, [('GET', 'Bearer private-api-key'), ('POST', 'Bearer private-api-key')])
        finally:
            for server in (source, target):
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join()

    def test_publish_does_not_retry(self):
        client = EventCore('https://example.com', 'key')
        with patch.object(client._opener, 'open', side_effect=urllib.error.URLError('ambiguous')) as call:
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
        with patch.object(client._opener, 'open', side_effect=urllib.error.URLError('ambiguous')) as call:
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
        with patch.object(client._opener, 'open', side_effect=error) as call:
            with self.assertRaises(EventCoreError) as raised:
                client.replay('orders', 'archive', 'run', 0, 5, 20)
            self.assertEqual(raised.exception.details['next_offset'], 6)
            self.assertEqual(call.call_count, 1)

    def test_dead_letter_mutation_is_not_retried(self):
        client = EventCore('https://example.com', 'key')
        with patch.object(client._opener, 'open', side_effect=OSError('connection lost')) as call:
            with self.assertRaises(OSError):
                client.resolve_dead_letter('orders.DLQ', 0, 9, 'retry')
            self.assertEqual(call.call_count, 1)
            req = call.call_args.args[0]
            self.assertTrue(req.full_url.endswith('/dead-letters/retry'))
            self.assertEqual(json.loads(req.data), {'partition': 0, 'offset': 9, 'confirm': True})
        with self.assertRaises(ValueError):
            client.resolve_dead_letter('orders.DLQ', 0, 9, 'delete')

if __name__ == '__main__':
    unittest.main()
