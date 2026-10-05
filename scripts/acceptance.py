"""Real SDK/HTTP/PostgreSQL/disk workflow. Run around a broker restart, see CI."""
import http.cookiejar
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from eventcore import EventCore, EventCoreError

class TestClient(EventCore):
    def request(self, method, path, body=None):
        for attempt in range(10):
            try:
                return super().request(method, path, body)
            except EventCoreError as error:
                # HTTP 429 here is rejection BEFORE execution, so this retry is safe.
                if error.status != 429 or attempt == 9:
                    raise
                time.sleep(1.05)

base = os.getenv('EVENTCORE_URL', 'http://localhost:8080')
phase = sys.argv[1]
state_path = '.acceptance.json'

def bootstrap():
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    def call(path, data, csrf=None):
        headers = {'Content-Type': 'application/json'}
        if csrf:
            headers['X-CSRF-Token'] = csrf
        with opener.open(urllib.request.Request(base + path, data=json.dumps(data).encode(),
                                               headers=headers), timeout=15) as response:
            return json.load(response)
    auth = call('/v1/auth/login', {'email': os.environ['BOOTSTRAP_EMAIL'],
                                'password': os.environ['BOOTSTRAP_PASSWORD'], 'workspace': 'demo'})
    return call('/v1/keys', {'name': 'acceptance', 'scopes': ['admin'],
                           'expires_at': (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()},
                auth['csrf_token'])['token']

def drain(client, group, member, epoch):
    ids = set()
    while True:
        batches = client.pull('orders', group, member, epoch, 100)
        if not batches:
            break
        for batch in batches:
            offsets = [event['offset'] for event in batch.events]
            assert offsets == list(range(offsets[0], offsets[0] + len(offsets)))
            for event in batch.events:
                assert event['id'] not in ids, 'unexpected duplicate during successful drain'
                ids.add(event['id'])
            batch.ack()
    return ids

if phase == 'before':
    token = bootstrap()
    client = TestClient(base, token)
    client.request('POST', '/v1/topics', {'name': 'orders', 'partitions': 4})
    client.request('POST', '/v1/topics/orders/groups', {'name': 'billing', 'start': 'earliest'})
    counts = [0] * 4
    last_key = {}
    ids = set()
    for start in range(0, 10000, 100):
        results = client.publish_batch('orders', [
            {'type': 'order.created', 'key': 'customer_' + str(i % 500), 'data': {'sequence': i}}
            for i in range(start, start + 100)])
        assert len(results) == 100
        for result in results:
            event = result['event']
            partition = event['partition']
            assert event['offset'] == counts[partition]
            counts[partition] += 1
            if event['key'] in last_key:
                previous = last_key[event['key']]
                assert previous[0] == partition and previous[1] < event['offset']
            last_key[event['key']] = (partition, event['offset'])
            ids.add(event['id'])
    assert len(ids) == 10000
    client.join('orders', 'billing', 'consumer-a')
    both = client.join('orders', 'billing', 'consumer-b')
    assert sum(both['lag'].values()) == 10000
    assignments = [set(member['partitions']) for member in both['members']]
    assert len(assignments) == 2 and not (assignments[0] & assignments[1])
    assert assignments[0] | assignments[1] == {0, 1, 2, 3}
    consumed = set()
    first = client.pull('orders', 'billing', 'consumer-a', both['epoch'], 100)
    for batch in first:
        prefix = batch.events[:5]
        batch.commit(prefix[-1]['offset'] + 1)
        consumed.update(event['id'] for event in prefix)
        try:
            batch.ack()
            raise AssertionError('settled token remained usable')
        except EventCoreError as error:
            assert error.status == 409
    partial = client.request('GET', '/v1/topics/orders/groups/billing')
    assert sum(partial['lag'].values()) == 10000 - len(consumed)
    client.request('POST', '/v1/topics/orders/groups/billing/leave', {'member': 'consumer-a'})
    reassigned = client.request('GET', '/v1/topics/orders/groups/billing')
    assert set(reassigned['members'][0]['partitions']) == {0, 1, 2, 3}
    consumed.update(drain(client, 'billing', 'consumer-b', reassigned['epoch']))
    assert consumed == ids
    client.request('POST', '/v1/topics/orders/groups/billing/leave', {'member': 'consumer-b'})
    final = client.request('GET', '/v1/topics/orders/groups/billing')
    assert sum(final['lag'].values()) == 0
    with open(state_path, 'w') as f:
        json.dump({'token': token, 'counts': counts, 'ids': sorted(ids)}, f)
    os.chmod(state_path, 0o600)
    print('PASS: 10,000 SDK events, key ordering, valid offsets, two members, rebalance, ACKs, zero lag')
elif phase == 'after':
    with open(state_path) as f:
        saved = json.load(f)
    client = TestClient(base, saved['token'])
    detail = client.request('GET', '/v1/topics/orders')
    assert [p['next_offset'] for p in detail['partitions']] == saved['counts']
    recovered = client.request('GET', '/v1/topics/orders/groups/billing')
    assert [recovered['offsets'][str(p)] for p in range(4)] == saved['counts']
    assert recovered['members'] == []
    event = client.publish('orders', 'order.created', {'sequence': 10000}, 'customer_0')
    assert event['offset'] == saved['counts'][event['partition']]
    for partition in range(4):
        client.request('POST', '/v1/topics/orders/groups/billing/reset',
                       {'partition': partition, 'offset': 0, 'confirm': True})
    joined = client.join('orders', 'billing', 'replay')
    replayed = drain(client, 'billing', 'replay', joined['epoch'])
    assert replayed == set(saved['ids']) | {event['id']}
    client.request('POST', '/v1/topics/orders/groups/billing/leave', {'member': 'replay'})
    print('PASS: broker restart preserved all events and commits, offsets continued, reset and replay of 10,001 events')
else:
    raise SystemExit('expected before or after')
