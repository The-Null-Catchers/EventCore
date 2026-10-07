"""Synchronous EventCore client. Publication is never retried implicitly."""
from __future__ import annotations
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable

class EventCoreError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status

@dataclass
class Delivery:
    client: EventCore
    topic: str
    group: str
    member: str
    batch: dict

    @property
    def events(self) -> list[dict]:
        return self.batch['events']

    def ack(self) -> None:
        self._settle('ack')

    def nack(self) -> None:
        self._settle('nack')

    def commit(self, next_offset: int) -> None:
        """Commit a processed prefix; the remaining events are delivered again."""
        self._settle('commit', {'next_offset': next_offset})

    def _settle(self, operation: str, extra: dict | None = None) -> None:
        self.client.request('POST', self.client.group_path(self.topic, self.group) + '/' + operation,
                            {'member': self.member, 'epoch': self.batch['epoch'],
                             'partition': self.batch['partition'], 'token': self.batch['token'], **(extra or {})})

class EventCore:
    def __init__(self, base_url: str, api_key: str, timeout: float = 10):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout

    def request(self, method: str, path: str, body: Any = None) -> Any:
        raw = None if body is None else json.dumps(body, separators=(',', ':')).encode()
        request = urllib.request.Request(self.base_url + path, data=raw, method=method,
                    headers={'Authorization': 'Bearer ' + self.api_key, 'Content-Type': 'application/json'})
        # GET is safe to retry. Mutations return ambiguous network failures to the caller.
        attempts = 3 if method == 'GET' else 1
        for attempt in range(attempts):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                payload = error.read(2 << 20)
                try:
                    message = json.loads(payload).get('error', str(error))
                except (ValueError, AttributeError):
                    message = str(error)
                if method == 'GET' and error.code in (429, 502, 503, 504) and attempt + 1 < attempts:
                    time.sleep(0.2 * 2 ** attempt)
                    continue
                raise EventCoreError(error.code, message) from error
            except (urllib.error.URLError, TimeoutError):
                if attempt + 1 == attempts:
                    raise
                time.sleep(0.2 * 2 ** attempt)

    @staticmethod
    def topic_path(topic: str) -> str:
        return '/v1/topics/' + urllib.parse.quote(topic, safe='')

    @classmethod
    def group_path(cls, topic: str, group: str) -> str:
        return cls.topic_path(topic) + '/groups/' + urllib.parse.quote(group, safe='')

    def publish(self, topic: str, event_type: str, data: Any, key: str = '', headers: dict | None = None, idempotency_key: str = '') -> dict:
        return self.request('POST', self.topic_path(topic) + '/events',
                            {'type': event_type, 'key': key, 'data': data, 'headers': headers or {}, 'idempotency_key': idempotency_key})

    def publish_batch(self, topic: str, events: list[dict]) -> list[dict]:
        """Each result contains event OR error. Batches are ordered, non-atomic."""
        return self.request('POST', self.topic_path(topic) + '/events/batch', {'events': events})['results']

    def join(self, topic: str, group: str, member: str) -> dict:
        return self.request('POST', self.group_path(topic, group) + '/join', {'member': member})

    def pull(self, topic: str, group: str, member: str, epoch: int, limit: int = 100) -> list[Delivery]:
        batches = self.request('POST', self.group_path(topic, group) + '/pull',
                               {'member': member, 'epoch': epoch, 'limit': limit})
        return [Delivery(self, topic, group, member, batch) for batch in batches]

    def subscribe(self, topic: str, group: str, member: str,
                  callback: Callable[[Delivery], None], stop: Callable[[], bool] = lambda: False) -> None:
        """Callback explicitly acknowledges whole batches after successful processing."""
        epoch = self.join(topic, group, member)['epoch']
        path = self.group_path(topic, group)
        try:
            backoff = 0.2
            while not stop():
                try:
                    batches = self.pull(topic, group, member, epoch)
                    backoff = 0.2
                    for delivery in batches:
                        callback(delivery)
                    if not batches:
                        time.sleep(0.2)
                except EventCoreError as error:
                    if error.status == 409:
                        epoch = self.join(topic, group, member)['epoch']
                    elif error.status in (429, 502, 503, 504):
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 5)
                    else:
                        raise
                except (urllib.error.URLError, TimeoutError):
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 5)
                    # Recover generation and pending leases without pretending ack succeeded.
                    epoch = self.join(topic, group, member)['epoch']
        finally:
            self.request('POST', path + '/leave', {'member': member})
