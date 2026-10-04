import argparse
import json
import os
from . import EventCore

def main():
    parser = argparse.ArgumentParser(prog='eventcore')
    parser.add_argument('--url', default=os.getenv('EVENTCORE_URL', 'http://localhost:8080'))
    parser.add_argument('--api-key', default=os.getenv('EVENTCORE_API_KEY'))
    commands = parser.add_subparsers(dest='command', required=True)
    topics = commands.add_parser('topics').add_subparsers(dest='operation', required=True)
    topics.add_parser('list')
    create = topics.add_parser('create')
    create.add_argument('name')
    create.add_argument('--partitions', type=int, default=4)
    inspect = commands.add_parser('topic')
    inspect.add_argument('operation', choices=['inspect'])
    inspect.add_argument('name')
    publish = commands.add_parser('publish')
    publish.add_argument('topic')
    publish.add_argument('--type', required=True)
    publish.add_argument('--data', required=True)
    publish.add_argument('--key', default='')
    consume = commands.add_parser('consume')
    consume.add_argument('topic')
    consume.add_argument('--group', required=True)
    consume.add_argument('--member', default='cli-' + str(os.getpid()))
    args = parser.parse_args()
    if not args.api_key:
        parser.error('EVENTCORE_API_KEY or --api-key required')
    client = EventCore(args.url, args.api_key)
    if args.command == 'topics':
        result = client.request('GET', '/v1/topics') if args.operation == 'list' else client.request(
            'POST', '/v1/topics', {'name': args.name, 'partitions': args.partitions})
    elif args.command == 'topic':
        result = client.request('GET', client.topic_path(args.name))
    elif args.command == 'publish':
        result = client.publish(args.topic, args.type, json.loads(args.data), args.key)
    else:
        def display(batch):
            for event in batch.events:
                print(json.dumps(event), flush=True)
            batch.ack()
        try:
            client.subscribe(args.topic, args.group, args.member, display)
        except KeyboardInterrupt:
            pass
        return
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()
