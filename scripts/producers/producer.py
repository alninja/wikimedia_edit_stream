import json
from requests_sse import EventSource
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

TOPIC = 'wiki_edits'
BOOTSTRAP_SERVERS = 'localhost:9092'

def create_topic_if_missing():
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=5).topics
    if TOPIC in existing:
        print(f"Topic '{TOPIC}' already exists.")
        return
    futures = admin.create_topics([
        NewTopic(TOPIC, num_partitions=3, replication_factor=1)
    ])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Delivered → partition [{msg.partition()}] offset {msg.offset()}')

def stream_wiki_events(producer):
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'  # fixed URL
    headers = {'User-Agent': 'DEZ-Project-Monitor/1.0 (Educational Project)'}

    print(f"Connecting to {url}...")

    with EventSource(url, headers=headers) as stream:
        print("Connected. Streaming events...")
        for event in stream:
            if event.type != 'message':
                continue
            try:
                change = json.loads(event.data)
            except ValueError:
                continue

            # discard canary events (wikimedia internal health checks)
            if change.get('meta', {}).get('domain') == 'canary':
                continue

            payload = {
                'id':        change.get('id'),
                'type':      change.get('type'),
                'user':      change.get('user'),
                'bot':       change.get('bot'),
                'title':     change.get('title'),
                'wiki':      change.get('wiki'),
                'timestamp': change.get('timestamp'),
                'domain':    change.get('meta', {}).get('domain'),
            }

            print(f"[{payload['type']}] {payload['user']} edited {payload['title']} ({payload['domain']})")

            producer.produce(
                TOPIC,
                key=str(payload.get('id', 'null')),
                value=json.dumps(payload),
                callback=delivery_report
            )
            producer.poll(1)

if __name__ == "__main__":
    create_topic_if_missing()
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS, 'client.id': 'wiki-producer'})
    try:
        stream_wiki_events(producer)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        print("Flushing remaining messages...")
        producer.flush()
        print("Done.")