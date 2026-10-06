"""Run inside interface container after docker compose up."""
import json
import time
import uuid
from pathlib import Path
from confluent_kafka import Consumer
from common.broker import BOOTSTRAP, producer, publish
from common.csv_input import load_csv, messages
from common.database import connect, results
from common.schema import validate_score
from fraud_detector.src.scorer import Scorer


def wait_for(predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.25)
    raise AssertionError('Timed out waiting for pipeline')


def main():
    prefix = 'smoke-' + uuid.uuid4().hex
    client = producer()
    reader = Consumer({'bootstrap.servers':BOOTSTRAP, 'group.id':prefix,
        'auto.offset.reset':'earliest', 'enable.auto.commit':False})
    reader.subscribe(['scores','transactions_dlq'])
    raw = Path('examples/test_sample.csv').read_bytes()
    batch = messages(load_csv(raw).iloc[:20], raw)
    for index, item in enumerate(batch):
        item['transaction_id'] = f'{prefix}-{index}'
    scorer = Scorer()
    expected = {item['transaction_id']:scorer.score(item) for item in batch}
    try:
        for item in batch:
            publish(client, 'transactions', item, item['transaction_id'])
        received = {}
        deadline = time.monotonic() + 90
        while len(received) < len(batch) and time.monotonic() < deadline:
            message = reader.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            value = json.loads(message.value())
            if message.topic() == 'scores' and value.get('transaction_id') in expected:
                validate_score(value)
                target = expected[value['transaction_id']]
                assert abs(value['score']-target['score']) < 1e-10
                assert value['fraud_flag'] == target['fraud_flag']
                received[value['transaction_id']] = value
        assert len(received) == len(batch), 'Missing Kafka scores'
        with connect() as conn:
            def stored():
                with conn.transaction():
                    return conn.execute('SELECT transaction_id,score,fraud_flag,created_at FROM fraud_scores WHERE transaction_id = ANY(%s)', (list(expected),)).fetchall()
            wait_for(lambda: len(stored()) == len(batch))
            rows = stored()
            for tid,score,flag,_ in rows:
                assert abs(score-expected[tid]['score']) < 1e-10
                assert flag == expected[tid]['fraud_flag']
            original = next(row for row in rows if row[0] == batch[0]['transaction_id'])
            # Send a duplicate score and a diagnostic marker with the same Kafka key.
            # Their order in the scores partition lets us verify writer consumed the duplicate.
            key = original[0]
            publish(client,'scores',expected[key],key)
            marker = prefix + '-writer-marker'
            publish(client,'scores',{'transaction_id':marker,'score':1.0,'fraud_flag':1},key)
            def marker_saved():
                with conn.transaction():
                    return conn.execute('SELECT 1 FROM fraud_scores WHERE transaction_id=%s',(marker,)).fetchone()
            wait_for(marker_saved)
            assert {r[0]:r for r in stored()} == {r[0]:r for r in rows}, 'Duplicate changed stored data'
            fraud, scores, total = results()
            assert any(row[0] == marker for row in fraud)
            assert 0 < len(scores) <= 100 and total >= len(batch)+1
        # A bad record and a following valid one share a partition.
        # Seeing the second score and the bad record DLQ proves processing continued.
        bad_id = prefix + '-bad'
        bad = {'transaction_id':bad_id,'data':{'amount':-1}}
        good = dict(batch[1], transaction_id=prefix+'-after-bad')
        bad_message = publish(client,'transactions',bad,bad_id)
        publish(client,'transactions',good,bad_id)
        dlq_seen, good_seen = False, False
        deadline = time.monotonic()+90
        while not (dlq_seen and good_seen) and time.monotonic() < deadline:
            message = reader.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            value = json.loads(message.value())
            if message.topic() == 'scores' and value.get('transaction_id') == good['transaction_id']:
                good_seen = True
            if message.topic() == 'transactions_dlq':
                dlq_seen = (value.get('source_partition') == bad_message.partition()
                    and value.get('source_offset') == bad_message.offset()) or dlq_seen
        assert good_seen and dlq_seen, 'DLQ/continuation failed'
        print(f'PASS: real CPU scores, PostgreSQL, duplicate, UI queries, DLQ. Prefix: {prefix}')
    finally:
        reader.close()

if __name__ == '__main__':
    main()
