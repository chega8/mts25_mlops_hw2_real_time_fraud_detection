import argparse
from pathlib import Path
from common.csv_input import load_csv, messages
from common.broker import producer, publish

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', type=Path)
    parser.add_argument('--limit', type=int, default=100)
    args = parser.parse_args()
    if not 1 <= args.limit <= 10000:
        parser.error('limit must be between 1 and 10000')
    raw = args.csv.read_bytes()
    batch = messages(load_csv(raw).iloc[:args.limit], raw)
    client = producer()
    for item in batch:
        publish(client, 'transactions', item, item['transaction_id'])
    print(f'Sent {len(batch)} transactions')

if __name__ == '__main__':
    main()
