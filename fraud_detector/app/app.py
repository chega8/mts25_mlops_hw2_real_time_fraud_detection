import logging
from common.broker import consume, producer, publish
from fraud_detector.src.scorer import Scorer

def main():
    scorer = Scorer()
    output = producer()
    def handle(value):
        result = scorer.score(value)
        publish(output, 'scores', result, result['transaction_id'])
        logging.info('Scored transaction %s: score=%.6f fraud_flag=%s',
                     result['transaction_id'], result['score'], result['fraud_flag'])
    consume('transactions', 'fraud-detector-v2', handle)

if __name__ == '__main__':
    main()
