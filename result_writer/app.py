import logging
from common.broker import consume
from common.database import connect, INSERT
from common.schema import validate_score

def main():
    with connect() as conn:
        def handle(value):
            params = validate_score(value)
            with conn.transaction():
                conn.execute(INSERT, params)
            logging.info('Saved or already present: %s', params[0])
        consume('scores', 'result-writer-v1', handle)

if __name__ == '__main__':
    main()
