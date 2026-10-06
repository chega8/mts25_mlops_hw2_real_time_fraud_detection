import os
import psycopg

INSERT = """INSERT INTO fraud_scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING"""

def connect():
    return psycopg.connect(host=os.getenv('POSTGRES_HOST','postgres'),
        dbname=os.getenv('POSTGRES_DB','fraud'), user=os.getenv('POSTGRES_USER','fraud'),
        password=os.getenv('POSTGRES_PASSWORD','fraud_local'), connect_timeout=10)


def results():
    with connect() as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        fraud = conn.execute('SELECT transaction_id, score, fraud_flag, created_at FROM fraud_scores WHERE fraud_flag=1 ORDER BY created_at DESC, id DESC LIMIT 10').fetchall()
        scores = conn.execute('SELECT score FROM fraud_scores ORDER BY created_at DESC, id DESC LIMIT 100').fetchall()
        total = conn.execute('SELECT count(*) FROM fraud_scores').fetchone()[0]
    return fraud, [row[0] for row in scores], total
