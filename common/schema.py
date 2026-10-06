import math

def identifier(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError('transaction_id must be string or integer')
    value = str(value)
    if not value.strip() or len(value) > 128 or '\x00' in value:
        raise ValueError('Invalid transaction_id length')
    return value


def transaction_payload(value):
    if not isinstance(value, dict):
        raise ValueError('Expected JSON object')
    tid = identifier(value.get('transaction_id'))
    data = value.get('data', value)
    if not isinstance(data, dict):
        raise ValueError('data must be an object')
    return tid, data


def validate_score(value):
    if not isinstance(value, dict) or set(value) != {'transaction_id','score','fraud_flag'}:
        raise ValueError('Expected exactly three score fields')
    tid = identifier(value['transaction_id'])
    score = value['score']
    flag = value['fraud_flag']
    if isinstance(score, bool) or not isinstance(score, (int,float)) or not math.isfinite(score) or not 0 <= score <= 1:
        raise ValueError('Invalid score')
    if type(flag) is not int or flag not in (0,1):
        raise ValueError('Invalid fraud_flag')
    return tid, float(score), flag
