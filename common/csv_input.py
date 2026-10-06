import hashlib
import io
import uuid
import pandas as pd
from common.schema import identifier
from fraud_detector.src.preprocessing import prepare_features

MAX_ROWS = 10000

def load_csv(raw):
    frame = pd.read_csv(io.BytesIO(raw), dtype=str, nrows=MAX_ROWS, encoding='utf-8-sig')
    if frame.empty:
        raise ValueError('Empty CSV')
    return frame


def messages(frame, raw):
    prepare_features(frame)
    digest = hashlib.sha256(raw).hexdigest()
    result, seen = [], set()
    for index, row in enumerate(frame.to_dict('records')):
        tid = identifier(row['transaction_id']) if 'transaction_id' in row else str(uuid.uuid5(uuid.NAMESPACE_URL, f'{digest}:{index}'))
        if tid in seen:
            raise ValueError('Duplicate transaction_id in CSV')
        seen.add(tid)
        data = {key: (None if pd.isna(value) else value) for key,value in row.items()
                if key not in {'transaction_id','target','is_fraud','score','fraud_flag'} and not key.startswith('Unnamed:')}
        result.append({'transaction_id': tid, 'data': data})
    return result
