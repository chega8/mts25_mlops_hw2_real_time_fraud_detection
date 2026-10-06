import hashlib
import json
import os
from pathlib import Path
from catboost import CatBoostClassifier
from fraud_detector.src.preprocessing import prepare_features, FEATURES, CATEGORIES, SCHEMA_VERSION

MODEL_DIR = Path(__file__).resolve().parents[1] / 'models'

class Scorer:
    def __init__(self, model_dir=MODEL_DIR):
        model_dir = Path(model_dir)
        self.metadata = json.loads((model_dir / 'metadata.json').read_text())
        path = model_dir / 'my_catboost.cbm'
        if (self.metadata['features'] != FEATURES or self.metadata['categories'] != CATEGORIES
            or self.metadata['schema_version'] != SCHEMA_VERSION
            or self.metadata['model_sha256'] != hashlib.sha256(path.read_bytes()).hexdigest()):
            raise ValueError('Model and preprocessing artifacts do not match')
        self.model = CatBoostClassifier()
        self.model.load_model(str(path))
        if self.model.feature_names_ != FEATURES or list(self.model.classes_) != [0,1]:
            raise ValueError('Invalid model features/classes')
        self.threshold = float(os.getenv('FRAUD_THRESHOLD') or self.metadata['threshold'])
        if not 0 <= self.threshold <= 1:
            raise ValueError('Invalid FRAUD_THRESHOLD')

    def score(self, transaction):
        from common.schema import transaction_payload
        transaction_id, data = transaction_payload(transaction)
        features = prepare_features([data])
        score = float(self.model.predict_proba(features, task_type='CPU', thread_count=1)[0,1])
        return {'transaction_id': transaction_id, 'score': score,
                'fraud_flag': int(score >= self.threshold)}
