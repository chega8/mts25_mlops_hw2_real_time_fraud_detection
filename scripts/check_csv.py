"""Offline compatibility check for the full real test.csv; no broker needed."""
import argparse
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd
from fraud_detector.src.preprocessing import prepare_features
from fraud_detector.src.scorer import Scorer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test',type=Path,required=True)
    parser.add_argument('--report',type=Path,default=Path('reports/test_csv_check.json'))
    args = parser.parse_args()
    scorer = Scorer()
    frame = pd.read_csv(args.test)
    scores = scorer.model.predict_proba(prepare_features(frame),task_type='CPU',thread_count=4)[:,1]
    if not np.isfinite(scores).all() or not ((scores>=0)&(scores<=1)).all():
        raise ValueError('Invalid model scores')
    report = dict(test_rows=len(frame),successfully_scored=len(scores),
        fraud_flags=int((scores>=scorer.threshold).sum()),min_score=float(scores.min()),
        max_score=float(scores.max()),threshold=scorer.threshold,
        test_sha256=hashlib.sha256(args.test.read_bytes()).hexdigest(),
        missing_values_by_column=frame.isna().sum().to_dict(),
        note='Offline compatibility/inference check; no test labels or Kafka/PostgreSQL used.')
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__ == '__main__':
    main()
