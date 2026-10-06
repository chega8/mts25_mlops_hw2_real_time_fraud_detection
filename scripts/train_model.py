"""Offline only: train a compact CPU model on the competition CSV."""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import catboost
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import roc_auc_score, average_precision_score, precision_recall_curve, precision_score, recall_score, f1_score
from fraud_detector.src.preprocessing import prepare_features, FEATURES, CATEGORIES, SCHEMA_VERSION


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=Path, required=True)
    parser.add_argument('--out', type=Path, default=Path('fraud_detector/models'))
    args = parser.parse_args()
    data = pd.read_csv(args.train)
    if not set(data.target.unique()) <= {0,1} or data.target.isna().any():
        raise ValueError('target must be binary')
    times = pd.to_datetime(data.transaction_time, utc=True)
    # Use one temporal holdout, preserving equal timestamps on the same side.
    boundary = times.sort_values().iloc[int(len(data)*0.8)]
    train_mask = times < boundary
    valid_mask = ~train_mask
    features = prepare_features(data)
    for mask in [train_mask,valid_mask]:
        if data.loc[mask,'target'].nunique() != 2:
            raise ValueError('Both classes required in train and validation')
    model = CatBoostClassifier(iterations=250, depth=6, learning_rate=0.08,
        loss_function='Logloss', eval_metric='AUC', auto_class_weights='Balanced',
        random_seed=42, thread_count=4, task_type='CPU', allow_writing_files=False)
    model.fit(features[train_mask], data.target[train_mask], cat_features=CATEGORIES,
        eval_set=(features[valid_mask], data.target[valid_mask]),
        early_stopping_rounds=35, use_best_model=True, verbose=50)
    y = data.target[valid_mask]
    scores = model.predict_proba(features[valid_mask], task_type='CPU')[:,1]
    precision, recall, thresholds = precision_recall_curve(y, scores)
    f1 = 2*precision[:-1]*recall[:-1] / np.maximum(precision[:-1]+recall[:-1],1e-15)
    threshold = float(thresholds[np.argmax(f1)])
    flags = scores >= threshold
    args.out.mkdir(parents=True,exist_ok=True)
    path = args.out / 'my_catboost.cbm'
    model.save_model(str(path))
    metadata = dict(schema_version=SCHEMA_VERSION, features=FEATURES, categories=CATEGORIES,
        source='competition_train_csv', dataset_sha256=hashlib.sha256(args.train.read_bytes()).hexdigest(),
        model_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), threshold=threshold,
        training_rows=int(train_mask.sum()), validation_rows=int(valid_mask.sum()),
        training_fraud=int(data.target[train_mask].sum()), validation_fraud=int(y.sum()),
        temporal_boundary=boundary.isoformat(), trees=model.tree_count_,
        metrics=dict(roc_auc=float(roc_auc_score(y,scores)), average_precision=float(average_precision_score(y,scores)),
            precision=float(precision_score(y,flags)), recall=float(recall_score(y,flags)), f1=float(f1_score(y,flags))),
        versions=dict(python=platform.python_version(), catboost=catboost.__version__, pandas=pd.__version__, numpy=np.__version__),
        training_parameters=model.get_params(),
        note='Threshold and best iteration selected on the same temporal validation; no independent test labels.')
    (args.out / 'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    print(json.dumps(metadata,indent=2))

if __name__ == '__main__':
    main()
