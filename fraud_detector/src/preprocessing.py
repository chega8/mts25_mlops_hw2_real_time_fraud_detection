"""Shared deterministic preprocessing for offline training and CPU inference."""
import numpy as np
import pandas as pd

CATEGORIES = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs']
NUMERIC = ['hour', 'year', 'month', 'day_of_month', 'day_of_week',
           'amount_log', 'population_city_log', 'distance_log']
FEATURES = NUMERIC + CATEGORIES
SCHEMA_VERSION = 1


def prepare_features(data):
    df = pd.DataFrame(data).copy()
    required = ['transaction_time', 'amount', 'population_city', 'lat', 'lon',
                'merchant_lat', 'merchant_lon'] + CATEGORIES
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f'Missing columns: {sorted(missing)}')
    dt = pd.to_datetime(df['transaction_time'], errors='raise', utc=True)
    if dt.isna().any():
        raise ValueError('Missing transaction_time')
    out = pd.DataFrame(index=df.index)
    for col, values in [('hour', dt.dt.hour), ('year', dt.dt.year),
                        ('month', dt.dt.month), ('day_of_month', dt.dt.day),
                        ('day_of_week', dt.dt.dayofweek)]:
        out[col] = values.astype(float)
    numbers = {}
    for col in required[1:7]:
        values = pd.to_numeric(df[col].replace('', np.nan), errors='raise').astype(float)
        if np.isinf(values).any():
            raise ValueError(f'Infinite {col}')
        numbers[col] = values
    if numbers['amount'].isna().any() or (numbers['amount'] < 0).any():
        raise ValueError('amount must be nonnegative and present')
    if (numbers['population_city'] < 0).any():
        raise ValueError('population_city must be nonnegative')
    for col, limit in [('lat',90), ('lon',180), ('merchant_lat',90), ('merchant_lon',180)]:
        if (numbers[col].abs() > limit).any():
            raise ValueError(f'Invalid {col}')
    lat1, lon1, lat2, lon2 = [np.deg2rad(numbers[c]) for c in ['lat','lon','merchant_lat','merchant_lon']]
    a = np.sin((lat2-lat1)/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin((lon2-lon1)/2)**2
    distance = 6371.009 * 2 * np.arcsin(np.sqrt(a.clip(0,1)))
    out['amount_log'] = np.log1p(numbers['amount'])
    out['population_city_log'] = np.log1p(numbers['population_city'])
    out['distance_log'] = np.log1p(distance)
    for col in CATEGORIES:
        out[col] = df[col].fillna('__missing__').astype(str).str.strip().replace('', '__missing__')
    return out[FEATURES]
