import json
import unittest
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd
from common.csv_input import load_csv, messages
from common.schema import validate_score
from fraud_detector.src.preprocessing import prepare_features, FEATURES
from fraud_detector.src.scorer import Scorer

RAW = Path('examples/test_sample.csv').read_bytes()

class ModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scorer = Scorer()
        cls.frame = load_csv(RAW)
        cls.batch = messages(cls.frame, RAW)

    def test_real_test_csv(self):
        features = prepare_features(self.frame)
        self.assertEqual(list(features), FEATURES)
        self.assertEqual(len(features), 100)
        self.assertTrue(np.isfinite(features.iloc[:,:8].to_numpy()).all())

    def test_actual_model_results(self):
        results = [self.scorer.score(item) for item in self.batch]
        for result in results:
            validate_score(result)
            self.assertEqual(result['fraud_flag'],int(result['score'] >= self.scorer.threshold))
        self.assertGreater(np.ptp([r['score'] for r in results]), 0.01)

    def test_target_not_used(self):
        original = self.batch[0]
        changed = dict(original, data=dict(original['data'], target=1, score=1, name_1='ignored'))
        self.assertEqual(self.scorer.score(original),self.scorer.score(changed))

    def test_missing_and_new_categories(self):
        value = dict(self.batch[0]['data'], merch='never_seen_merchant', gender=None, population_city=None, lat=None)
        result = self.scorer.score({'transaction_id':'new','data':value})
        validate_score(result)

    def test_invalid_data(self):
        for replacement in [{'amount':-1},{'lat':91},{'lon':181},{'amount':'inf'}, {'transaction_time':'bad'}, {'amount':None}]:
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                prepare_features([dict(self.batch[0]['data'],**replacement)])

    def test_stable_ids_and_target_removed(self):
        self.assertEqual(self.batch,messages(self.frame,RAW))
        self.assertNotEqual(self.batch[0]['transaction_id'],self.batch[1]['transaction_id'])
        changed = self.frame.copy()
        changed['target'] = 1
        self.assertNotIn('target',messages(changed,RAW)[0]['data'])

    def test_duplicate_ids(self):
        frame = self.frame.iloc[:2].copy()
        frame['transaction_id'] = 'same'
        with self.assertRaises(ValueError):
            messages(frame,RAW)

    def test_leading_zero_id(self):
        frame = self.frame.iloc[:1].copy()
        frame['transaction_id'] = '001'
        self.assertEqual(messages(frame,RAW)[0]['transaction_id'],'001')

    def test_threshold_override(self):
        with patch.dict('os.environ',{'FRAUD_THRESHOLD':'0'}):
            self.assertEqual(Scorer().score(self.batch[0])['fraud_flag'],1)
        with patch.dict('os.environ',{'FRAUD_THRESHOLD':'2'}):
            with self.assertRaises(ValueError):
                Scorer()

    def test_score_schema(self):
        for score in [float('nan'),float('inf'),-1,1.1,True,'0.5']:
            with self.assertRaises(ValueError):
                validate_score({'transaction_id':'x','score':score,'fraud_flag':0})

    def test_geo_distance(self):
        value = dict(self.batch[0]['data'],lat=0,lon=0,merchant_lat=0,merchant_lon=1)
        distance = np.expm1(prepare_features([value]).distance_log.iloc[0])
        self.assertAlmostEqual(distance,111.195,places=2)

class DeliveryTests(unittest.TestCase):
    def test_delivery_ack_required(self):
        from common.broker import publish
        class Producer:
            def produce(self,*args,**kwargs):
                self.callback = kwargs['on_delivery']
            def flush(self,timeout):
                self.callback(None,None)
                return 0
        publish(Producer(),'scores',{'score':0.5})
        class Failed(Producer):
            def flush(self,timeout):
                self.callback('unavailable',None)
                return 0
        with self.assertRaises(RuntimeError):
            publish(Failed(),'scores',{'score':0.5})

    def test_no_commit_on_infrastructure_failure(self):
        from common import broker
        events = []
        class Message:
            def error(self): return None
            def value(self): return b'{"transaction_id":"x"}'
        class Consumer:
            def __init__(self,config): self.polls = 0
            def subscribe(self,topics): pass
            def poll(self,timeout):
                self.polls += 1
                if self.polls == 1: return Message()
                raise RuntimeError('end')
            def commit(self,**kwargs): events.append('commit')
            def close(self): events.append('close')
        def failing(value): raise RuntimeError('database unavailable')
        with patch.object(broker,'Consumer',Consumer), patch.object(broker,'producer',lambda:None):
            with self.assertRaises(RuntimeError): broker.consume('scores','test',failing)
        self.assertEqual(events,['close'])

    def test_commit_after_handler(self):
        from common import broker
        events = []
        class Message:
            def error(self): return None
            def value(self): return b'{}'
        class Consumer:
            def __init__(self,config): self.polls = 0
            def subscribe(self,topics): pass
            def poll(self,timeout):
                self.polls += 1
                if self.polls == 1: return Message()
                raise RuntimeError('end')
            def commit(self,**kwargs): events.append('commit')
            def close(self): pass
        with patch.object(broker,'Consumer',Consumer), patch.object(broker,'producer',lambda:None):
            with self.assertRaises(RuntimeError): broker.consume('scores','test',lambda value:events.append('handler'))
        self.assertEqual(events,['handler','commit'])

if __name__ == '__main__':
    unittest.main()
