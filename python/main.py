import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta

class Item:
    def __init__(self, item_id, time):
        self.item_id = item_id
        self.time = time

    def __str__(self):
        return str((self.time, self.item_id))


class Sessions:
    def __init__(self, train_sessions, train_purchases):
        tmp = train_sessions \
            .groupby('session_id', group_keys=False) \
            .agg(pd.Series.tolist) \
            .merge(train_purchases, on='session_id') \
            .rename(columns={"item_id_x": "item_ids", "date_x": "times", "item_id_y": "target_item_id", "date_y": "target_time"})
        tmp['items'] = tmp.apply(lambda row: self._create_items(row), axis=1)
        tmp['time'] = tmp.apply(lambda row: row['items'][0].time, axis = 1)
        tmp['count'] = tmp.apply(lambda row: len(row['items']), axis = 1)
        tmp['duration'] = tmp.apply(lambda row: (row['items'][-1].time - row['items'][0].time), axis = 1)
        tmp['last_item'] = tmp.apply(lambda row: row['items'][-1], axis = 1)
        tmp['items'] = tmp.apply(lambda row: self._append(row['items'], Item(row['target_item_id'], row['target_time'])), axis = 1)

        tmp = tmp.drop(['item_ids', 'times'], axis=1)

        tmp = tmp.set_index(['session_id'])

        self.sessions = tmp

    def __str__(self):
        return str(self.sessions)

    def _create_items(self, row):
        item_ids = row['item_ids']
        times = row['times']

        items = [Item(item, time) for (time, item) in zip(times, item_ids)]
        items.sort(key=lambda x: x.time)

        return items

    def _append(self, items, item):
        items.append(item)
        return items


class ItemCache:
    def __init__(self, start, end, sess, items):
        item_sess = items \
                .merge(sess, on='session_id') \
                [['item_id', 'session_id', 'time', 'target_item_id', 'target_time', 'count', 'duration']]

        self.sess_features = item_sess.groupby('item_id', group_keys=True) \
                .agg({'session_id': pd.Series.nunique, 'count': pd.Series.sum, 'duration': pd.Series.sum})


        self.buy_features = item_sess[item_sess['item_id'] == item_sess['target_item_id']].groupby('item_id', group_keys=True) \
                .agg({'session_id': pd.Series.nunique, 'count': pd.Series.sum, 'duration': pd.Series.sum})

        self.candidates = sess['target_item_id'].unique()

class ItemFeatures:
    def __init__(self, feat):
        feat['fid'] = feat.apply(lambda row: str(row['feature_category_id']), axis=1)
        feat['fval'] = feat.apply(lambda row: str(row['feature_category_id']) + '-' + str(row['feature_value_id']), axis=1)
        fid_cols = feat[['item_id', 'fid']].rename(columns={"fid": "col"})
        fval_cols = feat[['item_id', 'fval']].rename(columns={"fval": "col"})
        fcols = pd.concat([fid_cols, fval_cols])

        self.features = fid_cols.groupby(['item_id', 'col']).size()

        cols = fcols['col'].unique()
        self.col_idx_map = pd.DataFrame({'col': cols, 'idx': pd.Series(range(0, len(cols)))})

        item_count = len(feat['item_id'].unique())
        self.sel_feat = self.features[self.features > item_count * 0.05]

train_sessions = pd.read_csv("/home/anandj/data/code/Layer/dressipi_recsys2022/train_sessions.csv", parse_dates=True)
train_purchases = pd.read_csv("/home/anandj/data/code/Layer/dressipi_recsys2022/train_purchases.csv", parse_dates=True)
item_features = pd.read_csv("/home/anandj/data/code/Layer/dressipi_recsys2022/item_features.csv")
train_sessions['date'] = pd.to_datetime(train_sessions['date'])
train_purchases['date'] = pd.to_datetime(train_purchases['date'])

ifeat = ItemFeatures(item_features)

sessions = Sessions(train_sessions, train_purchases)

start_month = pd.to_datetime("2020-01-01 00:00:00")
month = relativedelta(months=1)
end_month = pd.to_datetime("2021-06-01 00:00:00")
cur_month = start_month

while cur_month < end_month:
    start = cur_month
    end = cur_month + month
    wsess = sessions.sessions[sessions.sessions['time'].between(start, end, 'left')]
    witem = train_sessions[train_sessions['date'].between(start, end, 'left')]
    icache = ItemCache(start, end, wsess, witem);
    cur_month = end
