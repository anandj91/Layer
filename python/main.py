import pandas as pd
import numpy as np

class Item:
    def __init__(self, item_id, time):
        self.item_id = item_id
        self.time = time

    def __str__(self):
        return str((self.time, self.item_id))


class Sessions:
    def __init__(self, train_sessions, train_purchases):
        train_sessions['date'] = pd.to_datetime(train_sessions['date'])
        train_purchases['date'] = pd.to_datetime(train_purchases['date'])
        tmp = train_sessions \
            .groupby('session_id', group_keys=False) \
            .agg(pd.Series.tolist) \
            .merge(train_purchases, on='session_id') \
            .rename(columns={"item_id_x": "item_ids", "date_x": "times", "item_id_y": "target_item_id", "date_y": "target_time"})
        tmp['items'] = tmp.apply(lambda row: self._create_items(row), axis=1)
        tmp['win'] = tmp.apply(lambda row: row['items'][0].time.month * row['items'][0].time.year, axis = 1)
        tmp['count'] = tmp.apply(lambda row: len(row['items']), axis = 1)
        tmp['duration'] = tmp.apply(lambda row: (row['items'][-1].time - row['items'][0].time), axis = 1)
        tmp['target'] = tmp.apply(lambda row: Item(row['target_item_id'], row['target_time']), axis = 1)
        tmp['last_item'] = tmp.apply(lambda row: row['items'][-1], axis = 1)
        tmp['items'] = tmp.apply(lambda row: self._append(row['items'], row['target']), axis = 1)

        tmp = tmp.drop(['item_ids', 'times', 'target_item_id', 'target_time'], axis=1)

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

train_sessions = pd.read_csv("/home/anandj/data/code/Layer/dressipi_recsys2022/train_sessions.csv", parse_dates=True)
print(train_sessions)
train_purchases = pd.read_csv("/home/anandj/data/code/Layer/dressipi_recsys2022/train_purchases.csv", parse_dates=True)
print(train_purchases)

sessions = Sessions(train_sessions, train_purchases)
print(sessions)
