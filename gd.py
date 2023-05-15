import decimal
import json
import os.path
import random
import re
from pathlib import Path
from typing import List

import pandas as pd
import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from models import Station, get_engine


def load_cache() -> List[dict]:
    path = os.path.join(os.path.dirname(__file__), 'station_position.json')
    if not os.path.exists(path):
        return []
    return json.loads(Path(path).read_text(encoding='utf-8'))


class GdApi:
    def __init__(self):
        self.__keys = dict.fromkeys(['40dc93a78162adf178137595ebc3af10', 'fef325053c296db5d899b7fbf5523e87'], True)

    def __select_key(self):
        keys = list(filter(lambda k: self.__keys[k], self.__keys.keys()))
        try:
            return random.choice(keys)
        except IndexError:
            raise ValueError('no key is valid')

    def placev2(self, station, city):
        url = 'https://restapi.amap.com/v5/place/text'
        key = self.__select_key()
        params = {
            'types': '150200|150600',
            'keywords': station,
            'key': key,
            'region': city,
        }
        response = requests.get(url, params=params)
        try:
            return response.json()['pois']
        except KeyError:
            self.__keys[key] = False
            return self.placev2(station, city)

    def regeo(self):
        url = 'https://restapi.amap.com/v3/geocode/regeo'
        params = {
            'key': self.__select_key(),
            'location': '113.53,22.19'
        }
        requests.get(url, params=params).json()


def place_station():
    gd = GdApi()
    sess = Session(get_engine())
    handled_sts = set(sess.scalars(select(Station.station_name)))
    p = os.path.join(os.path.dirname(__file__), 'passed_sts.txt')
    if os.path.exists(p):
        passed_sts = set(Path(p).read_text(encoding='utf-8').splitlines())
    else:
        passed_sts = set()
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'station_all.csv')).drop_duplicates(subset=['station_name'])
    sts = set()
    try:
        for idx, row in df.iterrows():
            station, city = row['station_name'] + '站', row['city_name']
            if station in handled_sts or station in passed_sts:
                continue
            poss, pos = gd.placev2(station, city), None
            print(poss)
            # 没查到
            if len(poss) == 0:
                passed_sts.add(station)
                continue
            for p in poss:
                if p.get('typecode') is not None and p['typecode'].startswith('1502'):
                    pos = p
                    break
            # 查到但不是火车站
            if pos is None:
                passed_sts.add(station)
                continue
            station_name, remark = pos['name'], None
            match = re.search(r'(\(.+?\))', station_name)
            if match is not None:
                remark = match.group()[1:-1]
                station_name = station_name.replace(match.group(), '')
            if station_name not in handled_sts:
                sts.add(
                    Station(adcode=pos['adcode'] + '0' * 6, pname=pos['pname'], city_name=pos['cityname'],
                            adname=pos['adname'],
                            station_name=station_name, longitude=decimal.Decimal(pos['location'].split(',')[0]),
                            latitude=decimal.Decimal(pos['location'].split(',')[1]), remark=remark,
                            address=pos['address']))

            if len(sts) >= 100:
                sess.add_all(sts)
                sess.commit()
                sts.clear()
    finally:
        if len(sts) != 0:
            sess.add_all(sts)
            sess.commit()
        Path(os.path.dirname(__file__), 'passed_sts.txt').write_text('\n'.join(passed_sts), encoding='utf-8')
        sess.close()


if __name__ == '__main__':
    place_station()
