import datetime
import decimal
import os.path
import time
import sys
import logging

import pandas as pd
import requests
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import func
from loguru import logger

from models import WeatherRecordHour, get_engine

CUR_DIR = os.path.dirname(__file__)
logger.remove()
# logger.add(os.path.join(CUR_DIR, 'weather_history.log'), level=logging.INFO, mode='w')
logger.add(sys.stderr, level=logging.INFO)


class QWeather:
    def __init__(self):
        self.__key = '1ef7d2e60ca74fcf82c9f12736176704'
        self.__engine = get_engine()
        df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'China-City-List-latest.csv'))
        areas = ['长沙', '株洲', '湘潭', '衡阳', '邵阳', '岳阳', '常德', '张家界', '益阳', '郴州', '永州', '怀化',
                 '娄底', '湘西土家族苗族自治州', '广州', '韶关', '深圳', '珠海', '汕头', '佛山', '江门', '湛江', '茂名',
                 '肇庆', '惠州', '梅州', '汕尾', '河源', '阳江', '清远', '东莞', '中山', '潮州', '揭阳', '云浮', '海口',
                 '三亚', '三沙', '儋州']
        self.cities = df[df['Location_Name_ZH'].isin(areas)]
        assert self.cities.shape[0] == len(areas)

    def find_city(self, city_name, single=True):
        ret = self.cities[self.cities['Location_Name_ZH'] == city_name].copy()

        if single:
            if ret.shape[0] != 1:
                raise ValueError("find %s records, city_name %s" % (ret.shape[0], city_name))
            return ret.iloc[0]

        return ret

    def real_time_weather(self):
        url = 'https://devapi.qweather.com/v7/weather/now'
        sess = Session(self.__engine)
        records = []
        try:
            for idx, city in self.cities.iterrows():
                params = {
                    'key': self.__key,
                    'location': city['Location_ID'],
                    'lang': 'en'
                }
                data = requests.get(url, params=params).json()
                if data['code'] != '200':
                    raise ValueError(data)

                now = data['now']
                logger.info(now)
                obs_time = time.strptime(now['obsTime'], "%Y-%m-%dT%H:%M%z")
                area = city['Location_Name_ZH']
                record = WeatherRecordHour(area=area, obs_time=obs_time, temperature=int(now['temp']),
                                           feels_like=int(now['feelsLike']), dewpoint=int(now['dew']),
                                           humidity=int(now['humidity']), wind=now['windDir'],
                                           wind_dir=int(now['wind360']),
                                           wind_speed=int(now['windSpeed']), precip=decimal.Decimal(now['precip']),
                                           weather=now['text'], source='1', create_time=datetime.datetime.now())
                stmt = select(func.count(WeatherRecordHour.area)).where(WeatherRecordHour.area == area,
                                                                        WeatherRecordHour.obs_time == obs_time)
                if sess.scalar(stmt) == 0:
                    records.append(record)
        finally:
            if len(records) != 0:
                sess.add_all(records)
                logger.info("insert %s records" % len(records))
                sess.commit()
            sess.close()


if __name__ == '__main__':
    q_weather = QWeather()
    q_weather.real_time_weather()
    #
    # print(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M+%z"))
