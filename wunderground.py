import calendar
import datetime
import json
import logging
import os.path
import random
import re
import sys
import time
from pathlib import Path

import requests
from loguru import logger
from redis import StrictRedis

logger.remove()
logger.add(sys.stderr, level=logging.DEBUG)

START_YEAR = 2012
END_YEAR = 2023

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


class WunderGroundWeather:
    def __init__(self):
        self.session = requests.session()
        self.proxies = {'http': '127.0.0.1:7890', 'https': '127.0.0.1:7890'}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/112.0.0.0 Safari/537.36'}
        self.session.proxies.update(self.proxies)
        self.session.headers.update(self.headers)
        self.base_url = 'https://www.wunderground.com/history/daily/cn/%s'
        self.historical_url = 'https://api.weather.com/v1/location/%s/observations/historical.json'
        self.cache = StrictRedis(host='120.24.176.245', password='@rufeng20001123')
        self.location_map = {
            "长沙": "changsha",
            "株洲": "zhuzhou",
            "湘潭": "xiangtan",
            "衡阳": "hengyang",
            "邵阳": "shaoyang",
            "岳阳": "yueyang",
            "常德": "changde",
            "张家界": "zhangjiajie",
            "益阳": "yiyang",
            "郴州": "chenzhou",
            "永州": "yongzhou",
            "怀化": "huaihua",
            "娄底": "loudi",
            "湘西土家族苗族自治": "xiangxi-tujia-and-miao-autonomous-prefecture",
            "广州": "guangzhou",
            "韶关": "shaoguan",
            "深圳": "shenzhen",
            "珠海": "zhuhai",
            "汕头": "shantou",
            "佛山": "foshan",
            "江门": "jiangmen",
            "湛江": "zhanjiang",
            "茂名": "maoming",
            "肇庆": "zhaoqing",
            "惠州": "huizhou",
            "梅州": "meizhou",
            "汕尾": "shanwei",
            "河源": "heyuan",
            "阳江": "yangjiang",
            "清远": "qingyuan",
            "东莞": "dongguan",
            "中山": "zhongshan",
            "潮州": "chaozhou",
            "揭阳": "jieyang",
            "云浮": "yunfu",
            "海口": "haikou",
            "三亚": "sanya",
            "三沙": "sansha",
            "儋州": "danzhou",
        }

    def run(self):
        for city, pinyin in self.location_map.items():
            location_id, api_key = self._extract_loc_and_key(pinyin)
            logger.debug(f'{city} ==> {location_id}')
            params = {
                'apiKey': api_key,
                'units': 'e',
                'startDate': '',
                'endDate': ''
            }
            city_dir = os.path.join(DATA_DIR, city)
            if not os.path.exists(city_dir):
                os.mkdir(city_dir)

            for year in range(START_YEAR, END_YEAR + 1):
                for month in range(1, 13):
                    filename = os.path.join(city_dir, f'{year}-{month}.json')

                    if os.path.exists(filename):
                        logger.info(f'skip {filename}')
                        continue
                    if datetime.date(year, month, 1) >= datetime.date.today():
                        break
                    params['startDate'] = datetime.date(year, month, 1).strftime('%Y%m%d')
                    params['endDate'] = datetime.date(year, month, calendar.monthrange(year, month)[1]).strftime(
                        '%Y%m%d')
                    logger.info(f'crawl {city} {year} {month}')
                    res_json = requests.get(self.historical_url % location_id, params=params, headers=self.headers,
                                            proxies=self.proxies).json()
                    Path(filename).write_text(json.dumps(res_json, indent=4, ensure_ascii=False), encoding='utf-8')

                    logger.info(f'writed {filename}')
                    time.sleep(random.random())

    def _extract_loc_and_key(self, pinyin):
        response = requests.get(self.base_url % pinyin, headers=self.headers,
                                proxies=self.proxies)
        station_id = re.search(r'class="station-id">\((.+?)\)', response.text).group(1)
        api_key = re.search(r'apiKey=([a-z0-9]+)', response.text).group(1)

        return f'{station_id}:9:CN', api_key


if __name__ == '__main__':
    WunderGroundWeather().run()
