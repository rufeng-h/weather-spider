import calendar
import datetime
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import List, Tuple, Iterable
from urllib.parse import urljoin, quote_plus

import requests
from dateutil.relativedelta import relativedelta
from loguru import logger
from redis import StrictRedis
from scrapy import Selector
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models import WeatherRecord, load_db_auth

CUR_DIR = os.path.dirname(__file__)

logger.remove()
logger.add(os.path.join(CUR_DIR, 'weather_history.log'), level=logging.INFO, mode='w')
logger.add(sys.stderr, level=logging.INFO)

MONTH_FORMAT = '%Y%m'
DATE_FORMAT = '%Y-%m-%d'


class WeatherHistory:
    def __init__(self):
        self.base_url = 'https://lishi.tianqi.com/'
        self.session = requests.Session()
        self.logger = logger
        self.session.headers.update(
            {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/112.0.0.0 Safari/537.36'})
        mysql_auth = load_db_auth()['mysql']

        self.engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s" % (
            mysql_auth['username'], quote_plus(mysql_auth['password']), mysql_auth['host'], mysql_auth['port'],
            mysql_auth['database']), echo=False)

        self.db_session = Session(self.engine)

        self.cache = StrictRedis(**load_db_auth()['redis'])
        self.areas = {'长沙', '株洲', '湘潭', '衡阳', '邵阳', '岳阳', '常德', '张家界', '益阳', '郴州', '永州', '怀化',
                      '娄底', '湘西土家族苗族自治州', '广州', '韶关', '深圳', '珠海', '汕头', '佛山', '江门', '湛江',
                      '茂名',
                      '肇庆', '惠州', '梅州', '汕尾', '河源', '阳江', '清远', '东莞', '中山', '潮州', '揭阳', '云浮',
                      '海口',
                      '三亚', '三沙', '儋州', '新会', '顺德'}

    def parse(self, response, area_name) -> List[WeatherRecord]:
        ls = []
        selector = Selector(text=response.text)
        for li in selector.css('body > div.main.clearfix > div.main_left.inleft > div.tian_three > ul > li'):
            date = li.css('div:nth-child(1)::text').get().strip()
            if date.find(' ') != -1:
                date = date.split()[0]
            date = datetime.datetime.strptime(date, DATE_FORMAT)
            max_temp = li.css('div:nth-child(2)::text').get()
            if max_temp is not None:
                max_temp = max_temp.strip()[:-1]
                try:
                    max_temp = int(max_temp)
                except ValueError:
                    max_temp = None
            min_temp = li.css('div:nth-child(3)::text').get()
            if min_temp is not None:
                min_temp = min_temp.strip()[:-1]
                try:
                    min_temp = int(min_temp)
                except ValueError:
                    min_temp = None
            weather = li.css('div:nth-child(4)::text').get()
            if weather is not None:
                weather = weather.strip()
            wind = li.css('div:nth-child(5)::text').get()
            if wind is not None:
                wind = wind.strip()

            self.logger.debug(f'{area_name} {date} {max_temp} {min_temp} {wind}')

            ls.append(
                WeatherRecord(area_name=area_name, wdate=date, weather=weather, max_temp=max_temp,
                              min_temp=min_temp,
                              wind=wind))
        return ls

    def insert(self, data: Iterable[WeatherRecord]):
        self.db_session.add_all(data)
        self.db_session.commit()

    def close(self):
        self.db_session.close()

    @staticmethod
    def _build_cache_key(area, date):
        return f'{area}-{date.strftime(MONTH_FORMAT)}'

    def crwal_single_area(self, area: str, pinyin: str, start_date: datetime.date, end_date: datetime.date):

        def should_crawl():
            # if self.cache.get(self._build_cache_key(area, date)) is not None:
            #     return set()

            day_count = calendar.monthrange(date.year, date.month)[1]
            last = datetime.date(date.year, date.month, day_count)
            first = datetime.date(date.year, date.month, 1)
            stmt = select(WeatherRecord.wdate).where(WeatherRecord.area_name == area, WeatherRecord.wdate >= first,
                                                     WeatherRecord.wdate <= last)
            scalars = set(self.db_session.scalars(stmt))

            if len(scalars) == day_count:
                return set()

            if len(scalars) > day_count:
                raise ValueError(f'重复数据: {area} {date}')

            s2 = set((datetime.date(date.year, date.month, i) for i in range(1, day_count + 1)))

            s2 = s2.difference(scalars)

            today = datetime.date.today()
            s2 = set(filter(lambda d: d <= today, s2))

            return s2

        response = None
        try:
            date = start_date
            while date <= end_date:
                target_dates = should_crawl()
                if len(target_dates) != 0:
                    logger.info(
                        f'should crawl %s %s' % (area, ' '.join(map(lambda d: d.strftime(DATE_FORMAT), target_dates))))
                    url = urljoin(self.base_url, f'{pinyin}/{date.strftime(MONTH_FORMAT)}.html')
                    print(url)
                    response = self.session.get(url)
                    response.raise_for_status()
                    ws = self.parse(response, area)
                    ws = list(
                        filter(lambda a: datetime.date(a.wdate.year, a.wdate.month, a.wdate.day) in target_dates, ws))
                    self.insert(ws)
                    self.logger.info('insert %d records, miss %d records' % (len(ws), len(target_dates) - len(ws)))
                    time.sleep(3 + random.random() * 3)
                else:
                    date += relativedelta(months=1)
                # self.cache.set(self._build_cache_key(area, date), 1)
                date += relativedelta(months=1)

        except Exception as e:
            if response is not None:
                Path(CUR_DIR, 'error.html').write_text(response.text, encoding='utf-8')
            self.logger.error(e)
            raise e

    def run_inc(self):
        date = datetime.date.today() - relativedelta(months=1)
        start_date = datetime.date(date.year, date.month, 1)
        date = datetime.date.today() + relativedelta(months=1)
        end_date = datetime.date(date.year, date.month, 1)
        self._run(start_date, end_date)

    def run_full(self):
        start_date = datetime.date(2011, 1, 1)
        end_date = datetime.date.today()
        self._run(start_date, end_date, True)

    def _run(self, start_date, end_date, full=False):
        area_names, area_pinyins = self._crawl_city_list()
        self.logger.debug(area_names)

        # idx, dt = self._check_break_point(area_names)
        # self.crwal_single_area(area_names[idx], area_pinyins[idx], dt, end_date)
        for i in range(len(area_names)):
            if area_names[i] not in self.areas:
                continue
            if full and self.cache.get('weather:' + area_names[i]) is not None:
                continue
            self.crwal_single_area(area_names[i], area_pinyins[i], start_date, end_date)
            if full:
                self.cache.set('weather:' + area_names[i], 1)
        self.close()

    def _crawl_city_list(self) -> Tuple[List[str], List[str]]:
        area_names, area_pinyins = [], []
        response = self.session.get(self.base_url)
        response.raise_for_status()
        selector = Selector(text=response.text)
        for a in selector.css('div.tablebox table tr td li a'):
            area_name = a.css('::text').get().strip()
            pinyin = a.css('::attr(href)').get().strip().split('/')[-2]
            area_names.append(area_name)
            area_pinyins.append(pinyin)
        return area_names, area_pinyins


if __name__ == '__main__':
    spider = WeatherHistory()
    spider.run_inc()
