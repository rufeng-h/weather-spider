import datetime
import decimal
import glob
import json
import os
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from models import Area, get_engine, WeatherRecordHour, Station, WeatherRecord


def parse_area():
    ls = json.loads(Path('./area_code_2022.json').read_text(encoding='utf-8'))

    areas = []

    def handle(s: list):
        for a in s:
            if a.get('children') is not None:
                c = a.pop('children')
                handle(c)
            areas.append(Area(**a))

    handle(ls)

    print(areas)

    engine = get_engine(echo=False)

    with Session(engine) as sess:
        sess.add_all(areas)
        sess.commit()


def ck_format():
    import csv
    dtype = {'code': str, 'temprature': 'Int64', 'feels_like': 'Int64', 'dewpoint': 'Int64',
             'humidity': 'Int64',
             'wind_dir': 'Int64', 'wind_speed': 'Int64', 'pressure': 'Float64',
             'precip': 'Float64', 'source': str}

    df = pd.read_csv('csv/weather_hour.csv', dtype=dtype)
    df.to_csv(
        './weather_hour.csv', index=False,
        quoting=csv.QUOTE_NONNUMERIC)


def parse_weather_hour():
    """
    Object(o.S)(e, "observations", []).map(t=>{
                            const e = new Date(1e3 * Object(o.S)(t, "valid_time_gmt", 0))
                              , r = e.getUTCHours() + n.offset + e.getTimezoneOffset() / 60;
                            return e.setUTCHours(r),
                            {
                                date: e,
                                icon: Object(o.S)(t, "wx_icon", 0),
                                condition: Object(o.S)(t, "wx_phrase", "N/A"),
                                temperature: Object(o.S)(t, "temp", 0),
                                dewPoint: Object(o.S)(t, "dewPt", 0),
                                humidity: Object(o.S)(t, "rh", 0),
                                wind: Object(o.S)(t, "wdir", 0),
                                windcardinal: Object(o.S)(t, "wdir_cardinal", ""),
                                windSpeed: Object(o.S)(t, "wspd", 0),
                                windGust: Object(o.S)(t, "gust", 0),
                                pressure: Object(o.S)(t, "pressure", 0),
                                precipRate: Object(o.S)(t, "precip_hrly", 0),
                                precipTotal: Object(o.S)(t, "precip_total", 0)
                            }
    """

    def find_area(src):
        target = None
        for a in areas:
            if src.find(a.name) != -1 or a.name.find(src) != -1:
                if target is None:
                    target = a
                else:
                    raise ValueError(src + " " + target.name + ' ' + a.name)
        if target is None:
            raise ValueError(city)
        return target

    engine = get_engine()
    sess = Session(engine)
    areas = []
    areas = list(sess.scalars(select(Area).where(Area.level == 2)))

    if not os.path.exists('csv/weather_hour.csv'):
        with open('csv/weather_hour.csv', 'w', encoding='utf-8') as f:
            f.write(','.join((
                'area', 'code', 'obs_time', 'temprature', 'feels_like', 'dewpoint', 'humidity', 'wind', 'wind_dir',
                'wind_speed', 'pressure', 'precip', 'weather', 'source')) + '\n')

    handled_cities = pd.read_csv('csv/weather_hour.csv')['area'].drop_duplicates().values.tolist()
    print(handled_cities)

    f = open('csv/weather_hour.csv', 'a', encoding='utf-8')

    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    for city in os.listdir(data_dir):
        if city in handled_cities:
            continue
        for file in glob.glob(os.path.join(os.path.join(data_dir, city, '*.json'))):
            items = []
            js = json.loads(Path(file).read_text(encoding='utf-8'))
            obss = js.get('observations')
            if obss is None:
                continue
            for obs in obss:
                params = dict()
                params['area'] = city
                params['code'] = find_area(city).code
                params['obs_time'] = datetime.datetime.fromtimestamp(obs['valid_time_gmt'])
                params['temperature'] = int(round((obs['temp'] - 32) / 1.8, 0)) if obs['temp'] is not None else None
                params['feels_like'] = int(round((obs['feels_like'] - 32) / 1.8, 0)) if obs[
                                                                                            'feels_like'] is not None else None
                params['dewpoint'] = int(round((obs['dewPt'] - 32) / 1.8, 0)) if obs['dewPt'] is not None else None
                params['humidity'] = obs['rh']
                params['wind'] = obs['wdir_cardinal']
                params['wind_dir'] = obs['wdir']
                params['wind_speed'] = int(round(obs['wspd'] * 1.609344, 0)) if obs['wspd'] is not None else None
                params['pressure'] = obs['pressure']
                params['precip'] = 0 if obs.get('precip_total') is None else obs.get('precip_total')
                params['weather'] = obs['wx_phrase']
                params['source'] = '0'
                r = WeatherRecordHour(**params)
                items.append(r)
            f.write('\n'.join(map(lambda r: r.to_csv_string(), items)) + '\n')
            f.flush()
            print(file)

    sess.close()
    # WeatherRecordHour(**params)


def parse_station_pos():
    path = os.path.join(os.path.dirname(__file__), 'data', 'json', 'station_pos.json')
    data = json.loads(Path(path).read_text(encoding='utf-8'))
    sts = []
    for station, poss in data.items():
        pos = None
        for p in poss:
            if p['typecode'] == '150200':
                pos = p
                break
        if pos is None:
            raise ValueError(station)
        station_name, remark = pos['name'], None
        match = re.search(r'(\(.+?\))', station_name)
        if match is not None:
            remark = match.group()[1:-1]
            station_name = station_name.replace(match.group(), '')
        sts.append(Station(adcode=pos['adcode'], pname=pos['pname'], city_name=pos['cityname'], adname=pos['adname'],
                           station_name=station_name, longitude=decimal.Decimal(pos['location'].split(',')[0]),
                           latitude=decimal.Decimal(pos['location'].split(',')[1]), remark=remark,
                           address=pos['address']))
        with Session(get_engine(True)) as sess:
            sess.add_all(sts)
            sess.commit()


def map_adcode():
    session = Session(get_engine())
    area_names = list(session.scalars(select(WeatherRecord.area_name).distinct()))
    areas = list(session.scalars(select(Area)))
    mp = {}
    for area_name in area_names:
        for area in areas:
            if area.name.find(area_name) != -1:
                if mp.get(area_name) is None:
                    mp[area_name] = area
                else:
                    print(area_name + ' ' + area.name + ' ' + str(mp[area_name]))

    print(mp)


if __name__ == '__main__':
    # parse_weather_hour()
    # ck_format()
    # parse_station_pos()
    map_adcode()
