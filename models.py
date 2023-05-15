import configparser
import copy
import datetime
import decimal
import os
from urllib.parse import quote_plus

from sqlalchemy import Integer, VARCHAR, Date, DateTime, CHAR, Float, create_engine, DECIMAL
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

__engine = None


def load_db_auth():
    parser = configparser.ConfigParser()
    parser.read(os.path.join(os.path.dirname(__file__), 'db.conf'))
    return parser


class Base(DeclarativeBase):
    pass


class Station(Base):
    __tablename__ = 'station'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False, autoincrement=True)
    station_name: Mapped[str] = mapped_column(VARCHAR(7), nullable=False)
    pname: Mapped[str] = mapped_column(VARCHAR(15), nullable=False, comment='省', default='')
    city_name: Mapped[str] = mapped_column(VARCHAR(15), nullable=False, comment='市', default='')
    adname: Mapped[str] = mapped_column(VARCHAR(15), nullable=False, comment='区/县', default='')
    address: Mapped[str] = mapped_column(VARCHAR(255), nullable=False, comment='详细地址', default='')
    adcode: Mapped[str] = mapped_column(CHAR(12), nullable=False, comment='区县代码', default='')
    longitude: Mapped[decimal.Decimal] = mapped_column(DECIMAL(9, 6), nullable=True, comment='经度')
    latitude: Mapped[decimal.Decimal] = mapped_column(DECIMAL(9, 6), nullable=True, comment='维度')
    remark: Mapped[str] = mapped_column(VARCHAR(255), nullable=False, default='')
    update_time: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, default=datetime.datetime.now)

    def __str__(self):
        s = '-'.join((self.pname, self.city_name, self.adname, self.station_name))
        if self.remark is not None:
            return s + f'({self.remark})'
        return s

    __repr__ = __str__

    def __hash__(self):
        return hash(self.station_name)


class WeatherRecordHour(Base):
    """
  CREATE TABLE `weather_hour`
(
    `id`          bigint                                                       NOT NULL AUTO_INCREMENT,
    `area`        varchar(31) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '地区',
    `code`        char(12)                                                     null COMMENT '行政区划代码',
    `obs_time`    datetime                                                     NOT NULL COMMENT '观测时间',
    `temperature` tinyint                                                      NULL COMMENT '温度',
    `feels_like`  tinyint                                                      NULL COMMENT '体感温度',
    `dewpoint`    tinyint                                                               DEFAULT NULL COMMENT '露点温度',
    `humidity`    tinyint                                                      NULL COMMENT '湿度，%',
    `wind`        char(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci     NULL COMMENT '风向',
    `wind_dir`    tinyint                                                      null comment '风向角',
    `wind_speed`  tinyint                                                      NULL COMMENT '风速，mph',
    `pressure`    decimal(5, 2)                                                NULL COMMENT '压力',
    `precip`      decimal(5, 2)                                                NULL COMMENT '降水量',
    `weather`     varchar(31) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '天气情况',
    `source`      char(1)                                                      not null COMMENT '0 -> wunderground.com, 1 -> 和风api',
    `create_time` datetime                                                     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci
  ROW_FORMAT = DYNAMIC;
    """
    __tablename__ = 'weather_hour'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False, autoincrement=True)
    area: Mapped[str] = mapped_column(VARCHAR(31), nullable=False)
    code: Mapped[str] = mapped_column(CHAR(12), nullable=False)
    obs_time: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
    temperature: Mapped[int] = mapped_column(Integer, nullable=True)
    feels_like: Mapped[int] = mapped_column(Integer, nullable=True)
    dewpoint: Mapped[int] = mapped_column(Integer, nullable=True)
    humidity: Mapped[int] = mapped_column(Float, nullable=True)
    wind: Mapped[int] = mapped_column(CHAR(4), nullable=True)
    wind_dir: Mapped[int] = mapped_column(Integer, nullable=True)
    wind_speed: Mapped[int] = mapped_column(Integer, nullable=True, default=None)
    pressure: Mapped[float] = mapped_column(Float, nullable=True)
    precip: Mapped[float] = mapped_column(Float, nullable=True)
    weather: Mapped[str] = mapped_column(VARCHAR(31), nullable=True)
    source: Mapped[str] = mapped_column(CHAR(1), nullable=False)
    create_time: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, default=datetime.datetime.now)

    def __str__(self):
        return f'{self.area}: {self.obs_time}'

    __repr__ = __str__

    def to_csv_string(self):
        d = copy.deepcopy(self.__dict__)
        if '_sa_instance_state' in d:
            d.pop('_sa_instance_state')
        return ','.join(map(str, d.values()))


class Area(Base):
    __tablename__ = 'area'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False, autoincrement=True)
    code: Mapped[str] = mapped_column(CHAR(12), nullable=False)
    name: Mapped[str] = mapped_column(VARCHAR(15), nullable=False)
    level: Mapped[int] = mapped_column(Integer, nullable=False)
    pcode: Mapped[str] = mapped_column(CHAR(12), nullable=False, default='')
    create_time: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, default=datetime.datetime.now)

    def __str__(self):
        return f'{self.name}'

    __repr__ = __str__


class WeatherRecord(Base):
    __tablename__ = 'weather'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False, autoincrement=True)
    wdate: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    area_name: Mapped[str] = mapped_column(VARCHAR(31), nullable=False)
    max_temp: Mapped[int] = mapped_column(Integer, nullable=True)
    min_temp: Mapped[int] = mapped_column(Integer, nullable=True)
    weather: Mapped[str] = mapped_column(VARCHAR(31), nullable=True)
    wind: Mapped[str] = mapped_column(VARCHAR(31), nullable=True)
    update_time: Mapped[str] = mapped_column(DateTime, nullable=False, default=datetime.datetime.now)

    def __str__(self):
        return f'{self.area_name}: {self.wdate.strftime("%Y-%m-%d")}'

    __repr__ = __str__


def get_engine(echo=False):
    global __engine
    if __engine is None:
        mysql_auth = load_db_auth()['mysql']

        __engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s" % (
            mysql_auth['username'], quote_plus(mysql_auth['password']), mysql_auth['host'], mysql_auth['port'],
            mysql_auth['database']), echo=echo)
        Base.metadata.create_all(__engine)
    return __engine
