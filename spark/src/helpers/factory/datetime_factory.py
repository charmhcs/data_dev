import pendulum
from helpers.constant import CommonConstant
from pyspark.sql import SparkSession
import logging

class DatetimeFactory():
    def __init__(self,
                 execute_date_time: str,
                 timeznoe_interval: int = CommonConstant.UTC,
                 start_HHmmss: str = CommonConstant.DEFAULT_START_HHmmss,
                 end_HHmmss: str = CommonConstant.DEFAULT_END_HHmmss,
                 add_days: int = CommonConstant.ONE):
        self.execute_date_time = execute_date_time
        self.timeznoe_interval = timeznoe_interval
        self.start_HHmmss: str = start_HHmmss
        self.end_HHmmss: str = end_HHmmss
        self.add_days: int = add_days

    def default(self):
        return self.interval(CommonConstant.ZERO)

    def interval(self,
                 interval_days: int):
        date_time = pendulum.parse(self.execute_date_time).add(hours=self.timeznoe_interval)
        self.date_time = date_time
        self.date_timestamp = date_time.int_timestamp
        self.date = date_time.strftime('%Y-%m-%d')
        self.year = date_time.strftime('%Y')
        self.month = date_time.strftime('%m')
        self.month_no_pad = date_time.strftime('%-m')
        self.day = date_time.strftime('%d')
        self.day_no_pad = date_time.strftime('%-d')
        self.hour = date_time.strftime('%h')
        self.start_year = date_time.subtract(days=interval_days).strftime('%Y')
        self.end_year = date_time.add(days=self.add_days).strftime('%Y')
        self.start_month = date_time.subtract(days=interval_days).strftime('%m')
        self.end_month = date_time.add(days=self.add_days).strftime('%m')
        self.start_date = date_time.subtract(days=interval_days).strftime('%Y-%m-%d')
        self.end_date = date_time.add(days=self.add_days).strftime('%Y-%m-%d')
        self.start_date_time = date_time.subtract(days=interval_days).strftime('%Y-%m-%d') + ' ' + self.start_HHmmss
        self.end_date_time = date_time.add(days=self.add_days).strftime('%Y-%m-%d') + ' ' + self.end_HHmmss
        return self

    def logging(self,
                spark: SparkSession = None):
        if spark is None:
            logging.info(f"date : {self.execute_date_time}")
            logging.info(f"year : {self.year}")
            logging.info(f"month : {self.month}")
            logging.info(f"date : {self.start_date}")
            logging.info(f"start_date_time : {self.start_date_time}")
            logging.info(f"end_date_time : {self.end_date_time}")
        else :
            log = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
            log.info(f"date : {self.execute_date_time}")
            log.info(f"year : {self.year}")
            log.info(f"month : {self.month}")
            log.info(f"date : {self.start_date}")
            log.info(f"start_date_time : {self.start_date_time}")
            log.info(f"end_date_time : {self.end_date_time}")
