import ibis
from analyseUserBehavior.algorithm import *
from functools import wraps
from impala.dbapi import connect
import pytz
from datetime import datetime, date, timedelta


def decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            traceback.print_exc()
            raise ValueError(func.__name__ + ' has something wrong')

    return wrapper


class HiveAction(object):
    def __init__(self, table_csv="newhouselog_csv", table="newhouselog", local_path=HIVE_NEWHOUSELOG_CSV_PATH,
                 hive_path=HIVE_SERVER_NEWHOUSELOG_CSV_PATH):
        self.table_csv = table_csv
        self.table = table
        self.local_path = local_path
        self.hive_path = hive_path
        self.hdfs = ibis.hdfs_connect(host=HIVE_URL, port=HIVE_PORT)
        self.client = ibis.impala.connect(host=HIVE_URL, database='user_track', hdfs_client=self.hdfs)
        conn = connect(host=HIVE_URL)
        self.cursor = conn.cursor()

    @decorator
    def upload(self):
        self.hdfs.put(hdfs_path=self.hive_path, resource=self.local_path, overwrite=True)

    @decorator
    def sync(self):
        timez = pytz.timezone('Asia/Shanghai')
        format_date = (datetime.now(tz=timez) - timedelta(days=1)).strftime("%Y-%m-%d")
        self.cursor.execute("refresh user_track.{}".format(self.table_csv))
        self.cursor.execute(
            "insert into user_track.{} partition (data_date='{}') select * from user_track.{}".format(self.table,
                                                                                                      format_date,
                                                                                                      self.table_csv))


def begin(table_csv, table):
    hiveaction = HiveAction(table_csv, table)
    hiveaction.upload()
    hiveaction.sync()


if __name__ == '__main__':
    hiveaction = HiveAction()
    hiveaction.upload()
    hiveaction.sync()

    # format_date = "2018-9-11"
    # aa = "insert into user_track.newhouselog_test partition (data_date='{}') select * from user_track.newhouselog_csv".format(
    #     format_date)
    # print(aa)
