import ibis
from analyseUserBehavior.algorithm import *
from functools import wraps
from impala.dbapi import connect
from requests import Session
import pytz
from datetime import datetime, date, timedelta
import subprocess


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
        kt_cmd = 'kinit app -k -t /tmp/app_prod.keytab'
        status = subprocess.call([kt_cmd], shell=True)
        if status != 0:
            print("kinit ERROR:")
            print(subprocess.call([kt_cmd], shell=True))
            exit()
        session = Session()
        session.verify = False
        self.hdfs = ibis.hdfs_connect(host=HIVE_URL, port=HIVE_PORT, auth_mechanism='GSSAPI', session=session,
                                      use_https=False)
        # self.client = ibis.impala.connect(host=HIVE_URL, database='user_track', hdfs_client=self.hdfs)
        conn = connect(host=HIVE_URL, auth_mechanism='GSSAPI')
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

    @staticmethod
    def update_login(table_csv, table_csv_tmp, table):
        conn = connect(host=HIVE_URL, auth_mechanism='GSSAPI')
        cursor = conn.cursor()
        timez = pytz.timezone('Asia/Shanghai')
        format_date = (datetime.now(tz=timez) - timedelta(days=1)).strftime("%Y-%m-%d")

        cursor.execute(
            "create table user_track.{0}  as select csv.*,phone.passport_uid ,phone.phone from user_track.{1} csv left join user_track.dwb_account_device_phone phone on csv.device_id=phone.deviceid".format(
                table_csv_tmp, table_csv))

        cursor.execute(
            "insert into user_track.{0} partition (data_date='{1}') select * from user_track.{2}".format(table,
                                                                                                         format_date,
                                                                                                         table_csv_tmp))

        cursor.execute("drop table user_track.{}".format(table_csv_tmp))


def begin(table_csv, table, local_path=HIVE_NEWHOUSELOG_CSV_PATH, hive_path=HIVE_SERVER_NEWHOUSELOG_CSV_PATH):
    hive_action = HiveAction(table_csv, table, local_path, hive_path)
    hive_action.upload()
    hive_action.sync()


def update_login():
    HiveAction.update_login(table_csv=SQOOP_NEWHOUSELOG_TABLE_CSV, table_csv_tmp=SQOOP_NEWHOUSELOG_TABLE_CSV_TMP,
                            table=SQOOP_NEWHOUSELOG_TABLE)

    HiveAction.update_login(table_csv=SQOOP_SECONDHOUSELOG_TABLE_CSV, table_csv_tmp=SQOOP_SECONDHOUSELOG_TABLE_CSV_TMP,
                            table=SQOOP_SECONDHOUSELOG_TABLE)


if __name__ == '__main__':
    # hive_action = HiveAction()
    # hive_action.upload()

    update_login()

    # format_date = "2018-9-11"
    # aa = "insert into user_track.newhouselog_test partition (data_date='{}') select * from user_track.newhouselog_csv".format(
    #     format_date)
    # print(aa)
