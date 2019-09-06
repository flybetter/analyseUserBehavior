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
        self.hdfs = ibis.hdfs_connect(host=HIVE_URL, port=HIVE_PORT, auth_mechanism='GSSAPI')
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

    @decorator
    def update_login(self):
        timez = pytz.timezone('Asia/Shanghai')
        format_date = (datetime.now(tz=timez) - timedelta(days=1)).strftime("%Y-%m-%d")

        self.cursor.execute("drop table user_track.{}".format(self.table_csv))

        # self.cursor.execute("INVALIDATE METADATA")

        self.cursor.execute(
            "CREATE TABLE user_track.newhouselog_csv_tmp AS SELECT new.device_id,new.context_id,new.city_x,(case when length(trim(new.login_account))=0 then phone.phone when new.login_account='null' then phone.phone when new.login_account is null then phone.phone else new.login_account end) as login_account,new.start_time,new.end_time,new.object_id,new.channel,new.context,new.roomid,new.projecttype,new.modelid,new.projectid,new.shaixuan,new.prj_listid,new.city_y,new.city_name,new.prj_itemname,new.prj_loc,new.prj_decorate,new.prj_views,new.b_lng,new.b_lat,new.price_avg,new.price_show,new.pic_id,new.pic_prjid,new.pic_prjname,new.pic_type,new.pic_desc,new.pic_ting,new.pic_wei,new.pic_chu,new.pic_area,new.pic_sell_point,new.pic_hx_totalprice,new.room_id,new.flats,new.price,new.totalprice FROM user_track.newhouselog_csv as new left join user_track.account_device_phone as phone on new.device_id=phone.deviceid ")

        self.cursor.execute(
            "insert into user_track.{} partition (data_date='{}') select * from user_track.{}".format(self.table,
                                                                                                      format_date,
                                                                                                      self.table_csv))


def begin(table_csv, table, local_path=HIVE_NEWHOUSELOG_CSV_PATH, hive_path=HIVE_SERVER_NEWHOUSELOG_CSV_PATH):
    hive_action = HiveAction(table_csv, table, local_path, hive_path)
    hive_action.upload()
    hive_action.sync()


def update_login(table_csv=SQOOP_TABLE_CSV, table=SQOOP_TABLE, local_path=None, hive_path=None):
    hive_action = HiveAction(table_csv, table, local_path, hive_path)
    hive_action.update_login()


if __name__ == '__main__':
    hive_action = HiveAction(table="secondhouselog", table_csv="secondhouselog_csv_tmp",
                             local_path=HIVE_SECONDHOUSELOG_CSV_PATH,
                             hive_path=HIVE_SERVER_SECONDHOUSELOG_CSV_PATH)
    hive_action.upload()

    # update_login()

    # format_date = "2018-9-11"
    # aa = "insert into user_track.newhouselog_test partition (data_date='{}') select * from user_track.newhouselog_csv".format(
    #     format_date)
    # print(aa)
