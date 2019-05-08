from impala.dbapi import connect
from datetime import datetime, timedelta


def script():
    date = datetime(year=2019, month=4, day=11)
    for i in range(52):
        simplify_time = (date + timedelta(days=i)).strftime("%Y-%-m-%-d")
        time = (date + timedelta(days=i)).strftime("%Y-%m-%d")
        print(simplify_time)
        print(time)
        impala_sql(time, simplify_time)


def impala_sql(time, simplify_time):
    conn = connect(host="192.168.10.164")
    cursor = conn.cursor()
    cursor.execute(
        "insert into user_track.newhouselog partition(data_date='{}') select DEVICE_ID,CONTEXT_ID,CITY_x,LOGIN_ACCOUNT,START_TIME,END_TIME,OBJECT_ID,CHANNEL,CONTEXT,ROOMID,PROJECTTYPE,MODELID,PROJECTID,SHAIXUAN,PRJ_LISTID,CITY_y,CITY_NAME,PRJ_ITEMNAME,PRJ_LOC,PRJ_DECORATE,PRJ_VIEWS,B_LNG,B_LAT,PRICE_AVG,PRICE_SHOW,PIC_ID,PIC_PRJID,PIC_PRJNAME,PIC_TYPE,PIC_DESC,PIC_TING,PIC_WEI,PIC_CHU,PIC_AREA,PIC_SELL_POINT,PIC_HX_TOTALPRICE,ROOM_ID,FLATS,PRICE,TOTALPRICE from user_track.user_profile_perfect where data_date='{}'".format(
            time, time))


if __name__ == '__main__':
    script()
