from impala.dbapi import connect
from datetime import datetime, timedelta


def script():
    date = datetime(year=2019, month=5, day=14)
    for i in range(24):
        simplify_time = (date + timedelta(days=i)).strftime("%Y-%-m-%-d")
        time = (date + timedelta(days=i)).strftime("%Y-%m-%d")
        print(simplify_time)
        print(time)
        # impala_sql(time, simplify_time)
        impala_secondhouse(time)


def script2():
    date = datetime(year=2019, month=5, day=14)
    enddate = datetime(year=2019, month=6, day=10)
    days = (enddate - date).days
    for i in range(days):
        simplify_time = (date + timedelta(days=i)).strftime("%Y-%-m-%-d")
        time = (date + timedelta(days=i)).strftime("%Y-%m-%d")
        print(time)
        # impala_sql(time, simplify_time)
        impala_secondhouse(time)


def impala_sql(time, simplify_time):
    conn = connect(host="192.168.10.164")
    cursor = conn.cursor()
    cursor.execute(
        "insert into user_track.newhouselog partition(data_date='{}') select DEVICE_ID,CONTEXT_ID,CITY_x,LOGIN_ACCOUNT,START_TIME,END_TIME,OBJECT_ID,CHANNEL,CONTEXT,ROOMID,PROJECTTYPE,MODELID,PROJECTID,SHAIXUAN,PRJ_LISTID,CITY_y,CITY_NAME,PRJ_ITEMNAME,PRJ_LOC,PRJ_DECORATE,PRJ_VIEWS,B_LNG,B_LAT,PRICE_AVG,PRICE_SHOW,PIC_ID,PIC_PRJID,PIC_PRJNAME,PIC_TYPE,PIC_DESC,PIC_TING,PIC_WEI,PIC_CHU,PIC_AREA,PIC_SELL_POINT,PIC_HX_TOTALPRICE,ROOM_ID,FLATS,PRICE,TOTALPRICE from user_track.user_profile_perfect where data_date='{}'".format(
            time, time))


def impala_secondhouse(time):
    conn = connect(host="192.168.10.164")
    cursor = conn.cursor()
    cursor.execute(
        "insert into user_track.secondhouselog partition(data_date='{}') select device_id,context_id,city,login_account,start_time,end_time,content,object_id,context,secondhouse_id,esta_x,district_x,address_x,streetid_x,blockid,blockshowname,purpose,structure,buildtype,buildyear,buildarea,subfloor,floor,totalfloor,room,hall,toilet,kitchen,balcony,forward,price,averprice_x,environment,traffic,fitment,serverco,contactor,telno,mobile,creattime,updatetime,expiretime,city_name,block_id,blockname,district_y,streetid_y,area,address_y,bus,averprice_y,updateprice,forumid,newhouseid,b_map_x,b_map_y,accuracy,map_test,b_property_type,b_green,b_parking,b_developers,b_property_company,b_property_fees,b_bus,b_metro,b_num,bi_s,bi_spell,subway,sitename,subwayrange,app,esta_y,property_fees,nofee,plot_ratio,total_room,turn_time,b_area,feature from user_track.secondhouselog_back where data_date='{}'".format(
            time, time))


if __name__ == '__main__':
    script2()
