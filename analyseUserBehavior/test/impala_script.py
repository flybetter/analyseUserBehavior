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
    date = datetime(year=2019, month=3, day=16)
    enddate = datetime(year=2019, month=9, day=16)
    days = (enddate - date).days
    for i in range(days):
        simplify_time = (date + timedelta(days=i)).strftime("%Y-%-m-%-d")
        time = (date + timedelta(days=i)).strftime("%Y-%m-%d")
        print(time)
        # impala_sql(time, simplify_time)
        impala_newhouse(time)
        print("new house :" + time)
        impala_secondhouse(time)
        print("second house :" + time)


def impala_newhouse(time):
    conn = connect(host="cdho1.prod.house365", auth_mechanism='GSSAPI')
    cursor = conn.cursor()
    cursor.execute(
        "insert into user_track.newhouselog_login partition(data_date='{}') select device_id,context_id,city_x,login_account,start_time,end_time,object_id,channel,context,roomid,projecttype,modelid,projectid,shaixuan,prj_listid,city_y,city_name,prj_itemname,prj_loc,prj_decorate,prj_views,b_lng,b_lat,price_avg,price_show,pic_id,pic_prjid,pic_prjname,pic_type,pic_desc,pic_ting,pic_wei,pic_chu,pic_area,pic_sell_point,pic_hx_totalprice,room_id,flats,price,totalprice, phone.passport_uid, phone.phone from user_track.newhouselog left join user_track.dwb_account_device_phone phone on user_track.newhouselog.device_id= phone.deviceid where user_track.newhouselog.data_date = '{}'".format(
            time, time))


def impala_secondhouse(time):
    conn = connect(host="cdho1.prod.house365", auth_mechanism='GSSAPI')
    cursor = conn.cursor()
    cursor.execute(
        "insert into user_track.secondhouselog_login partition(data_date='{}')select second.device_id,second.context_id,second.city,second.login_account,second.start_time,second.end_time,second.content,second.object_id,second.context,second.secondhouse_id,second.esta_x,second.district_x,second.address_x,second.streetid_x,second.blockid,second.blockshowname,second.purpose,second.structure,second.buildtype,second.buildyear,second.buildarea,second.subfloor,second.floor,second.totalfloor,second.room,second.hall,second.toilet,second.kitchen,second.balcony,second.forward,second.price,second.averprice_x,second.environment,second.traffic,second.fitment,second.serverco,second.contactor,second.telno,second.mobile,second.creattime,second.updatetime,second.expiretime,second.city_name,second.block_id,second.blockname,second.district_y,second.streetid_y,second.area,second.address_y,second.bus,second.averprice_y,second.updateprice,second.forumid,second.newhouseid,second.b_map_x,second.b_map_y,second.accuracy,second.map_test,second.b_property_type,second.b_green,second.b_parking,second.b_developers,second.b_property_company,second.b_property_fees,second.b_bus,second.b_metro,second.b_num,second.bi_s,second.bi_spell,second.subway,second.sitename,second.subwayrange,second.app,second.esta_y,second.property_fees,second.nofee,second.plot_ratio,second.total_room,second.turn_time,second.b_area,second.feature,phone.passport_uid, phone.phone from user_track.secondhouselog second left join user_track.dwb_account_device_phone phone on second.device_id = phone.deviceid where second.data_date = '{}'".format(
            time, time))


if __name__ == '__main__':
    script2()
