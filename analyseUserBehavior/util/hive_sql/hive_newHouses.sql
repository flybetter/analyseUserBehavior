-- hive
CREATE TABLE newhouselog_csv (
  device_id STRING,
  context_id STRING,
  city_x STRING,
  login_account STRING,
  start_time STRING,
  end_time STRING,
  object_id STRING,
  channel     INT,
  context STRING,
  roomid STRING,
  projecttype BIGINT,
  modelid STRING,
  projectid   BIGINT,
  shaixuan STRING,
  prj_listid  BIGINT,
  city_y STRING,
  city_name STRING,
  prj_itemname STRING,
  prj_loc STRING,
  prj_decorate STRING,
  prj_views STRING,
  b_lng STRING,
  b_lat STRING,
  price_avg   BIGINT,
  price_show STRING,
  pic_id STRING,
  pic_prjid STRING,
  pic_prjname STRING,
  pic_type    INT,
  pic_desc STRING,
  pic_ting STRING,
  pic_wei     INT,
  pic_chu     INT,
  pic_area    DOUBLE,
  pic_sell_point STRING,
  pic_hx_totalprice STRING,
  room_id STRING,
  flats       INT,
  price       DOUBLE,
  totalprice  DOUBLE
)COMMENT 'This is new house table csv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';



CREATE TABLE newhouselog (
  device_id STRING,
  context_id STRING,
  city_x STRING,
  login_account STRING,
  start_time STRING,
  end_time STRING,
  object_id STRING,
  channel     INT,
  context STRING,
  roomid STRING,
  projecttype BIGINT,
  modelid STRING,
  projectid   BIGINT,
  shaixuan STRING,
  prj_listid  BIGINT,
  city_y STRING,
  city_name STRING,
  prj_itemname STRING,
  prj_loc STRING,
  prj_decorate STRING,
  prj_views STRING,
  b_lng STRING,
  b_lat STRING,
  price_avg   BIGINT,
  price_show STRING,
  pic_id STRING,
  pic_prjid STRING,
  pic_prjname STRING,
  pic_type    INT,
  pic_desc STRING,
  pic_ting STRING,
  pic_wei     INT,
  pic_chu     INT,
  pic_area    DOUBLE,
  pic_sell_point STRING,
  pic_hx_totalprice STRING,
  room_id STRING,
  flats       INT,
  price       DOUBLE,
  totalprice  DOUBLE
)partitioned by (data_date string)
COMMENT 'This is new house log table'
STORED AS PARQUET;


insert into newhouselog_test partition (data_date='2019-4-28') select * from newhouselog_csv;


insert into newhouselog partition(data_date='2019-03-15') select DEVICE_ID,CONTEXT_ID,CITY_x,LOGIN_ACCOUNT,START_TIME,END_TIME,OBJECT_ID,CHANNEL,CONTEXT,ROOMID,PROJECTTYPE,MODELID,PROJECTID,SHAIXUAN,PRJ_LISTID,CITY_y,CITY_NAME,PRJ_ITEMNAME,PRJ_LOC,PRJ_DECORATE,PRJ_VIEWS,B_LNG,B_LAT,PRICE_AVG,PRICE_SHOW,PIC_ID,PIC_PRJID,PIC_PRJNAME,PIC_TYPE,PIC_DESC,PIC_TING,PIC_WEI,PIC_CHU,PIC_AREA,PIC_SELL_POINT,PIC_HX_TOTALPRICE,ROOM_ID,FLATS,PRICE,TOTALPRICE from user_profile_final where data_date='2019-03-15'
