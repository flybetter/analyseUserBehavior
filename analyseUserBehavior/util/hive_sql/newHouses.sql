CREATE  TABLE `newhouselog_csv`(
  id bigint,
  DEVICE_ID string ,
  CONTEXT_ID string ,
  CITY_x string ,
  LOGIN_ACCOUNT double ,
  START_TIME string ,
  END_TIME string ,
  OBJECT_ID double ,
  CHANNEL string ,
  CONTEXT string ,
  ROOMID string ,
  PROJECTTYPE string ,
  MODELID string ,
  PROJECTID string ,
  SHAIXUAN string ,
  PRJ_LISTID string ,
  CITY_y string ,
  CITY_NAME string ,
  PRJ_ITEMNAME string ,
  PRJ_LOC string ,
  PRJ_DECORATE string ,
  PRJ_VIEWS string ,
  B_LNG string ,
  B_LAT string ,
  PRICE_AVG double ,
  PRICE_SHOW string ,
  PIC_ID double ,
  PIC_PRJID string ,
  PIC_PRJNAME string ,
  PIC_TYPE string ,
  PIC_DESC string ,
  PIC_TING string ,
  PIC_WEI int ,
  PIC_CHU int ,
  PIC_AREA double ,
  PIC_SELL_POINT string ,
  PIC_HX_TOTALPRICE double ,
  ROOM_ID int ,
  FLATS double ,
  PRICE double ,
  TOTALPRICE double
  )COMMENT 'This is new house table csv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


CREATE  TABLE `newhouselog`(
  id bigint,
  DEVICE_ID string ,
  CONTEXT_ID string ,
  CITY_x string ,
  LOGIN_ACCOUNT double ,
  START_TIME string ,
  END_TIME string ,
  OBJECT_ID double ,
  CHANNEL string ,
  CONTEXT string ,
  ROOMID string ,
  PROJECTTYPE string ,
  MODELID string ,
  PROJECTID string ,
  SHAIXUAN string ,
  PRJ_LISTID string ,
  CITY_y string ,
  CITY_NAME string ,
  PRJ_ITEMNAME string ,
  PRJ_LOC string ,
  PRJ_DECORATE string ,
  PRJ_VIEWS string ,
  B_LNG string ,
  B_LAT string ,
  PRICE_AVG double ,
  PRICE_SHOW string ,
  PIC_ID double ,
  PIC_PRJID string ,
  PIC_PRJNAME string ,
  PIC_TYPE string ,
  PIC_DESC string ,
  PIC_TING string ,
  PIC_WEI int ,
  PIC_CHU int ,
  PIC_AREA double ,
  PIC_SELL_POINT string ,
  PIC_HX_TOTALPRICE double ,
  ROOM_ID double ,
  FLATS double ,
  PRICE double ,
  TOTALPRICE double
  )partitioned by (DATA_DATE string)
STORED AS PARQUET;

insert into newhouselog_test partition (data_date='2019-4-28') select * from newhouselog_csv;