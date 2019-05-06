-- hive
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



-- mysql
create table user_profile_final
(
  id  bigint(20) primary key  auto_increment,
  DEVICE_ID         varchar(255) null,
  CONTEXT_ID        text         null,
  CITY_x            text         null,
  LOGIN_ACCOUNT     double       null,
  START_TIME        datetime     null,
  END_TIME          datetime     null,
  CHANNEL           bigint       null,
  OBJECT_ID         double       null,
  CONTEXT           bigint       null,
  ROOMID            text         null,
  PROJECTTYPE       bigint       null,
  MODELID           text         null,
  PROJECTID         bigint       null,
  SHAIXUAN          double       null,
  PRJ_LISTID        bigint       null,
  CITY_y            text         null,
  CITY_NAME         text         null,
  PRJ_ITEMNAME      text         null,
  PRJ_LOC           text         null,
  PRJ_DECORATE      text         null,
  PRJ_VIEWS         varchar(255) null,
  B_LNG             varchar(255) null,
  B_LAT             varchar(255) null,
  PRICE_AVG         bigint       null,
  PRICE_SHOW        text         null,
  PIC_ID            double       null,
  PIC_PRJID         double       null,
  PIC_PRJNAME       text         null,
  PIC_DESC          text         null,
  PIC_TYPE          double       null,
  PIC_TING          varchar(255) null,
  PIC_WEI           double       null,
  PIC_CHU           double       null,
  PIC_AREA          double       null,
  PIC_SELL_POINT    text         null,
  PIC_HX_TOTALPRICE varchar(255) null,
  ROOM_ID           double       null,
  FLATS             double       null,
  PRICE             double       null,
  TOTALPRICE        double       null,
  DATA_DATE         bigint       null
);

insert into newhouselog_test partition(data_date='2019-3-15') select DEVICE_ID,CONTEXT_ID,CITY_x,LOGIN_ACCOUNT,START_TIME,END_TIME,OBJECT_ID,CHANNEL,CONTEXT,ROOMID,PROJECTTYPE,MODELID,PROJECTID,SHAIXUAN,PRJ_LISTID,CITY_y,CITY_NAME,PRJ_ITEMNAME,PRJ_LOC,PRJ_DECORATE,PRJ_VIEWS,B_LNG,B_LAT,PRICE_AVG,PRICE_SHOW,PIC_ID,PIC_PRJID,PIC_PRJNAME,PIC_TYPE,PIC_DESC,PIC_TING,PIC_WEI,PIC_CHU,PIC_AREA,PIC_SELL_POINT,PIC_HX_TOTALPRICE,ROOM_ID,FLATS,PRICE,TOTALPRICE from user_profile_final where data_date='2019-03-15'
