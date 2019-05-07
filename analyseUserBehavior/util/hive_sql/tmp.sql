INVALIDATE METADATA

select count(*),data_date from newhouselog group by data_date;

insert into newhouselog partition(data_date='2019-04-30') select DEVICE_ID,CONTEXT_ID,CITY_x,LOGIN_ACCOUNT,START_TIME,END_TIME,OBJECT_ID,CHANNEL,CONTEXT,ROOMID,PROJECTTYPE,MODELID,PROJECTID,SHAIXUAN,PRJ_LISTID,CITY_y,CITY_NAME,PRJ_ITEMNAME,PRJ_LOC,PRJ_DECORATE,PRJ_VIEWS,B_LNG,B_LAT,PRICE_AVG,PRICE_SHOW,PIC_ID,PIC_PRJID,PIC_PRJNAME,PIC_TYPE,PIC_DESC,PIC_TING,PIC_WEI,PIC_CHU,PIC_AREA,PIC_SELL_POINT,PIC_HX_TOTALPRICE,ROOM_ID,FLATS,PRICE,TOTALPRICE from user_profile_v4 where data_date='2019-04-30'

alter table user_profile_v4 change OBJECT_ID OBJECT_ID double;

alter table user_profile_v4 change CHANNEL CHANNEL string;

alter table user_profile_v4 change CONTEXT CONTEXT string;

alter table user_profile_v4 change PROJECTID PROJECTID string ;

alter table user_profile_v4 change PROJECTTYPE PROJECTTYPE string ;

alter table user_profile_v4 change SHAIXUAN SHAIXUAN string ;

alter table user_profile_v4 change PRJ_LISTID PRJ_LISTID string ;

alter table user_profile_v4 change pic_prjid pic_prjid string ;

alter table user_profile_v4 change pic_type pic_type string ;

alter table user_profile_v4 change pic_wei pic_wei int ;

alter table user_profile_v4 change pic_ting pic_ting string ;

alter table user_profile_v4 change pic_chu pic_chu int ;

alter table user_profile_v4 change pic_hx_totalprice pic_hx_totalprice double ;



ALTER TABLE user_profile_v4 MODIFY CHANNEL int(5);
ALTER TABLE user_profile_v4 MODIFY CITY_NAME varchar(20);
ALTER TABLE user_profile_v4 MODIFY CITY_x varchar(20);
ALTER TABLE user_profile_v4 MODIFY CITY_y varchar(20);
ALTER TABLE user_profile_v4 MODIFY CONTEXT varchar(20);
ALTER TABLE user_profile_v4 MODIFY CONTEXT_ID varchar(20);
ALTER TABLE user_profile_v4 MODIFY FLATS int(5);
ALTER TABLE user_profile_v4 MODIFY LOGIN_ACCOUNT varchar(255);
ALTER TABLE user_profile_v4 MODIFY MODELID varchar(255);
ALTER TABLE user_profile_v4 MODIFY PIC_CHU int(5);
ALTER TABLE user_profile_v4 MODIFY PIC_HX_TOTALPRICE double;
ALTER TABLE user_profile_v4 MODIFY PIC_ID varchar(255);
ALTER TABLE user_profile_v4 MODIFY PIC_PRJID varchar(255);
ALTER TABLE user_profile_v4 MODIFY PIC_PRJNAME varchar(255);
ALTER TABLE user_profile_v4 MODIFY PIC_SELL_POINT varchar(255);
ALTER TABLE user_profile_v4 MODIFY PIC_TYPE int(5);
ALTER TABLE user_profile_v4 MODIFY PIC_WEI int(5);
ALTER TABLE user_profile_v4 MODIFY PRICE_AVG double;
ALTER TABLE user_profile_v4 MODIFY PRICE_SHOW varchar(255);
ALTER TABLE user_profile_v4 MODIFY PRJ_DECORATE varchar(255);
ALTER TABLE user_profile_v4 MODIFY PRJ_ITEMNAME varchar(255);
ALTER TABLE user_profile_v4 MODIFY PRJ_LOC varchar(255);
ALTER TABLE user_profile_v4 MODIFY ROOMID varchar(255);
ALTER TABLE user_profile_v4 MODIFY ROOM_ID varchar(255);
ALTER TABLE user_profile_v4 MODIFY SHAIXUAN varchar(255);





