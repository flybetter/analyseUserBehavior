CREATE TABLE `user_profile_perfect` (
  `B_LAT` varchar(255) DEFAULT NULL,
  `B_LNG` varchar(255) DEFAULT NULL,
  `CHANNEL` int(5) DEFAULT NULL,
  `CITY_NAME` varchar(20) DEFAULT NULL,
  `CITY_x` varchar(20) DEFAULT NULL,
  `CITY_y` varchar(20) DEFAULT NULL,
  `CONTEXT` varchar(255) DEFAULT NULL,
  `CONTEXT_ID` varchar(255) DEFAULT NULL,
  `DATA_DATE` varchar(255) DEFAULT NULL,
  `DEVICE_ID` varchar(255) DEFAULT NULL,
  `END_TIME` datetime DEFAULT NULL,
  `FLATS` int(5) DEFAULT NULL,
  `LOGIN_ACCOUNT` varchar(255) DEFAULT NULL,
  `MODELID` varchar(255) DEFAULT NULL,
  `OBJECT_ID` varchar(255) DEFAULT NULL,
  `PIC_AREA` double DEFAULT NULL,
  `PIC_CHU` int(5) DEFAULT NULL,
  `PIC_DESC` text,
  `PIC_HX_TOTALPRICE` varchar(255) DEFAULT NULL,
  `PIC_ID` varchar(255) DEFAULT NULL,
  `PIC_PRJID` varchar(255) DEFAULT NULL,
  `PIC_PRJNAME` varchar(255) DEFAULT NULL,
  `PIC_SELL_POINT` varchar(255) DEFAULT NULL,
  `PIC_TING` varchar(255) DEFAULT NULL,
  `PIC_TYPE` int(5) DEFAULT NULL,
  `PIC_WEI` int(5) DEFAULT NULL,
  `PRICE` double DEFAULT NULL,
  `PRICE_AVG` bigint(20) DEFAULT NULL,
  `PRICE_SHOW` varchar(255) DEFAULT NULL,
  `PRJ_DECORATE` varchar(255) DEFAULT NULL,
  `PRJ_ITEMNAME` varchar(255) DEFAULT NULL,
  `PRJ_LISTID` bigint(20) DEFAULT NULL,
  `PRJ_LOC` varchar(255) DEFAULT NULL,
  `PRJ_VIEWS` varchar(255) DEFAULT NULL,
  `PROJECTID` bigint(20) DEFAULT NULL,
  `PROJECTTYPE` bigint(20) DEFAULT NULL,
  `ROOMID` varchar(255) DEFAULT NULL,
  `ROOM_ID` varchar(255) DEFAULT NULL,
  `SHAIXUAN` varchar(255) DEFAULT NULL,
  `START_TIME` datetime DEFAULT NULL,
  `TOTALPRICE` double DEFAULT NULL,
  `CONTENT` varchar(1024) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8

alter table user_profile_perfect add column content varchar (255);

alter table user_profile_perfect add column human_date varchar (255);


update user_profile_perfect set human_date= from_unixtime(DATA_DATE/1000,'%Y-%m-%d');
alter table user_profile_perfect drop  column CONTENT;
alter table user_profile_perfect drop  column DATA_DATE;

alter table user_profile_perfect change column human_date DATA_DATE varchar (255)