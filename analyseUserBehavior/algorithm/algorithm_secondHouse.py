# coding=UTF-8
import pandas as pd
import numpy as np
import redis
import os
import json
from ..config.gobal_config import get_config

file_secondhouse_path = get_config('FILE_SECONDHOUSE_PATH')
file_secondhouselog_path = get_config('FILE_SECONDHOUSELOG_PATH')
file_block_path = get_config('FILE_BLOCK_PATH')
redis_host = get_config('REDIS_HOST')
remain_days = get_config('REMAIN_DAYS')
redis_secondhouse_prefix = get_config('REDIS_SECONDHOUSE_PREFIX')
redis_db = get_config("REDIS_DB")


def custom(df):
    df['CONTEXT'] = df['CONTEXT_ID'].split('-', 1)[1] if '-' in df['CONTEXT_ID'] else df['CONTEXT_ID']
    return df


def get_secondhouselog_data(file_path=file_secondhouselog_path):
    paths = os.listdir(file_path)
    for path in paths:
        print(path)
        colName = ["DEVICE_ID", "CONTEXT_ID", "CITY", "DATA_DATE", "LOGIN_ACCOUNT", "START_TIME", "END_TIME", "CONTENT",
                   "OBJECT_ID"]
        df = pd.read_csv(file_path + path, names=colName, header=None,
                         dtype={'LOGIN_ACCOUNT': np.str, 'DATA_DATE': np.str}, low_memory=False)
        df['DATA_DATE'] = pd.to_datetime(df['DATA_DATE'], format='%Y%m%d', errors='coerce')
        df = df.dropna(subset=['DATA_DATE'])
        df['START_TIME'] = pd.to_datetime(df['START_TIME'], errors='coerce')
        df = df.dropna(subset=['START_TIME'])
        df['END_TIME'] = pd.to_datetime(df['END_TIME'], errors='coerce')
        df = df.dropna(subset=['END_TIME'])
        df = df.apply(custom, axis=1)
        return df


def get_secondhouse_data(file_path=file_secondhouse_path):
    paths = os.listdir(file_path)
    for path in paths:
        print(path)
        colName = ["SECONDHOUSE_ID", "ESTA", "DISTRICT", "ADDRESS", "STREETID", "BLOCKID", "ISREAL", "BLOCKSHOWNAME",
                   "MRIGHT",
                   "PURPOSE", "STRUCTURE", "BUILDTYPE", "BUILDYEAR", "BUILDAREA", "GARDENAREA", "SUBFLOOR", "FLOOR",
                   "TOTALFLOOR", "ROOM", "HALL", "TOILET", "KITCHEN", "BALCONY", "FORWARD", "PRICE", "AVERPRICE",
                   "PRICETERM", "PRICETYPE", "BASESERVICE", "EQUIPMENT", "ENVIRONMENT", "TRAFFIC", "FITMENT",
                   "SERVERCO", "CONTACTOR", "TELNO", "MOBILE", "PIC1", "CREATTIME", "UPDATETIME", "EXPIRETIME"]
        df = pd.read_csv(file_path + path, names=colName,
                         low_memory=False, dtype={'SECONDHOUSE_ID': object, 'BLOCKID': object})
        return df


def get_block_data(file_path=file_block_path):
    paths = os.listdir(file_path)
    for path in paths:
        print(path)
        colName = ["CITY_NAME", "BLOCK_ID", "BLOCKNAME", "DISTRICT", "STREETID", "AREA", "ADDRESS", "BUS", "AVERPRICE",
                   "UPDATEPRICE", "FORUMID", "NEWHOUSEID", "B_MAP_X", "B_MAP_Y", "ACCURACY", "MAP_TEST",
                   "B_PROPERTY_TYPE", "B_GREEN", "B_PARKING", "B_DEVELOPERS", "B_PROPERTY_COMPANY", "B_PROPERTY_FEES",
                   "B_BUS", "B_METRO", "B_NUM", "BI_S", "BI_SPELL", "SUBWAY", "SITENAME", "SUBWAYRANGE", "APP", "ESTA",
                   "PROPERTY_FEES", "NOFEE", "PLOT_RATIO", "TOTAL_ROOM", "TURN_TIME", "B_AREA", "FEATURE"]
        df = pd.read_csv(file_path + path, names=colName, low_memory=False,
                         dtype={'ID': object, 'B_MAP_X': np.str, 'B_MAP_Y': np.str})
        return df


def merge_secondhouse(df_secondhouse, df_secondhouselog, df_block):
    df = pd.merge(left=df_secondhouselog, right=df_secondhouse, how="left",
                  left_on='CONTEXT',
                  right_on='SECONDHOUSE_ID')
    df = df.merge(df_block, left_on=['CITY', 'BLOCKID'], right_on=['CITY_NAME', 'BLOCK_ID'], how='left')
    return df


def preparation(df):
    df.sort_values(["DEVICE_ID", "DATA_DATE"], ascending=[True, True], inplace=True)
    return df


def redis_action(df):
    for device_id, data in df.groupby("DEVICE_ID"):
        for date, values in data.groupby("DATA_DATE"):
            print(values.to_json(orient="records", force_ascii=False))
            redis_push(redis_secondhouse_prefix + device_id, values.to_json(orient="records", force_ascii=False))


def redis_push(name, value):
    r = redis.Redis(host=redis_host, port=6379, db=redis_db)
    r.lpush(name, value)
    num = r.llen(name)
    if num > int(remain_days):
        r.rpop(name)


def begin():
    df_secondhouselog = get_secondhouselog_data()
    df_secondhouse = get_secondhouse_data()
    df_block = get_block_data()
    df_merge_data = merge_secondhouse(df_secondhouse, df_secondhouselog, df_block)
    df_preparation = preparation(df_merge_data)
    redis_action(df_preparation)


if __name__ == '__main__':
    df_secondhouselog = get_secondhouselog_data()
    df_secondhouse = get_secondhouse_data()
    df_block = get_block_data()
    df_merge_data = merge_secondhouse(df_secondhouse, df_secondhouselog, df_block)
    df_preparation = preparation(df_merge_data)
    print(df_preparation.head(100))
    # redis_action(df_preparation)
