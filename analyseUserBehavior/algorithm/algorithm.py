# coding=UTF-8
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from io import StringIO
import datetime
import redis
import pickle

from pyhdfs import HdfsClient

HDFS_NEWHOUSE_PATH = "/recom/test/testNewHouse.txt__f1be1a82_457a_4b75_b5d4_b80f082086a2"
HDFS_NEWHOUSELOG_PATH = "/recom/testLog/testLog.txt__065b387d_c2dc_4b7e_b227_a6d5e0138d5d"

REDIS_HOST = "192.168.10.221"

REMAIN_DAYS = 30


def get_newhouse_data(path=HDFS_NEWHOUSE_PATH):
    # TODO 安卓那边的埋点数据，有数据会丢
    client = HdfsClient(hosts='192.168.10.221:50070')
    data = client.open(path)
    colName = ["PRJ_LISTID", "CHANNEL", "CITY", "CITY_NAME", "PRJ_ITEMNAME", "PRJ_LOC", "PRJ_DECORATE", "PRJ_VIEWS",
               "B_LNG", "B_LAT", "PRICE_AVG"]
    df = pd.read_csv(StringIO(data.read().decode('utf-8')), names=colName, header=None, delimiter="\t",
                     dtype={'B_LNG': np.str, 'B_LAT': np.str}, na_values="null")
    return df


def get_newhouselog_data(path=HDFS_NEWHOUSELOG_PATH):
    client = HdfsClient(hosts='192.168.10.221:50070')
    data = client.open(path)
    colName = ["DEVICE_ID", "CONTEXT_ID", "CITY", "DATA_DATE", "LOGIN_ACCOUNT"]
    df = pd.read_csv(StringIO(data.read().decode('utf-8')), names=colName, header=None, delimiter="\t",
                     parse_dates=["DATA_DATE"], na_values="null")
    df["CHANNEL"], df["CONTEXT"] = df["CONTEXT_ID"].str.split('-', 1).str
    df["CHANNEL"] = df["CHANNEL"].astype("int64")
    df["CONTEXT"] = df["CONTEXT"].astype("int64")
    return df


def merge_newhouse(df_newhouse, df_newhouselog):
    df = pd.merge(left=df_newhouselog, right=df_newhouse, how="left",
                  left_on=['CITY', 'CHANNEL', 'CONTEXT'],
                  right_on=['CITY_NAME', 'CHANNEL', 'PRJ_LISTID'])
    return df


def preparation(df):
    df.sort_values(["DEVICE_ID", "DATA_DATE"], ascending=[True, True], inplace=True)
    df.to_csv("demo.csv", index=False)
    return df


def redis_action(df=None):
    df = pd.read_csv("demo_short.csv")
    for device_id, data in df.groupby("DEVICE_ID"):
        print(device_id)
        for date, values in data.groupby("DATA_DATE"):
            print(date)
            print(values.to_json(orient="records", force_ascii=False))
            redis_push(device_id, values.to_json(orient="records", force_ascii=False))


def redis_push(name, value):
    r = redis.Redis(host=REDIS_HOST, port=6379)
    r.lpush(name, value)
    num = r.llen(name)
    if num > 30:
        r.rpop(name)


if __name__ == '__main__':
    # df_newhouselog = get_newhouselog_data()
    # df_newhouse = get_newhouse_data()
    # df_merge_data = merge_newhouse(df_newhouse, df_newhouselog)
    # df_preparation = preparation(df_merge_data)
    redis_action()
