# coding=UTF-8
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from io import StringIO

from pyhdfs import HdfsClient

HDFS_NEWHOUSE_PATH = "/recom/test/testFile.txt__96ec6d96_4288_45ae_9d66_de2afca6b1f0"
HDFS_NEWHOUSELOG_PATH = "/recom/testLog/testLog.txt__f08e1fd1_87a1_4653_b8ac_5ab0e41ec9a6"


def get_newhouse_data(path=HDFS_NEWHOUSE_PATH):
    client = HdfsClient(hosts='192.168.10.221:50070')
    data = client.open(path)
    colName = ["PRJ_LISTID", "CHANNEL", "CITY", "CITY_NAME", "PRJ_ITEMNAME", "PRJ_LOC", "PRJ_DECORATE", "PRJ_VIEWS",
               "B_LNG", "B_LAT", "PRICE_AVG"]
    df = pd.read_csv(StringIO(data.read().decode('utf-8')), names=colName, header=None, delimiter="\t")
    return df


def get_newhouselog_data(path=HDFS_NEWHOUSELOG_PATH):
    client = HdfsClient(hosts='192.168.10.221:50070')
    data = client.open(path)
    colName = ["DEVICE_ID", "CONTEXT_ID", "CITY", "DATA_DATE"]
    df = pd.read_csv(StringIO(data.read().decode('utf-8')), names=colName, header=None, delimiter="\t")
    df["CHANNEL"], df["CONTEXT"] = df["CONTEXT_ID"].str.split('-', 1).str
    df["CHANNEL"] = df["CHANNEL"].astype("int64")
    df["CONTEXT"] = df["CONTEXT"].astype("int64")
    return df


def merge_newhouse(df_newhouse, df_newhouselog):
    df_detail = pd.merge(left=df_newhouselog, right=df_newhouse, how="left",
                         left_on=['CITY', 'CHANNEL', 'CONTEXT'],
                         right_on=['CITY_NAME', 'CHANNEL', 'PRJ_LISTID'])
    print(df_detail.iloc[0])


if __name__ == '__main__':
    df_newhouselog = get_newhouselog_data()
    df_newhouse = get_newhouse_data()
    merge_newhouse(df_newhouse, df_newhouselog)
