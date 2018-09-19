import pandas as pd
from pyhdfs import HdfsClient
import redis
from io import StringIO
import json

REDIS_PHONEDEVICE_PREFIX = 'PD^'

HDFS_PHONEDEVICE_PATH = '/recom/phoneDevice/phoneDevice.txt__c76a247a_03a6_4d1c_a545_57b09569ee47'


def get_phonedevice_data(path=HDFS_PHONEDEVICE_PATH):
    client = HdfsClient(hosts="192.168.10.221:50070")
    data = client.open(path)
    columns = ["DEVICE", "PHONE"]
    df = pd.read_csv(StringIO(data.read().decode('utf-8')), names=columns, header=None, delimiter="\t", index_col=False)
    df.drop_duplicates(inplace=True)
    return df


def redis_action(df):
    r = redis.Redis(host='192.168.10.221', port=6379)
    for name, data in df.groupby("PHONE"):
        print(name)
        for value in json.loads(data["DEVICE"].to_json(orient='split', index=False))['data']:
            print(value)
            r.sadd(REDIS_PHONEDEVICE_PREFIX + str(name), value)


if __name__ == '__main__':
    df = get_phonedevice_data()
    redis_action(df)
