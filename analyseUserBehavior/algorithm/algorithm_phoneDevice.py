import pandas as pd
import redis
import json
from ..config.gobal_config import get_config
import os

file_path = get_config("FILE_PHONEDEVICE_PATH")
redis_phonedevice_prefix = get_config("REDIS_PHONEDEVICE_PREFIX")
redis_host = get_config("REDIS_HOST")
redis_db = get_config("REDIS_DB")


def get_phonedevice_data():
    paths = os.listdir(file_path)
    for path in paths:
        columns = ["DEVICE", "PHONE"]
        df = pd.read_csv(file_path + path, names=columns, header=None,
                         index_col=False)
        df.drop_duplicates(inplace=True)
        return df


def redis_action(df):
    r = redis.Redis(host=redis_host, port=6379, db=redis_db)
    for name, data in df.groupby("PHONE"):
        for value in json.loads(data["DEVICE"].to_json(orient='split', index=False))['data']:
            r.sadd(redis_phonedevice_prefix + str(name), value)


def begin():
    df = get_phonedevice_data()
    redis_action(df)


if __name__ == '__main__':
    begin()
