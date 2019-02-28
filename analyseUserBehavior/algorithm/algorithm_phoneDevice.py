from analyseUserBehavior.algorithm import *


def get_phonedevice_data():
    paths = os.listdir(FILE_PHONEDEVICE_PATH)
    for path in paths:
        columns = ["DEVICE", "PHONE"]
        df = pd.read_csv(FILE_PHONEDEVICE_PATH + path, names=columns, header=None,
                         index_col=False, low_memory=False)
        df.drop_duplicates(inplace=True)
        return df


def redis_action(df):
    r = redis.Redis(host=REDIS_HOST, port=6379, db=REDIS_DB)
    for name, data in df.groupby("PHONE"):
        for value in json.loads(data["DEVICE"].to_json(orient='split', index=False))['data']:
            r.sadd(REDIS_PHONEDEVICE_PREFIX + str(name), value)


def begin():
    df = get_phonedevice_data()
    redis_action(df)


if __name__ == '__main__':
    begin()
