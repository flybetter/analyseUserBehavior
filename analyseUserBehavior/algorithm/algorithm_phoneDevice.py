from analyseUserBehavior.algorithm import *


def get_phonedevice_data():
    paths = os.listdir(FILE_PHONEDEVICE_PATH)
    for path in paths:
        columns = ["DEVICE", "PHONE"]
        df = pd.read_csv(FILE_PHONEDEVICE_PATH + path, names=columns, header=None,
                         index_col=False, low_memory=False)
        df.drop_duplicates(inplace=True)
        df = df[df['PHONE'].str.startswith('1', na=False)]
        return df


def redis_action(df):
    devices_pool = redis.ConnectionPool(host=REDIS_HOST, db=REDIS_PHONE_DEVICES_DB)
    devices_r = redis.Redis(connection_pool=devices_pool)
    phone_devices = dict()
    for phone, data in df.groupby("PHONE"):
        phone_devices[REDIS_PHONEDEVICE_PREFIX + phone] = set(data['DEVICE'])

    with devices_r.pipeline(transaction=False) as p:
        for k, v in phone_devices.items():
            p.sadd(k, *v)
        p.execute()


def init():
    redis_deivceids = list()
    offical_pool = redis.ConnectionPool(host=REDIS_HOST, db=REDIS_DB)
    offical_r = redis.Redis(connection_pool=offical_pool)
    for key in offical_r.scan_iter(match=REDIS_NEWHOUSE_PREFIX + "*", count=500):
        deviceid = re.match(NHLOG_REGULAR, key.decode('utf=8')).group(1)
        redis_deivceids.append(deviceid)

    df = get_phonedevice_data()
    filter_df = df[df['DEVICE'].isin(redis_deivceids)]
    redis_action(filter_df)


def begin():
    df = get_phonedevice_data()
    redis_action(df)


if __name__ == '__main__':
    begin()
