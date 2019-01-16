import pandas as pd
from redis import Redis
from ...config.gobal_config import get_config
import os

if __name__ == '__main__':
    REDIS_PHONEDEVICE_PREFIX = get_config('REDIS_PHONEDEVICE_PREFIX')
    paths = os.listdir(get_config('FILE_INITPHONEDEVICE_PATH'))
    REDIS_HOST = get_config('REDIS_HOST')
    REDIS_DB = get_config('REDIS_DB')
    redis = Redis(host=REDIS_HOST, db=REDIS_DB)
    for path in paths:
        df = pd.read_csv(path, names=['PHONE', 'DEVICE_ID'], )
        for name, grouped in df.groupby(['PHONE']):
            print('')
