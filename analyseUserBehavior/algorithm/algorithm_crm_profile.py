from analyseUserBehavior.algorithm import *
import multiprocessing as mp
from datetime import datetime


def operator_status(func):
    """

    :param func: decorator
    :return:
    """

    def get_status(*args, **kwargs):
        global func_result
        try:
            func_result = func(*args, **kwargs)
        except Exception as e:
            traceback.print_exc()
        return func_result

    return get_status


class CrmProfile:
    def __init__(self, redis_host=REDIS_HOST, redis_crm_db=REDIS_CRM_DB, redis_db=REDIS_DB):
        crm_pool = redis.ConnectionPool(host=redis_host, db=redis_crm_db)
        self.crm_r = redis.Redis(connection_pool=crm_pool)

        pool = redis.ConnectionPool(host=redis_host, db=redis_db)
        self.offical_r = redis.Redis(connection_pool=pool)

        self.crm_profile_dict = dict()

    def begin(self):
        df = self.get_crm_profile_data()
        # pool = mp.Pool(processes=10)
        for key in self.offical_r.scan_iter(match=REDIS_PHONEDEVICE_PREFIX + '*', count=500):
            re_data = re.match(CRM_REGULAR, key.decode('utf-8'))
            if re_data:
                phone = re_data.group(1)
                result = self.get_crm_profile_detail(phone, df)
                result = self.get_crm_house_data(phone, result)
                if len(result) > 0:
                    self.crm_profile_dict[REDIS_CRM_PREFIX + phone] = result

        self.redis_pipline_save()

    def get_crm_house_data(self, phone, result):
        data = list()
        for device in self.offical_r.smembers(REDIS_PHONEDEVICE_PREFIX + phone):
            json_datas = self.offical_r.lrange(REDIS_NEWHOUSE_PREFIX + device.decode('utf-8'), 0, REMAIN_DAYS)
            if len(json_datas) > 0:
                for json_data in json_datas:
                    data.extend(json.loads(json_data.decode('utf-8')))
        if len(data) > 0:
            result_json = json.dumps(data, ensure_ascii=False)
            df = pd.read_json(result_json, orient='records', dtype={'PRJ_ITEMNAME': np.str}).dropna(
                subset=['B_LAT', 'B_LNG', 'PRJ_ITEMNAME'])
            cities = dict()
            for key, value in df.groupby('CITY_NAME'):
                cities[key] = self.crm_profile_action(value, result)
            return cities
        else:
            return data

    @operator_status
    def crm_profile_action(self, df, result):
        fiter_df = df[df['PRICE_SHOW'].astype(str).str.contains('元/㎡', na=False)]
        result['avg_price'] = fiter_df['PRICE_AVG'].mean()
        result['area'] = df['PIC_AREA'].mean()
        result['sum_price'] = pd.to_numeric(
            df['PIC_HX_TOTALPRICE'].astype(str).map(lambda x: re.sub(u'[\u4E00-\u9FA5]', '', x)),
            errors='coerce').mean()
        result['toliet'] = df['PIC_WEI'].mean()
        result['livingroom'] = df['PIC_TING'].mean()
        di = {8: 1, 9: 2, 10: 3, 11: 4, 21: 5, 22: 6}
        df = df.replace({"PIC_TYPE": di})
        result['bedroom'] = df['PIC_TYPE'].mean()
        result['kitchen'] = df['PIC_CHU'].mean()
        if len(df) > 0:
            df = df.sort_values(by='START_TIME', ascending=False)
            result['top_item_name'] = df['PRJ_ITEMNAME'].value_counts().index.get_values()[0]
        else:
            result['top_item_name'] = np.NAN
        return result

    def get_crm_profile_data(self):
        paths = os.listdir(FILE_CRM_USER_PATH)
        for path in paths:
            columns = ["ID", "IDCARD", "PHONE"]
            df = pd.read_csv(FILE_CRM_USER_PATH + path, names=columns, header=None,
                             index_col=False, low_memory=False, dtype={'PHONE': str})
            df.drop_duplicates(inplace=True)
            return df

    def get_custom_crm_profile_data(self):
        columns = ["ID", "IDCARD", "PHONE"]
        df = pd.read_csv(r'/Users/michael/Downloads/crmUser.txt', names=columns, header=None,
                         index_col=False, low_memory=False)
        df.drop_duplicates(inplace=True)
        return df

    def get_crm_profile_detail(self, phone, df):
        result = dict()
        df_result = df[df['PHONE'].astype(str).str.contains(phone)]
        if len(df_result) > 0:
            result['userId'] = df_result.iloc[0]['ID'].astype(str)
            result['IDCard'] = df_result.iloc[0]['IDCARD']
        else:
            result['userId'] = np.NAN
            result['IDCard'] = np.NAN
        return result

    def redis_save(self, phone, result):
        print('phone:{0},value:{1}'.format(phone, json.dumps(result, ensure_ascii=False)))
        self.crm_r.set(REDIS_CRM_PREFIX + phone, json.dumps(result, ensure_ascii=False))

    def redis_pipline_save(self):
        with self.crm_r.pipeline(transaction=False) as pipe:
            for (k, v) in self.crm_profile_dict.items():
                pipe.hmset(k, v)
            pipe.execute()


def begin():
    crm_profile = CrmProfile()
    crm_profile.begin()


if __name__ == '__main__':
    start_time = datetime.now()
    print("start time :" + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    crm_profile = CrmProfile()
    crm_profile.begin()
    end_time = datetime.now()
    print("end time:" + end_time.strftime('%Y-%m-%d %H:%M:%S') + " cost time:" + str((end_time - start_time).seconds))
