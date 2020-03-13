from analyseUserBehavior.algorithm import *
import multiprocessing as mp
from datetime import datetime
import copy
import urllib
from datetime import timedelta


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
    def __init__(self, redis_host=REDIS_HOST, redis_crm_host=REDIS_CRM_HOST, redis_crm_db=REDIS_CRM_DB,
                 redis_db=REDIS_DB, redis_phone_devices_db=REDIS_PHONE_DEVICES_DB):
        crm_pool = redis.ConnectionPool(host=redis_crm_host, db=redis_crm_db)
        self.crm_r = redis.Redis(connection_pool=crm_pool)

        pool = redis.ConnectionPool(host=redis_host, db=redis_db)
        self.offical_r = redis.Redis(connection_pool=pool)

        devices_pool = redis.ConnectionPool(host=redis_host, db=redis_phone_devices_db)
        self.devices_r = redis.Redis(connection_pool=devices_pool)

        self.crm_profile_dict = dict()

        self.companyCount = self.companyHouse_url()

    def begin(self):
        df = self.get_crm_profile_data()
        # df = self.get_custom_crm_profile_data()
        for key in self.devices_r.scan_iter(match=REDIS_PHONEDEVICE_PREFIX + '*', count=500):
            re_data = re.match(CRM_REGULAR, key.decode('utf-8'))
            if re_data:
                phone = re_data.group(1)
                result = self.get_crm_profile_detail(phone, df)
                result = self.get_crm_house_data(phone, result)
                if len(result) > 0:
                    self.redis_pipline_save_sample(REDIS_CRM_PREFIX + phone, result)
                    # self.crm_profile_dict[] = result

        # self.redis_pipline_save()

    def get_crm_house_data(self, phone, result):
        data = list()
        for device in self.devices_r.smembers(REDIS_PHONEDEVICE_PREFIX + phone):
            json_datas = self.offical_r.lrange(REDIS_NEWHOUSE_PREFIX + device.decode('utf-8'), 0, REMAIN_DAYS)
            if len(json_datas) > 0:
                for json_data in json_datas:
                    data.extend(json.loads(json_data.decode('utf-8')))
        if len(data) > 0:
            # print("phone:" + phone)
            result_json = json.dumps(data, ensure_ascii=False)
            df = pd.read_json(result_json, orient='records', dtype={'PRJ_ITEMNAME': np.str}).dropna(
                subset=['B_LAT', 'B_LNG', 'PRJ_ITEMNAME'])
            cities = dict()
            for key, value in df.groupby('CITY_NAME'):
                cities[key] = CrmProfile.custom_result(self.crm_profile_action(value, copy.deepcopy(result)))
            return cities
            # print("cities:" + json.dumps(cities))
        else:
            return data

    @staticmethod
    def get_sum_price(sum_price, area, avg_price):
        if pd.notna(sum_price):
            return re.sub(u'[\u4E00-\u9FA5]', '', str(sum_price))
        elif pd.notna(area) and pd.notna(avg_price):
            return area * avg_price / 10000
        else:
            return np.NAN

    @operator_status
    def crm_profile_action(self, df, result):
        fiter_df = df[df["PRICE_SHOW"].astype(str).str.contains('元/㎡', na=False)]
        result["avg_price"] = fiter_df['PRICE_AVG'].mean()
        result["area"] = df['PIC_AREA'].mean()
        sum_price_df = df.copy()
        sum_price_df.loc[:, 'PIC_HX_TOTALPRICE'] = sum_price_df.apply(
            lambda x: CrmProfile.get_sum_price(x['PIC_HX_TOTALPRICE'], x['PIC_AREA'], x['PRICE_AVG']), axis=1)
        result["sum_price"] = pd.to_numeric(sum_price_df['PIC_HX_TOTALPRICE'], errors='coerce').mean()
        result["toliet"] = df['PIC_WEI'].mean()
        result["livingroom"] = df['PIC_TING'].mean()
        di = {8: 1, 9: 2, 10: 3, 11: 4, 21: 5, 22: 6}
        df = df.replace({"PIC_TYPE": di})
        result["bedroom"] = df['PIC_TYPE'].mean()
        result["kitchen"] = df['PIC_CHU'].mean()

        # get the latest 30days result
        boundary = datetime.now() - timedelta(days=30)
        datas = df[df['START_TIME'] > boundary].copy()
        result["count"] = len(datas)
        company_df = datas[datas['PRJ_ITEMNAME'].isin(self.companyCount)]
        result["companyCount"] = len(company_df)

        if len(df) > 0:
            df_result = df.sort_values(by='START_TIME', ascending=False)
            df_count = df_result.groupby('CONTEXT_ID').size().reset_index(name='COUNT')
            df_order = df_result.groupby('CONTEXT_ID').nth(0)
            datas = df_order.merge(df_count, how='left', on='CONTEXT_ID')
            datas.sort_values(by='COUNT', ascending=False, inplace=True)
            # result['top_item_name'] = df['PRJ_ITEMNAME'].value_counts().index.get_values()[0]
            result["top_item_name"] = datas.iloc[0, 31]
            datas.sort_values(by='START_TIME', ascending=False)
            result['latest_time'] = str(datas.iloc[0, 40])[0:19]
        else:
            result["top_item_name"] = np.NAN
        return result

    @staticmethod
    def get_crm_profile_data():
        paths = os.listdir(FILE_CRM_USER_PATH)
        for path in paths:
            columns = ["ID", "IDCARD", "PHONE"]
            df = pd.read_csv(FILE_CRM_USER_PATH + path, names=columns, header=None,
                             index_col=False, low_memory=False, dtype={'PHONE': str})
            df.drop_duplicates(inplace=True)
            return df

    @staticmethod
    def get_custom_crm_profile_data():
        columns = ["ID", "IDCARD", "PHONE"]
        df = pd.read_csv(r'/Users/michael/Downloads/crmUser.txt', names=columns, header=None,
                         index_col=False, low_memory=False)
        df.drop_duplicates(inplace=True)
        return df

    def get_crm_profile_detail(self, phone, df):
        result = dict()
        df_result = df[df["PHONE"].astype(str).str.contains(phone)]
        if len(df_result) > 0:
            result["userId"] = df_result.iloc[0]['ID'].astype(str)
            result["IDCard"] = df_result.iloc[0]['IDCARD']
        else:
            result["userId"] = np.NAN
            result["IDCard"] = np.NAN
        return result

    def redis_save(self, phone, result):
        print('phone:{0},value:{1}'.format(phone, json.dumps(result, ensure_ascii=False)))
        self.crm_r.set(REDIS_CRM_PREFIX + phone, json.dumps(result, ensure_ascii=False))

    def redis_pipline_save(self):
        # print('redis_pipline_save')
        with self.crm_r.pipeline(transaction=False) as pipe:
            for (k, v) in self.crm_profile_dict.items():
                pipe.hmset(k, v)
            pipe.execute()

    def redis_pipline_save_sample(self, key, mapping):
        print('redis_pipline_save_sample,key:' + key)
        with self.crm_r.pipeline(transaction=False) as pipe:
            pipe.hmset(key, mapping)
            pipe.execute()

    @staticmethod
    def custom_result(result):
        """
        to replace the np.Nan to null and round the float value in result
        :param result:
        :return:
        """
        for (k, v) in result.items():
            if pd.isna(v):
                result[k] = None
            elif re.match(r"^[-+]?[0-9]+\.[0-9]+$", str(v)) is not None:
                result[k] = int(round(v))

        return json.dumps(result, ensure_ascii=False)

    def companyHouse_url(self):
        url = 'http://crm.house365.com/api/directProject/projects'
        response = urllib.request.urlopen(url)
        jsonBody = json.loads(response.read().decode(encoding='utf-8'))
        company = list(map(lambda x: x['project_name'], jsonBody))
        print(json.dumps(company, ensure_ascii=False))
        return company


def begin():
    crm_profile = CrmProfile()
    crm_profile.begin()


def companyhouse_url():
    url = 'http://crm.house365.com/api/directProject/projects'
    response = urllib.request.urlopen(url)
    jsonBody = json.loads(response.read().decode(encoding='utf-8'))
    company = list(map(lambda x: x['project_name'], jsonBody))
    print(json.dumps(company, ensure_ascii=False))
    return company


if __name__ == '__main__':
    # start_time = datetime.now()
    # print("start time :" + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    # begin()
    # end_time = datetime.now()
    # print("end time:" + end_time.strftime('%Y-%m-%d %H:%M:%S') + " cost time:" + str((end_time - start_time).seconds))
    companyhouse_url()
