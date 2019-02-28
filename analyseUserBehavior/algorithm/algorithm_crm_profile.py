from analyseUserBehavior.algorithm import *

crm_pool = redis.ConnectionPool(host=REDIS_HOST, db=REDIS_CRM_DB)
crm_r = redis.Redis(pool=crm_pool)

pool = redis.ConnectionPool(host=REDIS_HOST, db=REDIS_DB)
offical_r = redis.Redis(pool=pool)


def begin():
    df = get_crm_profile_data()
    for key in offical_r.scan_iter(REDIS_PHONEDEVICE_PREFIX + '*', 5000):
        re_data = re.match(CRM_REGULAR, key)
        if re_data:
            phone = re_data.group(1)
            result = get_crm_profile_detail(phone, df)
            # TODO subprocess
            get_crm_house_data(phone, result)


def get_crm_house_data(phone, result):
    data = list()
    for devices in offical_r.smembers(REDIS_PHONEDEVICE_PREFIX + phone):
        for device in devices:
            json_datas = offical_r.lrange(REDIS_NEWHOUSE_PREFIX + device, 0, REMAIN_DAYS)
            if len(json) > 0:
                for json_data in json_datas:
                    data.extend(json_data.loads(json_data.decode('utf-8')))
    result = crm_profile_action(data, result)
    redis_save(phone, result)


def crm_profile_action(data, result):
    result_json = json.dumps(data, ensure_ascii=False)
    df = pd.read_json(result_json, orient='records')
    fiter_df = df[df['PRICE_SHOW'].str.contains('元/㎡', na=False)]
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
    return result


def get_crm_profile_data():
    paths = os.listdir(FILE_CRM_USER_PATH)
    for path in paths:
        columns = ["ID", "IDCARD", "PHONE"]
        df = pd.read_csv(FILE_PHONEDEVICE_PATH + path, names=columns, header=None,
                         index_col=False, low_memory=False)
        df.drop_duplicates(inplace=True)
        return df


def get_crm_profile_detail(phone, df):
    result = dict()
    df_result = df[df['PHONE'] == phone]
    if len(df_result) > 0:
        result['USERID'] = df_result['ID']
        result['IDCARD'] = df_result['IDCARD']
    return result


def redis_save(phone, result):
    crm_r.set(REDIS_CRM_PREFIX + phone, json.dumps(result))


if __name__ == '__main__':
    # df_user = get_crm_profile_data()
    data = re.match(r'PD\^(\d+)', 'PD^13357708210')
    print(data.group(1))
