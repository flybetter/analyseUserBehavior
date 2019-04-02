import pandas as pd
import redis
import json
import re
import numpy as np

elastic_server = ''
michael_server = '192.168.10.221'

r = redis.Redis(host=michael_server, db=4)


def get_sum_price(sum_price, area, avg_price):
    if pd.notna(sum_price):
        return re.sub(u'[\u4E00-\u9FA5]', '', str(sum_price))
    elif pd.notna(area) and pd.notna(avg_price):
        return area * avg_price / 10000
    else:
        return np.NAN


def crm_profile_action(df, result):
    fiter_df = df[df["PRICE_SHOW"].astype(str).str.contains('元/㎡', na=False)]
    result["avg_price"] = fiter_df['PRICE_AVG'].mean()
    result["area"] = df['PIC_AREA'].mean()
    sum_price_df = df.copy()
    sum_price_df.loc[:, 'PIC_HX_TOTALPRICE'] = sum_price_df.apply(
        lambda x: get_sum_price(x['PIC_HX_TOTALPRICE'], x['PIC_AREA'], x['PRICE_AVG']), axis=1)
    result["sum_price"] = pd.to_numeric(sum_price_df['PIC_HX_TOTALPRICE'], errors='coerce').mean()
    result["toliet"] = df['PIC_WEI'].mean()
    result["livingroom"] = df['PIC_TING'].mean()
    di = {8: 1, 9: 2, 10: 3, 11: 4, 21: 5, 22: 6}
    df = df.replace({"PIC_TYPE": di})
    result["bedroom"] = df['PIC_TYPE'].mean()
    result["kitchen"] = df['PIC_CHU'].mean()
    result["count"] = len(df)
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


def custom_result_final(key):
    cities = dict()
    results = list()
    temp_result = dict()
    devices = r.smembers(key)
    for deivce in devices:
        temp = deivce.decode('utf-8')
        datas = r.lrange("NHLOG^" + temp, 0, 30)
        for data in datas:
            results.extend(json.loads(data.decode('utf-8')))

    pd_json = json.dumps(results, ensure_ascii=False)
    message = pd.read_json(pd_json, orient='records')
    message.to_csv('demo.csv')

    # for key, value in temp_pd.groupby('CITY_NAME'):
    #     print(key)
    #     print(value)
    #     cities[key] = custom_result(crm_profile_action(value, temp_result))
    #
    # print(json.dumps(cities))


if __name__ == '__main__':
    custom_result_final("PD^")
