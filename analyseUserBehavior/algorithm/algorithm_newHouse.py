from analyseUserBehavior.algorithm import *
from analyseUserBehavior.algorithm import algorithm_hive_transmission


def custom(df):
    try:
        object = json.loads(df['CONTENT'])
        df['ROOMID'] = object.get('roomId', np.NaN)
        df['PROJECTTYPE'] = object.get('projectType', np.NaN)
        df['MODELID'] = object.get('modelId', np.NaN)
        df['PROJECTID'] = object.get('projectId', np.NaN)
        df['SHAIXUAN'] = object.get('shaiXuan', np.NaN)
        return df
    except:
        df['ROOMID'] = np.NaN
        df['PROJECTTYPE'] = np.NaN
        df['MODELID'] = np.NaN
        df['PROJECTID'] = np.NaN
        df['SHAIXUAN'] = np.NaN
        return df


def get_newhouse_data(file_path=FILE_NEWHOUSE_PATH):
    paths = os.listdir(file_path)
    for path in paths:
        print(file_path)
        colName = ["PRJ_LISTID", "CHANNEL", "CITY", "CITY_NAME", "PRJ_ITEMNAME", "PRJ_LOC", "PRJ_DECORATE", "PRJ_VIEWS",
                   "B_LNG", "B_LAT", "PRICE_AVG", "PRICE_SHOW"]
        df = pd.read_csv(file_path + path, names=colName, header=None,
                         dtype={'B_LNG': np.str, 'B_LAT': np.str, 'PRJ_LISTID': np.int64}, low_memory=False)
        return df


def get_newhouselog_data(file_path=FILE_NEWHOUSELOG_PATH):
    paths = os.listdir(file_path)
    for path in paths:
        print(path)
        colName = ["DEVICE_ID", "CONTEXT_ID", "CITY", "DATA_DATE", "LOGIN_ACCOUNT", "START_TIME", "END_TIME", "CONTENT",
                   "OBJECT_ID"]
        df = pd.read_csv(file_path + path, names=colName, header=None,
                         dtype={'LOGIN_ACCOUNT': np.str, 'DATA_DATE': np.str}, low_memory=False)

        df["CHANNEL"], df["CONTEXT"] = df["CONTEXT_ID"].str.split('-', 1).str
        df["CHANNEL"] = pd.to_numeric(df['CHANNEL'], errors='coerce')
        df['CONTEXT'] = pd.to_numeric(df['CONTEXT'], errors='coerce')
        df['DATA_DATE'] = pd.to_datetime(df['DATA_DATE'], format='%Y%m%d', errors='coerce')
        df = df.dropna(subset=['DATA_DATE'])
        df['START_TIME'] = pd.to_datetime(df['START_TIME'], errors='coerce')
        df = df.dropna(subset=['START_TIME'])
        df['END_TIME'] = pd.to_datetime(df['END_TIME'], errors='coerce')
        df = df.dropna(subset=['END_TIME'])
        df = df.apply(custom, axis=1)
        return df


def get_newhousemodel_data(file_path=FILE_NEWHOUSEMODEL_PATH):
    paths = os.listdir(file_path)
    for path in paths:
        print(path)
        colName = ['PIC_ID', 'PIC_PRJID', 'PIC_PRJNAME', 'PIC_TYPE', 'PIC_DESC', 'PIC_TING', 'PIC_WEI', 'PIC_CHU',
                   'PIC_AREA', 'PIC_SELL_POINT', 'PIC_HX_TOTALPRICE']
        df = pd.read_csv(file_path + path, names=colName, low_memory=False, dtype={'PIC_ID': object})
        return df


def get_newhouseroom_data(file_path=FILE_NEWHOUSEROOM_PATH):
    paths = os.listdir(file_path)
    for path in paths:
        print(path)
        colName = ['ROOM_ID', 'FLATS', 'PRICE', 'TOTALPRICE']
        df = pd.read_csv(file_path + path, names=colName, low_memory=False, dtype={'ROOM_ID': object})
        return df


def merge_newhouse(df_newhouse, df_newhouselog, df_newhousemodel, df_newhouseroom):
    df = pd.merge(left=df_newhouselog, right=df_newhouse, how="left",
                  left_on=['CITY', 'CHANNEL', 'CONTEXT'],
                  right_on=['CITY_NAME', 'CHANNEL', 'PRJ_LISTID'])
    df = df.merge(df_newhousemodel, left_on='MODELID', right_on='PIC_ID', how='left')
    df = df.merge(df_newhouseroom, left_on='ROOMID', right_on='ROOM_ID', how='left')
    return df


def preparation(df):
    df.sort_values(["DEVICE_ID", "DATA_DATE"], ascending=[True, True], inplace=True)
    return df


def redis_action(df):
    for device_id, data in df.groupby("DEVICE_ID"):
        for date, values in data.groupby("DATA_DATE"):
            # print(values.to_json(orient="records", force_ascii=False))
            redis_push(REDIS_NEWHOUSE_PREFIX + device_id, values.to_json(orient="records", force_ascii=False))


def redis_push(name, value):
    r = redis.Redis(host=REDIS_HOST, port=6379, db=REDIS_DB)
    r.lpush(name, value)
    num = r.llen(name)
    if num > int(REMAIN_DAYS):
        r.rpop(name)


def get_sum_price(sum_price, area, avg_price):
    if pd.notna(sum_price):
        return re.sub(u'[\u4E00-\u9FA5]', '', str(sum_price))
    elif pd.notna(area) and pd.notna(avg_price):
        return area * avg_price / 10000
    else:
        return np.NAN


def csv_action(df):
    # df["DATA_DATE"] = pd.to_datetime(df["DATA_DATE"]).dt.date
    df = df.drop(columns=['CONTENT', 'DATA_DATE'])
    df["CHANNEL"] = df["CHANNEL"].astype('uint8')
    df["FLATS"].replace(np.nan, 0, inplace=True)
    df["FLATS"] = df["FLATS"].astype('uint8')
    df["PRJ_LISTID"].replace(np.nan, 0, inplace=True)
    df["PRJ_LISTID"] = df["PRJ_LISTID"].astype('uint32')
    df["PRICE_AVG"].replace(np.nan, 0, inplace=True)
    df["PRICE_AVG"] = df["PRICE_AVG"].astype('uint32')
    df["PIC_TYPE"].replace(np.nan, 0, inplace=True)
    df["PIC_TYPE"] = df["PIC_TYPE"].astype('uint32')
    df["PIC_AREA"] = pd.to_numeric(df['PIC_AREA'], errors='coerce')
    df["PIC_HX_TOTALPRICE"] = df.apply(lambda x: get_sum_price(x['PIC_HX_TOTALPRICE'], x['PIC_AREA'], x['PRICE_AVG']),
                                       axis=1)
    df.to_csv(HIVE_NEWHOUSELOG_CSV_PATH, header=False, index=False)


def begin():
    df_newhouselog = get_newhouselog_data()
    df_newhouse = get_newhouse_data()
    df_newhousemodel = get_newhousemodel_data()
    df_newhouseroom = get_newhouseroom_data()
    df_merge_data = merge_newhouse(df_newhouse, df_newhouselog, df_newhousemodel, df_newhouseroom)
    df_preparation = preparation(df_merge_data)
    csv_action(df_preparation)
    algorithm_hive_transmission.begin(table="newhouselog", table_csv="newhouselog_csv")
    redis_action(df_preparation)


if __name__ == '__main__':
    df_newhouselog = get_newhouselog_data()
    df_newhouse = get_newhouse_data()
    df_newhousemodel = get_newhousemodel_data()
    df_newhouseroom = get_newhouseroom_data()
    df_merge_data = merge_newhouse(df_newhouse, df_newhouselog, df_newhousemodel, df_newhouseroom)
    df_preparation = preparation(df_merge_data)
    csv_action(df_preparation)
    algorithm_hive_transmission.begin(table="newhouselog", table_csv="newhouselog_csv")
    # redis_action(df_preparation)
