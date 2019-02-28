from analyseUserBehavior.algorithm import *


def custom(df):
    df['CONTEXT'] = df['CONTEXT_ID'].split('-', 1)[1] if '-' in df['CONTEXT_ID'] else df['CONTEXT_ID']
    return df


def get_secondhouselog_data(file_path=FILE_SECONDHOUSELOG_PATH):
    paths = os.listdir(file_path)
    for path in paths:
        print(path)
        colName = ["DEVICE_ID", "CONTEXT_ID", "CITY", "DATA_DATE", "LOGIN_ACCOUNT", "START_TIME", "END_TIME", "CONTENT",
                   "OBJECT_ID"]
        df = pd.read_csv(file_path + path, names=colName, header=None,
                         dtype={'LOGIN_ACCOUNT': np.str, 'DATA_DATE': np.str}, low_memory=False)
        df['DATA_DATE'] = pd.to_datetime(df['DATA_DATE'], format='%Y%m%d', errors='coerce')
        df = df.dropna(subset=['DATA_DATE'])
        df['START_TIME'] = pd.to_datetime(df['START_TIME'], errors='coerce')
        df = df.dropna(subset=['START_TIME'])
        df['END_TIME'] = pd.to_datetime(df['END_TIME'], errors='coerce')
        df = df.dropna(subset=['END_TIME'])
        df = df.apply(custom, axis=1)
        return df


def get_secondhouse_data(file_path=FILE_SECONDHOUSE_PATH):
    paths = os.listdir(file_path)
    for path in paths:
        print(path)
        colName = ["SECONDHOUSE_ID", "ESTA", "DISTRICT", "ADDRESS", "STREETID", "BLOCKID", "ISREAL", "BLOCKSHOWNAME",
                   "MRIGHT",
                   "PURPOSE", "STRUCTURE", "BUILDTYPE", "BUILDYEAR", "BUILDAREA", "GARDENAREA", "SUBFLOOR", "FLOOR",
                   "TOTALFLOOR", "ROOM", "HALL", "TOILET", "KITCHEN", "BALCONY", "FORWARD", "PRICE", "AVERPRICE",
                   "PRICETERM", "PRICETYPE", "BASESERVICE", "EQUIPMENT", "ENVIRONMENT", "TRAFFIC", "FITMENT",
                   "SERVERCO", "CONTACTOR", "TELNO", "MOBILE", "PIC1", "CREATTIME", "UPDATETIME", "EXPIRETIME"]
        df = pd.read_csv(file_path + path, names=colName,
                         low_memory=False, dtype={'SECONDHOUSE_ID': object, 'BLOCKID': object})
        return df


def get_block_data(file_path=FILE_BLOCK_PATH):
    paths = os.listdir(file_path)
    for path in paths:
        print(path)
        colName = ["CITY_NAME", "BLOCK_ID", "BLOCKNAME", "DISTRICT", "STREETID", "AREA", "ADDRESS", "BUS", "AVERPRICE",
                   "UPDATEPRICE", "FORUMID", "NEWHOUSEID", "B_MAP_X", "B_MAP_Y", "ACCURACY", "MAP_TEST",
                   "B_PROPERTY_TYPE", "B_GREEN", "B_PARKING", "B_DEVELOPERS", "B_PROPERTY_COMPANY", "B_PROPERTY_FEES",
                   "B_BUS", "B_METRO", "B_NUM", "BI_S", "BI_SPELL", "SUBWAY", "SITENAME", "SUBWAYRANGE", "APP", "ESTA",
                   "PROPERTY_FEES", "NOFEE", "PLOT_RATIO", "TOTAL_ROOM", "TURN_TIME", "B_AREA", "FEATURE"]
        df = pd.read_csv(file_path + path, names=colName, low_memory=False,
                         dtype={'ID': object, 'B_MAP_X': np.str, 'B_MAP_Y': np.str, 'BLOCK_ID': object})
        return df


def merge_secondhouse(df_secondhouse, df_secondhouselog, df_block):
    df = pd.merge(left=df_secondhouselog, right=df_secondhouse, how="left",
                  left_on='CONTEXT',
                  right_on='SECONDHOUSE_ID')
    df = df.merge(df_block, left_on=['CITY', 'BLOCKID'], right_on=['CITY_NAME', 'BLOCK_ID'], how='left')
    return df


def preparation(df):
    df.sort_values(["DEVICE_ID", "DATA_DATE"], ascending=[True, True], inplace=True)
    return df


def redis_action(df):
    for device_id, data in df.groupby("DEVICE_ID"):
        for date, values in data.groupby("DATA_DATE"):
            print(values.to_json(orient="records", force_ascii=False))
            redis_push(REDIS_SECONDHOUSE_PREFIX + device_id, values.to_json(orient="records", force_ascii=False))


def redis_push(name, value):
    r = redis.Redis(host=REDIS_HOST, port=6379, db=REDIS_DB)
    r.lpush(name, value)
    num = r.llen(name)
    if num > int(REMAIN_DAYS):
        r.rpop(name)


def begin():
    df_secondhouselog = get_secondhouselog_data()
    df_secondhouse = get_secondhouse_data()
    df_block = get_block_data()
    df_merge_data = merge_secondhouse(df_secondhouse, df_secondhouselog, df_block)
    df_preparation = preparation(df_merge_data)
    redis_action(df_preparation)


if __name__ == '__main__':
    df_secondhouselog = get_secondhouselog_data()
    df_secondhouse = get_secondhouse_data()
    df_block = get_block_data()
    df_merge_data = merge_secondhouse(df_secondhouse, df_secondhouselog, df_block)
    df_preparation = preparation(df_merge_data)
    print(df_preparation.head(100))
    # redis_action(df_preparation)
