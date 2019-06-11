from analyseUserBehavior.algorithm import *
from analyseUserBehavior.algorithm import algorithm_hive_transmission


# from sqlalchemy import create_engine


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
        colName = ["SECONDHOUSE_ID", "CITY_NAME_data", "ESTA", "DISTRICT", "ADDRESS", "STREETID", "BLOCKID", "BLOCKSHOWNAME",
                   "PURPOSE", "STRUCTURE", "BUILDTYPE", "BUILDYEAR", "BUILDAREA", "SUBFLOOR", "FLOOR",
                   "TOTALFLOOR", "ROOM", "HALL", "TOILET", "KITCHEN", "BALCONY", "FORWARD", "PRICE", "AVERPRICE",
                   "ENVIRONMENT", "TRAFFIC", "FITMENT",
                   "SERVERCO", "CONTACTOR", "TELNO", "MOBILE", "CREATTIME", "UPDATETIME", "EXPIRETIME"]
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
                  left_on=['CONTEXT', 'CITY'],
                  right_on=['SECONDHOUSE_ID', 'CITY_NAME_data'])
    df = df.merge(df_block, left_on=['CITY', 'BLOCKID'], right_on=['CITY_NAME', 'BLOCK_ID'], how='left')
    return df


def preparation(df):
    df.sort_values(["DEVICE_ID", "DATA_DATE"], ascending=[True, True], inplace=True)
    return df


def redis_action(df):
    for device_id, data in df.groupby("DEVICE_ID"):
        for date, values in data.groupby("DATA_DATE"):
            # print(values.to_json(orient="records", force_ascii=False))
            redis_push(REDIS_SECONDHOUSE_PREFIX + device_id, values.to_json(orient="records", force_ascii=False))


def redis_push(name, value):
    r = redis.Redis(host=REDIS_HOST, port=6379, db=REDIS_DB)
    r.lpush(name, value)
    num = r.llen(name)
    if num > int(REMAIN_DAYS):
        r.rpop(name)


def csv_action(df):
    # df["DATA_DATE"] = pd.to_datetime(df["DATA_DATE"]).dt.date
    df = df.drop(columns='DATA_DATE')
    df = df.drop(columns='CITY_NAME_data')
    df["SUBFLOOR"].replace(np.nan, 0, inplace=True)
    df["SUBFLOOR"] = df["SUBFLOOR"].astype('uint8')
    df["FLOOR"].replace(np.nan, 0, inplace=True)
    df["FLOOR"] = df["FLOOR"].astype('uint8')
    df["TOTALFLOOR"].replace(np.nan, 0, inplace=True)
    df["TOTALFLOOR"] = df["TOTALFLOOR"].astype('uint8')
    df["ROOM"].replace(np.nan, 0, inplace=True)
    df["ROOM"] = df["ROOM"].astype('uint8')
    df["HALL"].replace(np.nan, 0, inplace=True)
    df["HALL"] = df["HALL"].astype('uint8')
    df["TOILET"].replace(np.nan, 0, inplace=True)
    df["TOILET"] = df["TOILET"].astype('uint8')
    df["KITCHEN"].replace(np.nan, 0, inplace=True)
    df["KITCHEN"] = df["KITCHEN"].astype('uint8')
    df["BALCONY"].replace(np.nan, 0, inplace=True)
    df["BALCONY"] = df["BALCONY"].astype('uint8')
    df["PRICE"].replace(np.nan, 0, inplace=True)
    df.to_csv(HIVE_SECONDHOUSELOG_CSV_PATH, header=False, index=False, sep='|')


# def to_mysql(df):
# engine = create_engine('mysql+pymysql://root:idontcare@192.168.10.221/demo')
# df.to_sql('secondhouse_demo', con=engine, if_exists='append', index=False)


def begin():
    df_secondhouselog = get_secondhouselog_data()
    df_secondhouse = get_secondhouse_data()
    df_block = get_block_data()
    df_merge_data = merge_secondhouse(df_secondhouse, df_secondhouselog, df_block)
    df_preparation = preparation(df_merge_data)
    csv_action(df_preparation)
    algorithm_hive_transmission.begin(table="secondhouselog", table_csv="secondhouselog_csv",
                                      local_path=HIVE_SECONDHOUSELOG_CSV_PATH,
                                      hive_path=HIVE_SERVER_SECONDHOUSELOG_CSV_PATH)
    # redis_action(df_preparation)


if __name__ == '__main__':
    df_secondhouselog = get_secondhouselog_data()
    df_secondhouse = get_secondhouse_data()
    df_block = get_block_data()
    df_merge_data = merge_secondhouse(df_secondhouse, df_secondhouselog, df_block)
    df_preparation = preparation(df_merge_data)
    csv_action(df_preparation)
    algorithm_hive_transmission.begin(table="secondhouselog", table_csv="secondhouselog_csv",
                                      local_path=HIVE_SECONDHOUSELOG_CSV_PATH,
                                      hive_path=HIVE_SERVER_SECONDHOUSELOG_CSV_PATH)
