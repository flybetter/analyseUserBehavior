import redis
import pandas as pd
import json
import numpy as np

if __name__ == '__main__':
    pool = redis.ConnectionPool(host='202.102.83.162', db=1)
    r = redis.Redis(connection_pool=pool)
    #13813021753
    temp_result = r.lrange('NHLOG^0C4A2D64-38CF-4531-8EA2-B3BAE6397845', 0, 30)
    data = list()
    for json_data in temp_result:
        data.extend(json.loads(json_data.decode('utf-8')))
    result_json = json.dumps(data, ensure_ascii=False)
    df = pd.read_json(result_json, orient='records', dtype={'PRJ_ITEMNAME': np.str})
    print(df.to_csv('demo.csv'))
