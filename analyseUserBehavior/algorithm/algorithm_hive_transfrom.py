import ibis
from analyseUserBehavior.algorithm import *
from functools import wraps


def decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            traceback.print_exc()
            raise ValueError(func.__name__ + ' has something wrong')

    return wrapper


class HiveAction(object):
    def __init__(self, table_csv=HIVE_TABLE_NAME_CSV, table=HIVE_TABLE_NAME, local_path=HIVE_NEWHOUSELOG_CSV_PATH,
                 hive_path=HIVE_SERVER_NEWHOUSELOG_CSV_PATH):
        self.table_csv = table_csv
        self.table = table
        self.local_path = local_path
        self.hive_path = hive_path
        self.hdfs = ibis.hdfs_connect(host=HIVE_URL, port=HIVE_PORT)
        self.client = ibis.impala.connect(host=HIVE_URL, database='user_track', hdfs_client=self.hdfs)

    @decorator
    def upload(self):
        self.hdfs.put(hdfs_path=self.hive_path, resource=self.local_path, overwrite=True)

    @decorator
    def sync(self):
        hive_table_csv = self.client.table(self.table_csv)
        hive_table = self.client.table(self.table)
        hive_table.insert(hive_table_csv)


if __name__ == '__main__':
    hiveaction = HiveAction()
    hiveaction.upload()
    hiveaction.sync()
