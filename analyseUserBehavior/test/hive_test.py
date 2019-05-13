import ibis
from impala.dbapi import connect

conn = connect(host='192.168.10.164', port=21050)
cursor = conn.cursor()
cursor.execute("insert into user_track.newhouselog_test partition (data_date='2019-4-27') select * from user_track.newhouselog_csv")
print(cursor.description)

# webhdfs_host = "192.168.10.164"
# impala_host = "192.168.10.164"

# hdfs = ibis.hdfs_connect(host=webhdfs_host, port=9870)
# client = ibis.impala.connect(host=impala_host, database='user_track', hdfs_client=hdfs)
# hdfs.put(hdfs_path="/user/hive/warehouse/user_track.db/newhouselog_csv/", resource="./one_day_newhouse_test.csv",
#          overwrite=True)

# client.upload(hdfs_path="/user/hive/warehouse/user_track.db/newhouselog_csv/", local_path='./one_day_newhouse_test.csv',
#               overwrite=True)


# table2 = client.table("newhouselog_csv");
# table = client.table("newhouselog")
# table.insert(table2)

# hdfs = ibis.hdfs_connect(host=webhdfs_host, port=9870)
# print(hdfs.ls("/"))
