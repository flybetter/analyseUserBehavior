import ibis

webhdfs_host = "192.168.10.164"
impala_host = "192.168.10.164"

hdfs = ibis.hdfs_connect(host=webhdfs_host, port=9870)
client = ibis.impala.connect(host=impala_host, database='user_track', hdfs_client=hdfs)
# hdfs.put(hdfs_path="/user/hive/warehouse/user_track.db/newhouselog_csv/", resource="./one_day_newhouse_test.csv",
#          overwrite=True)

table2 = client.table("newhouselog_csv");

table = client.table("newhouselog")

table.insert(table2)

# t = table.functional_alltypes
# print(t)

#
#
#
# table = client.table("newhouselog", database="user_track")

# print(table.schema())
