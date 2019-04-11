# from pyhive import hive
#
#
# class HiveUtil:
#     def __int__(self):
#         pass
#
#     def save(self, tablename, pandas_df):
#         pass
#
#
# if __name__ == '__main__':
#     hiveConn = hive.connect(host='192.168.10.164')
#     cursor = hiveConn.cursor()
#     sql = " LOAD DATA LOCAL INPATH '/Users/michael/PycharmProjects/deeplearning/analyseUserBehavior/analyseUserBehavior/test/script/user.txt' OVERWRITE INTO TABLE user "
#     # 执行sql语句
#     cursor.execute(sql)
#     # 得到执行语句的状态
#     status = cursor.poll().operationState
#     print("status:", status)
#     # 关闭hive连接
#     cursor.close()
#     hiveConn.close()
#
#     # cursor.execute('select * from user')
#     # print(cursor.fetchone())
#     # print(cursor.fetchall())


# coding:utf-8
from pyhive import hive
from TCLIService.ttypes import TOperationState

if __name__ == '__main__':
    try:
        # 打开hive连接
        hiveConn = hive.connect(host='192.168.83.135', )
        cursor = hiveConn.cursor()
        # 执行sql语句
        sql = ''' LOAD DATA LOCAL INPATH '/Users/michael/PycharmProjects/deeplearning/analyseUserBehavior/analyseUserBehavior/test/script/user.txt' OVERWRITE INTO TABLE demo.user '''
        cursor.execute(sql, async_=1)
        # 得到执行语句的状态
        status = cursor.poll().operationState
        print("status:", status)
        # 关闭hive连接
        cursor.close()
        hiveConn.close()
    except Exception as e:
        print(e)



