import ibis
from hdfs.ext.kerberos import KerberosClient
# from krbcontext import krbcontext
from requests import Session
from requests_kerberos import HTTPKerberosAuth, DISABLED
import logging
import os


def hdfs_test():
    session = Session()
    session.verify = False
    hdfs = ibis.hdfs_connect(host='cdho1.prod.house365', port='9870', auth_mechanism='GSSAPI', use_https=False,
                             session=session)

    list = hdfs.ls('/')
    print(list)

    # hdfs = ibis.hdfs_connect(host='cdho1.prod.house365', port='9870', auth_mechanism='GSSAPI', use_https=False,
    #                          session=session)
    # hdfs.put(hdfs_path='/user/hive/warehouse/user_track.db/newhouselog_csv/',
    #          resource='/home/michael/csv/newHouseLog/newHouseLog.csv', overwrite=True)


def update2():
    session = Session()
    session.verify = False
    hdfs = ibis.hdfs_connect(host='cdho1.prod.house365', port='9870', auth_mechanism='GSSAPI', use_https=False,
                             session=session)
    list = hdfs.ls('/')
    print(list)
    # hdfs.put(hdfs_path='/user/hive/warehouse/user_track.db/newhouselog_csv/',
    #          resource='/home/michael/csv/newHouseLog/newHouseLog.csv', overwrite=True)


def demo2():
    session = Session()
    session.verify = False
    client = KerberosClient(url="http://cdho1.prod.house365:9870", session=session)
    client.upload(hdfs_path='/user/hive/warehouse/user_track.db/newhouselog_csv/',
                  local_path='/home/michael/csv/newHouseLog/newHouseLog.csv', overwrite=True)


# def hdfs_demo():
#     hdfs_url = 'http://192.168.10.129:9870'
#     keytab_path = '/Users/michael/Downloads/krb5.keytab'
#     ccache_file = '/tmp/krb5cc_0'
#     print(keytab_path)
#     with krbcontext(using_keytab=True, keytab_file=keytab_path, principal="demo", password='12345678',
#                     ccache_file=ccache_file):
#         client = KerberosClient(hdfs_url)
#         print(client.list("/"))


if __name__ == '__main__':
    update2()
    # hdfs_test()
