import ibis
from hdfs.ext.kerberos import KerberosClient
from krbcontext import krbcontext
from requests import Session
from requests_kerberos import HTTPKerberosAuth, DISABLED
import logging

logging.basicConfig(level=logging.DEBUG)


def hdfs_test():
    # hdfs = ibis.hdfs_connect(host='202.102.74.89', port='9870', auth_mechanism='GSSAPI')
    session = Session()
    session.verify = False
    client = KerberosClient(url="http://192.168.10.129:9870", root='/tmp', force_preemptive=True,
                            principal='demo', session=session)
    print(client.list('/'))

    # list = hdfs.ls('/user/hive/warehouse/test.db/account/')


if __name__ == '__main__':
    hdfs_test()
