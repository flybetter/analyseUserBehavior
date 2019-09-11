from impala.dbapi import connect


def demo2():
    # conn = connect(host='192.168.10.129')
    path = '/Users/michael/Downloads/app.keytab'
    # conn = connect(host='test.h365.h365', auth_mechanism='GSSAPI', use_ssl=False, kerberos_service_name='impala',
    #                ca_cert=path)
    conn = connect(host="cdho1.prod.house365", port=21050, auth_mechanism='GSSAPI', kerberos_service_name='impala',database='test')
    # conn = connect(host="192.168.10.129", port=21050, timeout=100000, use_ssl=False, ca_cert=None,
    #                ldap_user=None, ldap_password=None, kerberos_service_name='impala')
    cu = conn.cursor()
    cu.execute('SELECT * FROM account')
    print(cu.fetchall())


if __name__ == '__main__':
    demo2()

# pip install cython thriftpy==0.3.9