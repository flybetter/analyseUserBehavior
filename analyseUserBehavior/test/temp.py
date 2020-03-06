import redis

if __name__ == '__main__':
    pool = redis.ConnectionPool(host='172.17.1.37', db=8)
    offical_r = redis.Redis(connection_pool=pool)

    for key in offical_r.scan_iter(match='NHCRM^*', count=500):
        print(key)
        key_type = offical_r.type(key)

        if str(key_type, encoding="utf-8") == 'string':
            print("delete")
            offical_r.delete(key)
