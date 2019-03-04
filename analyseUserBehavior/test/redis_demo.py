import redis

if __name__ == '__main__':
    pool = redis.ConnectionPool(host='192.168.10.221', db=4)
    r = redis.Redis(connection_pool=pool)
    name = r.get('test')
    print(name.decode('utf-8'))
