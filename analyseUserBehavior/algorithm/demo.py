import pandas as pd
import redis
from analyseUserBehavior.algorithm.algorithm import REDIS_HOST


def count_value():
    r = redis.Redis(host=REDIS_HOST, port=6379)
    r.lpush("04556DE6-4A99-496A-B8D1-371364D1B17B", "22")


if __name__ == '__main__':
    count_value()
