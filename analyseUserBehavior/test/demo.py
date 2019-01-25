import pandas as pd
import redis
from analyseUserBehavior.algorithm.algorithm_newHouse import REDIS_HOST
import re


def count_value():
    r = redis.Redis(host=REDIS_HOST, port=6379, db=1)
    r.lpush("04556DE6-4A99-496A-B8D1-371364D1B17B", "22")


def test():
    with open("select_CONTEXT_ID______from_DWB_DA_APP_S.csv") as f:
        result = list()

        for line in f.readlines():
            print(line, '**' + str(len(line)))
            result.append(len(line))

        print(max(result))


if __name__ == '__main__':
    test()
