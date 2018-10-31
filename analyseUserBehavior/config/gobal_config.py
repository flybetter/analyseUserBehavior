import configparser
import os
import pandas as pd

"""
读取配置文件信息
"""
## 用来控制环境参数
ENV = 'develop'


def get_config(name):
    con = configparser.ConfigParser()
    con.read(os.path.dirname(os.path.abspath(__file__)) + os.sep + 'settings.ini', encoding='utf-8')
    value = con.get(ENV, name)
    return value


if __name__ == '__main__':
    value = get_config('FILE_PHONEDEVICE_PATH')
    print(value)
    # con = ConfigParser()
    # res = con.get_config('FILE_PHONEDEVICE_PATH')
    # print(os.path.abspath(__file__))
    # print(res)
    # paths = os.listdir(res)
    # for path in paths:
    #     df = pd.read_csv(res + path)
    #     print(df.head(100))
