import json
from datetime import datetime, date, timedelta
import re
import pytz


# FILES = ["newHouse.json", "newHouseLog.json", "phoneDevice.json"]
# newHouse.json is a full scale
FILES = ["/app/datax/bin/newHouseLog.json",
         "/app/datax/bin/phoneDevice.json"]


def format_json(date, file):
    with open(file) as f:
        data = f.read()
        data = re.sub(r"'\d{8}'", "'" + date + "'", data)
    with open(file, 'w') as f:
        f.write(data)


def begin():
    timez = pytz.timezone('Asia/Shanghai')
    date = (datetime().now(tz=timez) - timedelta(days=1)).strftime("%Y%m%d")
    for file in FILES:
        format_json(date, file)


if __name__ == '__main__':
    date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    for file in FILES:
        format_json(date, file)