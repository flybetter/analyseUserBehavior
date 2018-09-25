import json
from datetime import datetime, date
import re

# FILES = ["newHouse.json", "newHouseLog.json", "phoneDevice.json"]
# newHouse.json is a full scale
FILES = ["/home/michael/python_project/analyseUserBehavior/analyseUserBehavior/command_tool/newHouseLog.json",
         "/home/michael/python_project/analyseUserBehavior/analyseUserBehavior/command_tool/phoneDevice.json"]


def format_json(date, file):
    with open(file) as f:
        data = f.read()
        data = re.sub(r"'\d{8}'", "'" + date + "'", data)
    with open(file, 'w') as f:
        f.write(data)


def begin():
    date = datetime.today().strftime("%Y%m%d")
    for file in FILES:
        format_json(date, file)


if __name__ == '__main__':
    date = datetime.today().strftime("%Y%m%d")
    for file in FILES:
        format_json(date, file)
