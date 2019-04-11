import subprocess
import sys
import os

COMMANDS = [
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/newHouseLog.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/newHouse.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/phoneDevice.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/newHouseRoom.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/newHouseModel.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/block.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/secondHouse.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/secondHouseLog.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/crmUserAccountInfo.json"]

DEV_COMMANDS = [
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/develop_env/newHouseLog.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/develop_env/newHouse.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/develop_env/phoneDevice.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/develop_env/newHouseRoom.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/develop_env/newHouseModel.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/develop_env/block.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/develop_env/secondHouse.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/develop_env/secondHouseLog.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/util/datax_json/develop_env/crmUserAccountInfo.json"]


def action(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    for line in p.stdout:
        print(line.decode('utf-8'))
    p.wait()
    print(p.returncode)


def begin():
    env = os.getenv("active", "production")
    commands = COMMANDS
    if env == 'develop':
        commands = DEV_COMMANDS
    for temp in commands:
        action(temp)


if __name__ == '__main__':
    for command in COMMANDS:
        action(command)
