import subprocess
import sys
import os

COMMANDS = [
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/newHouseLog.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/newHouse.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/phoneDevice.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/newHouseRoom.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/newHouseModel.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/block.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/secondHouse.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/secondHouseLog.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/crmUserAccountInfo.json"]

DEV_COMMANDS = [
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/develop_env/newHouseLog.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/develop_env/newHouse.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/develop_env/phoneDevice.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/develop_env/newHouseRoom.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/develop_env/newHouseModel.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/develop_env/block.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/develop_env/secondHouse.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/develop_env/secondHouseLog.json",
    "python /app/datax/bin/datax.py /app/analyseUserBehavior/analyseUserBehavior/command_tool/develop_env/crmUserAccountInfo.json"]


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
