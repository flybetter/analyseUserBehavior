import subprocess
import sys

COMMANDS = [
    "python /home/michael/datax/bin/datax.py /home/michael/python_project/analyseUserBehavior/analyseUserBehavior/command_tool/newHouse.json",
    "python /home/michael/datax/bin/datax.py /home/michael/python_project/analyseUserBehavior/analyseUserBehavior/command_tool/newHouseLog.json",
    "python /home/michael/datax/bin/datax.py /home/michael/python_project/analyseUserBehavior/analyseUserBehavior/command_tool/phoneDevice.json"]


def action(command):
    p = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE)
    while True:
        out = p.stderr.read(1)
        if out == '' and p.poll() != None:
            break
        if out != '':
            sys.stdout.write(out)
            sys.stdout.flush()


def begin():
    for command in COMMANDS:
        action(command)


if __name__ == '__main__':
    for command in COMMANDS:
        action(command)
