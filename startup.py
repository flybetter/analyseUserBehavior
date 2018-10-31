from analyseUserBehavior.algorithm import algorithm_newhouse, algorithm_phoneDevice
from analyseUserBehavior.command_tool import create_json, datax_command
from apscheduler.schedulers.blocking import BlockingScheduler
import pytz
import datetime


def begin():
    create_json.begin()
    print("create_json finished")
    datax_command.begin()
    print("datax finished")
    algorithm_newhouse.begin()
    print("algorithm_newhouse hdfs synchronous finished ")
    algorithm_phoneDevice.begin()
    print("algorithm_phoneDevice hdfs synchronous finished")


def begin2():
    print("test")


if __name__ == '__main__':
    timez = pytz.timezone('Asia/Shanghai')
    scheduler = BlockingScheduler(timezone=timez)
    scheduler.add_executor('processpool')
    scheduler.add_job(begin, 'cron', hour=4, minute=00, second=00)
    # scheduler.add_job(begin2, 'interval', seconds=2)
    scheduler.start()
    # begin()
