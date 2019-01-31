from analyseUserBehavior.algorithm import algorithm_newHouse, algorithm_phoneDevice,algorithm_secondHouse
from analyseUserBehavior.command_tool import create_json, datax_command
from apscheduler.schedulers.blocking import BlockingScheduler
import pytz
import datetime


def begin():
    create_json.begin()
    print("create_json finished")
    datax_command.begin()
    print("datax finished")
    algorithm_newHouse.begin()
    print("algorithm_newhouse  synchronous finished ")
    algorithm_secondHouse.begin()
    print("algorithm_secondHouse synchronous finished")
    algorithm_phoneDevice.begin()
    print("algorithm_phoneDevice synchronous finished")



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
