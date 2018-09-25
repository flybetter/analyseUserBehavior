from analyseUserBehavior.algorithm import algorithm_newhouse, algorithm_phoneDevice
from analyseUserBehavior.command_tool import create_json, tools
from apscheduler.schedulers.blocking import BlockingScheduler


def begin():
    create_json.begin()
    print("create_json finished")
    tools.begin()
    print("datax finished")
    algorithm_newhouse.begin()
    print("algorithm_newhouse hdfs synchronous finished ")
    algorithm_phoneDevice.begin()
    print("algorithm_phoneDevice hdfs synchronous finished")


def begin2():
    print("test")


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_executor('processpool')
    scheduler.add_job(begin, 'cron', hour=22, minute=00, second=00)
    scheduler.start()
