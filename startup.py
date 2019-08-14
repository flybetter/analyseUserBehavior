from analyseUserBehavior.algorithm import algorithm_newHouse, algorithm_phoneDevice, algorithm_secondHouse, \
    algorithm_crm_profile, algorithm_datax_creatjson, algorithm_datax_command, algorithm_sqoop, \
    algorithm_hive_transmission
from apscheduler.schedulers.blocking import BlockingScheduler
import pytz
from datetime import datetime
import subprocess


def begin():
    timez = pytz.timezone('Asia/Shanghai')
    start_time = datetime.now(timez)
    print("start time:" + start_time.strftime('%Y-%m-%d %H:%M:%S'))

    algorithm_datax_creatjson.begin()
    create_json_end_time = datetime.now(timez)
    print("create_json finished, cost time:" + str((create_json_end_time - start_time).seconds))

    algorithm_datax_command.begin()
    datax_command_time = datetime.now(timez)
    print("datax finished, cost time:" + str((datax_command_time - create_json_end_time).seconds))

    algorithm_sqoop.begin()
    sqoop_command_time = datetime.now(timez)
    print("sqoop command finished, cost time:" + str((sqoop_command_time - datax_command_time).seconds))

    algorithm_newHouse.begin()
    algorithm_newHouse_time = datetime.now(timez)
    print("algorithm_newhouse  synchronous finished ,cost time:" + str(
        (algorithm_newHouse_time - sqoop_command_time).seconds))

    algorithm_secondHouse.begin()
    algorithm_secondHouse_time = datetime.now(timez)
    print("algorithm_secondHouse synchronous finished, cost time:" + str(
        (algorithm_secondHouse_time - algorithm_newHouse_time).seconds))

    # algorithm_phoneDevice.begin()
    # algorithm_phoneDevice_time = datetime.now(timez)
    # print("algorithm_phoneDevice synchronous finished,cost time:" + str(
    #     (algorithm_phoneDevice_time - algorithm_newHouse_time).seconds))

    # algorithm_crm_profile.begin()
    # algorithm_crm_profile_time = datetime.now(timez)
    # print("algorithm_crm_profile synchronous finished,cost time:" + str(
    #     (algorithm_crm_profile_time - algorithm_phoneDevice_time).seconds))

    algorithm_hive_transmission.update_login()
    algorithm_hive_transmission_time = datetime.now(timez)
    # print("algorithm_crm_profile update_login finished,cost time:" + str(
    #     (algorithm_hive_transmission_time - algorithm_crm_profile_time).seconds))

    print("algorithm_crm_profile update_login finished,cost time:" + str(
        (algorithm_hive_transmission_time - algorithm_secondHouse_time).seconds))

    print("end time:" + algorithm_hive_transmission_time.strftime('%Y-%m-%d %H:%M:%S'))


def begin2():
    timez = pytz.timezone('Asia/Shanghai')
    start_time = datetime.now(timez)
    print("start time:" + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    algorithm_datax_creatjson.begin()
    create_json_end_time = datetime.now(timez)
    print("create_json finished, cost time:" + str((create_json_end_time - start_time).seconds))

    algorithm_datax_command.begin()
    datax_command_time = datetime.now(timez)
    print("datax finished, cost time:" + str((datax_command_time - create_json_end_time).seconds))

    # algorithm_newHouse.begin()
    algorithm_newHouse_time = datetime.now(timez)
    print("algorithm_newhouse  synchronous finished ,cost time:" + str(
        (algorithm_newHouse_time - datax_command_time).seconds))

    algorithm_secondHouse.begin()
    algorithm_secondHouse_time = datetime.now()
    print("algorithm_secondHouse synchronous finished, cost time:" + str(
        (algorithm_secondHouse_time - algorithm_newHouse_time).seconds))

    # algorithm_phoneDevice.begin()
    # algorithm_phoneDevice_time = datetime.now(timez)
    # print("algorithm_phoneDevice synchronous finished,cost time:" + str(
    #     (algorithm_phoneDevice_time - algorithm_newHouse_time).seconds))

    # algorithm_crm_profile.begin()
    # algorithm_crm_profile_time = datetime.now(timez)
    # print("algorithm_crm_profile synchronous finished,cost time:" + str(
    #     (algorithm_crm_profile_time - algorithm_phoneDevice_time).seconds))


def mysql_house():
    p = subprocess.Popen("python /app/datax/bin/datax.py /app/datax/bin/mysql_house.json", shell=True,
                         stdout=subprocess.PIPE)
    for line in p.stdout:
        print(line.decode('utf-8'))
    p.wait()
    print(p.returncode)


if __name__ == '__main__':
    timez = pytz.timezone('Asia/Shanghai')
    scheduler = BlockingScheduler(timezone=timez)
    scheduler.add_executor('processpool')
    scheduler.add_job(begin, 'cron', hour=4, minute=00, second=00, misfire_grace_time=30)
    # scheduler.add_job(begin2, 'interval', seconds=2)
    scheduler.start()
    # begin()
