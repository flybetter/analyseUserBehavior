from analyseUserBehavior.algorithm import *
import paramiko


class SQOOPClient(object):
    def __init__(self, sever_url, username, password):
        self.sever_url = sever_url
        self.username = username
        self.password = password
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=sever_url, port=22, username=username, password=password)
        self.client = client

    def command(self, command):
        stdin, stdout, stderr = self.client.exec_command(command)
        print(stdout.read().decode('utf-8'))

    def close(self):
        self.client.close()


def begin():
    sshClient = SQOOPClient(sever_url=SQOOP_URL, username=SQOOP_USERNAME, password=SQOOP_PASSWORD)
    sshClient.command(SQOOP_COMMAND)
    sshClient.close()


if __name__ == '__main__':
    sshClient = SQOOPClient(sever_url="192.168.10.164", username="root", password="000000")
    sshClient.command( "sudo -u hdfs sqoop import --connect jdbc:oracle:thin:@202.102.83.165:1521:app --username app --password app --table DWB_ACCOUNT_DEVICE_PHONE -m 1 --hive-import --hive-overwrite  --hive-database user_track")
    sshClient.close()
