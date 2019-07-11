from setuptools import setup, find_packages

setup(
    name="analyseUserBehavior",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        'APScheduler==3.5.3',
        'certifi==2018.8.24',
        'chardet==3.0.4',
        'cycler==0.10.0',
        'docopt==0.6.2',
        # 'hdfs==2.1.0',
        'idna==2.7',
        'kiwisolver==1.0.1',
        # 'matplotlib==2.2.3',
        'numpy==1.15.1',
        'pandas==0.23.4',
        # 'PyHDFS==0.2.1',
        'pyparsing==2.2.0',
        'python-dateutil==2.7.3',
        'pytz==2018.5',
        'redis==2.10.6',
        # 'requests==2.19.1',
        # 'scikit-learn==0.19.2',
        # 'simplejson==3.16.0',
        'six==1.11.0',
        # 'sklearn==0.0',
        'tzlocal==1.5.1',
        # 'pyspark==2.4.1',
        'ibis-framework==1.0.0',
        'paramiko==2.6.0',
    ]
)
