import pandas as pd
import numpy as np
import redis
import os
import json
from analyseUserBehavior.config.gobal_config import get_config
import re
import traceback
import math

REDIS_HOST = get_config('REDIS_HOST')
REDIS_DB = get_config("REDIS_DB")

REMAIN_DAYS = get_config('REMAIN_DAYS')

FILE_NEWHOUSE_PATH = get_config('FILE_NEWHOUSE_PATH')
FILE_NEWHOUSELOG_PATH = get_config('FILE_NEWHOUSELOG_PATH')
FILE_NEWHOUSEROOM_PATH = get_config('FILE_NEWHOUSEROOM_PATH')
FILE_NEWHOUSEMODEL_PATH = get_config('FILE_NEWHOUSEMODEL_PATH')
FILE_SECONDHOUSE_PATH = get_config('FILE_SECONDHOUSE_PATH')
FILE_SECONDHOUSELOG_PATH = get_config('FILE_SECONDHOUSELOG_PATH')
FILE_BLOCK_PATH = get_config('FILE_BLOCK_PATH')
FILE_PHONEDEVICE_PATH = get_config("FILE_PHONEDEVICE_PATH")

REDIS_PHONEDEVICE_PREFIX = get_config("REDIS_PHONEDEVICE_PREFIX")
REDIS_NEWHOUSE_PREFIX = get_config('REDIS_NEWHOUSE_PREFIX')
REDIS_SECONDHOUSE_PREFIX = get_config('REDIS_SECONDHOUSE_PREFIX')

REDIS_PHONE_DEVICES_DB = get_config('REDIS_PHONE_DEVICES_DB')

# CRM
REDIS_CRM_DB = get_config('REDIS_CRM_DB')
REDIS_CRM_HOST = get_config('REDIS_CRM_HOST')
REDIS_CRM_PREFIX = get_config('REDIS_CRM_PREFIX')
FILE_CRM_USER_PATH = get_config('FILE_CRM_USER_PATH')

CRM_REGULAR = r'PD\^(\d+)'
NHLOG_REGULAR = r'NHLOG\^(.+)'

# HIVE
HIVE_NEWHOUSELOG_CSV_PATH = get_config("HIVE_NEWHOUSELOG_CSV_PATH")
HIVE_URL = get_config("HIVE_URL")
HIVE_PORT = get_config("HIVE_PORT")
HIVE_SERVER_NEWHOUSELOG_CSV_PATH = get_config("HIVE_SERVER_NEWHOUSELOG_CSV_PATH")
HIVE_TABLE_NAME_CSV = get_config("HIVE_TABLE_NAME_CSV")
HIVE_TABLE_NAME = get_config("HIVE_TABLE_NAME")
