import os
import pandas as pd

data_dir = '/home/tmp/data_demo/'

data_result = '/home/tmp/data_result/'

phone_data_dir = '/home/michael/datax_data/phoneDevice/'


def get_phonedevice_data():
    paths = os.listdir(phone_data_dir)
    for path in paths:
        columns = ["DEVICE", "PHONE"]
        df = pd.read_csv(phone_data_dir + path, names=columns, header=None,
                         index_col=False, low_memory=False)
        df.drop_duplicates(inplace=True)
        df = df[df['PHONE'].str.startswith('1', na=False)]
        return df


def get_data(phone_df):
    paths = os.listdir(data_dir)
    for path in paths:
        columns = ['DEVICE', 'DISTRICT']
        df = pd.read_csv(data_dir + path, names=columns, header=None,
                         index_col=False, low_memory=False)
        data_df = calculate(phone_df, df)
        data_df.to_csv(data_result + path)
        return df


def calculate(phone_df, data_df):
    df = data_df.merge(phone_df, on='DEVICE', how='left')
    return df


if __name__ == '__main__':
    phone_df = get_phonedevice_data()
    get_data(phone_df)
