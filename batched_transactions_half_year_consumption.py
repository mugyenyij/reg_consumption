import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import os, sys,pickle
import time
start_time = time.time()
if __name__ == '__main__':
    filename = sys.argv[1]
    mypath = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/batched_data/transactions_batched_grped_consumer_ids'
    savepath = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/batched_data/half_year_transaction_consumption'
    df = pd.read_pickle(os.path.join(mypath,filename))
    df['consumer_id'] = df['consumer_id'].astype('str')
    df['consumer_id'] = df['consumer_id'].str.strip()
    df['transaction_date']= pd.to_datetime(df['transaction_date'])
    df['year'] = df.sort_values(['transaction_date']).transaction_date.dt.year
    df['month'] = df.sort_values(['transaction_date']).transaction_date.dt.month

    years = [2013, 2014, 2015, 2016, 2017, 2018, 2019]
    ids0 = df[((df.transaction_date.dt.year==2012))].consumer_id.unique().tolist()
    for year in years:
        ids0 = df[(df.consumer_id.isin(ids0)) & (df.transaction_date.dt.year==year)].consumer_id.unique().tolist()
    df = df[df.consumer_id.isin(ids0)]

    # Lists
    consumer_ids = []
    mean_consumption_2_12 = []
    mean_consumption_1_13 = []
    mean_consumption_2_13 = []
    mean_consumption_1_14 = []
    mean_consumption_2_14 = []
    mean_consumption_1_15 = []
    mean_consumption_2_15 = []
    mean_consumption_1_16 = []
    mean_consumption_2_16 = []
    mean_consumption_1_17 = []
    mean_consumption_2_17 = []
    mean_consumption_1_18 = []
    mean_consumption_2_18 = []


    meta_datafile = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/metadata_folder/REG_metadata.pck'
    meta_df = pickle.load(open(meta_datafile,'rb'))
    meta_df['consumer_id'] = meta_df['consumer_id'].astype('str')
    meta_df['consumer_id'] = meta_df['consumer_id'].str.strip()
    b = ['10. Residential', '2. T1 Tx FC1 AR STS'] # residential customers
    residential_meta = meta_df[meta_df.vending_category_name.apply(lambda y: any(x in b for x in y))]

    half_year_1 = [1,2,3,4,5,6]
    half_year_2 = [7,8,9,10,11,12]
    nine_months = [1,2,3,4,5,6,7,8,9]
    three_months = [10,11,12]
    df_residental = df[df.consumer_id.isin(residential_meta.consumer_id)]
    print ('Starting file:',filename)
    grouped_df = df_residental.groupby('consumer_id')
    for name,group in grouped_df:
        group = group.sort_values(['transaction_date']).groupby(['year','month']).sum('kWh_sold').reset_index()
        half_2_12 = group[(group.year == 2012) & (group.month.isin(half_year_2))]
        half_1_13 = group[(group.year == 2013) & (group.month.isin(half_year_1))]
        half_2_13 = group[(group.year == 2013) & (group.month.isin(half_year_2))]
        half_1_14 = group[(group.year == 2014) & (group.month.isin(half_year_1))]
        half_2_14 = group[(group.year == 2014) & (group.month.isin(half_year_2))]
        half_1_15 = group[(group.year == 2015) & (group.month.isin(nine_months))]
        half_2_15 = group[(group.year == 2015) & (group.month.isin(three_months))]
        half_1_16 = group[(group.year == 2016) & (group.month.isin(half_year_1))]
        half_2_16 = group[(group.year == 2016) & (group.month.isin(half_year_2))]
        half_1_17 = group[(group.year == 2017) & (group.month.isin(half_year_1))]
        half_2_17 = group[(group.year == 2017) & (group.month.isin(half_year_2))]
        half_1_18 = group[(group.year == 2018) & (group.month.isin(half_year_1))]
        half_2_18 = group[(group.year == 2018) & (group.month.isin(half_year_2))]

        mean_consumption_2_12.append(half_2_12.kWh_sold.mean())
        mean_consumption_1_13.append(half_1_13.kWh_sold.mean())
        mean_consumption_2_13.append(half_2_13.kWh_sold.mean())
        mean_consumption_1_14.append(half_1_14.kWh_sold.mean())
        mean_consumption_2_14.append(half_2_14.kWh_sold.mean())
        mean_consumption_1_15.append(half_1_15.kWh_sold.mean())
        mean_consumption_2_15.append(half_2_15.kWh_sold.mean())
        mean_consumption_1_16.append(half_1_16.kWh_sold.mean())
        mean_consumption_2_16.append(half_2_16.kWh_sold.mean())
        mean_consumption_1_17.append(half_1_17.kWh_sold.mean())
        mean_consumption_2_17.append(half_2_17.kWh_sold.mean())
        mean_consumption_1_18.append(half_1_18.kWh_sold.mean())
        mean_consumption_2_18.append(half_2_18.kWh_sold.mean())
        consumer_ids.append(name)
        if name == '110640':
            print (f'This is the file Im looking for {filename}')

    summary_df = pd.DataFrame()
    summary_df['consumer_id'] = consumer_ids
    summary_df['mean_consumption_2_12'] = mean_consumption_2_12
    summary_df['mean_consumption_1_13'] = mean_consumption_1_13
    summary_df['mean_consumption_2_13'] = mean_consumption_2_13
    summary_df['mean_consumption_1_14'] = mean_consumption_1_14
    summary_df['mean_consumption_2_14'] = mean_consumption_2_14
    summary_df['mean_consumption_1_15'] = mean_consumption_1_15
    summary_df['mean_consumption_2_15'] = mean_consumption_2_15
    summary_df['mean_consumption_1_16'] = mean_consumption_1_16
    summary_df['mean_consumption_2_16'] = mean_consumption_2_16
    summary_df['mean_consumption_1_17'] = mean_consumption_1_17
    summary_df['mean_consumption_2_17'] = mean_consumption_2_17
    summary_df['mean_consumption_1_18'] = mean_consumption_1_18
    summary_df['mean_consumption_2_18'] = mean_consumption_2_18

    summary_df.to_pickle(os.path.join(savepath,filename))
    print('Done with file: ',filename)
    print("--- %s seconds ---" % (time.time() - start_time))
