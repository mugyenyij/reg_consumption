import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import os, sys, pickle, time
start_time = time.time() 
if __name__ == '__main__':
    filename = sys.argv[1]
    mypath = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/batched_data/smoothed_batched_grped_consumer_ids'
    savepath = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/batched_data/quarterly_consumption'
    df = pd.read_pickle(os.path.join(mypath,filename))
    df['consumer_id'] = df['consumer_id'].astype('str')
    df['consumer_id'] = df['consumer_id'].str.strip()

    unique_consumers = df.consumer_id.unique().tolist()
    consumer_ids = []
    past_year_mean_consumption = []
    past_year_median_consumption = []
    first_quarter_after_mean_consumption = []
    first_quarter_after_median_consumption = []
    second_quarter_after_mean_consumption = []
    second_quarter_after_median_consumption = []
    third_quarter_after_mean_consumption = []
    third_quarter_after_median_consumption = []
    fourth_quarter_after_mean_consumption = []
    fourth_quarter_after_median_consumption = []

    meta_datafile = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/metadata_folder/REG_metadata.pck'
    meta_df = pickle.load(open(meta_datafile,'rb'))
    meta_df['consumer_id'] = meta_df['consumer_id'].astype('str')
    meta_df['consumer_id'] = meta_df['consumer_id'].str.strip()
    residential_meta = meta_df[meta_df.vending_category_name.apply(lambda x: '10. Residential' in x)]

    df_residental = df[df.consumer_id.isin(residential_meta.consumer_id)]
    print('Starting file:',filename)
    grouped_df = df_residental.groupby('consumer_id')
    for name,group in grouped_df:
        past_year_df = group[group.year == 2016]
        first_quarter_after_df = group[(group.year == 2017) & (group.month.isin([1,2,3]))]
        second_quarter_after_df = group[(group.year == 2017) & (group.month.isin([4,5,6]))]
        third_quarter_after_df = group[(group.year == 2017) & (group.month.isin([7,8,9]))]
        fourth_quarter_after_df = group[(group.year == 2017) & (group.month.isin([10,11,12]))]

        past_year_mean_consumption.append(past_year_df.kWhs.mean())
        past_year_median_consumption.append(past_year_df.kWhs.median())
        first_quarter_after_mean_consumption.append(first_quarter_after_df.kWhs.mean())
        first_quarter_after_median_consumption.append(first_quarter_after_df.kWhs.median())
        second_quarter_after_mean_consumption.append(second_quarter_after_df.kWhs.mean())
        second_quarter_after_median_consumption.append(second_quarter_after_df.kWhs.median())
        third_quarter_after_mean_consumption.append(third_quarter_after_df.kWhs.mean())
        third_quarter_after_median_consumption.append(third_quarter_after_df.kWhs.median())
        fourth_quarter_after_mean_consumption.append(fourth_quarter_after_df.kWhs.mean())
        fourth_quarter_after_median_consumption.append(fourth_quarter_after_df.kWhs.median())

        consumer_ids.append(name)
        summary_df = pd.DataFrame()
        summary_df['consumer_id'] = consumer_ids
        summary_df['past_year_mean_consumption'] = past_year_mean_consumption
        summary_df['past_year_median_consumption'] = past_year_median_consumption
        summary_df['first_quarter_mean_consumption'] = first_quarter_after_mean_consumption
        summary_df['first_quarter_median_consumption'] = first_quarter_after_median_consumption
        summary_df['second_quarter_mean_consumption'] = second_quarter_after_mean_consumption
        summary_df['second_quarter_median_consumption'] = second_quarter_after_median_consumption
        summary_df['third_quarter_mean_consumption'] = third_quarter_after_mean_consumption
        summary_df['third_quarter_median_consumption'] = third_quarter_after_median_consumption
        summary_df['fourth_quarter_mean_consumption'] = fourth_quarter_after_mean_consumption
        summary_df['fourth_quarter_median_consumption'] = fourth_quarter_after_median_consumption
        summary_df['diff_past_with_1Q'] = summary_df.iloc[:,3] - summary_df.iloc[:,1]
        summary_df['diff_past_with_2Q'] = summary_df.iloc[:,5] - summary_df.iloc[:,1]
        summary_df['diff_past_with_3Q'] = summary_df.iloc[:,7] - summary_df.iloc[:,1]
        summary_df['diff_past_with_4Q'] = summary_df.iloc[:,9] - summary_df.iloc[:,1]
        summary_df.to_pickle(os.path.join(savepath,filename))
    print('Done with file: ',filename)
    print("--- %s seconds ---" % (time.time() - start_time))
