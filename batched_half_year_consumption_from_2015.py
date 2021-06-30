import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import os, sys,pickle
import time 
start_time = time.time() 
"""
This file calculates customers half yearly average consumption from 2015 - 2019
"""
if __name__ == '__main__':
    filename = sys.argv[1]
    mypath = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/batched_data/smoothed_grouped_meter_consumer_pairs'
    savepath = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/batched_data/half_year_smoothed_consumption_from_2015'
    df = pd.read_pickle(os.path.join(mypath,filename))

    # Select customers that have continous data from 2015 - 2019
    years = [2016, 2017, 2018, 2019]
    # Select customers that have continous data from 2015 - 2019
    ids0 = df[((df.year==2015))].meter_serial_number_consumer_id.unique().tolist()
    for year in years:  
        ids0 = df[(df.meter_serial_number_consumer_id.isin(ids0)) & 
                  (df.year==year)].meter_serial_number_consumer_id.unique().tolist()
    df = df[df.meter_serial_number_consumer_id.isin(ids0)]

    # Lists
    meter_serial_number_consumer_id = []
    mean_consumption_1_15 = []
    mean_consumption_2_15 = []
    mean_consumption_1_16 = []
    mean_consumption_2_16 = []
    mean_consumption_1_17 = []
    mean_consumption_2_17 = []
    mean_consumption_1_18 = []
    mean_consumption_2_18 = []
    mean_consumption_1_19 = []
    mean_consumption_2_19 = []

    # Use meta file to filter out "Non Residential" customers
    meta_datafile = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/metadata_folder/REG_metadata_March_3_2021.pck'
    meta_df = pickle.load(open(meta_datafile,'rb'))
    
    meta_df['meter_serial_number_consumer_id'] = meta_df['meter_serial_number'].astype(str)+'_'+meta_df['consumer_id']
    b = ['10. Residential', '2. T1 Tx FC1 AR STS']
    residential_meta = meta_df[meta_df.vending_category_name.apply(lambda y: any(x in b for x in y))]

    # Compute half year consumption for all customers
    half_year_1 = [1,2,3,4,5,6] 
    half_year_2 = [7,8,9,10,11,12] 
    first_part_2015 = [1,2,3,4,5,6,7,8] # tariff change occurred on 1st Sept
    second_part_2015 = [9,10,11,12]
    df_residental = df[df.meter_serial_number_consumer_id.isin(residential_meta.meter_serial_number_consumer_id)]
    grouped_df = df_residental.groupby('meter_serial_number_consumer_id')
    print ('Starting file:',filename)
    for name,group in grouped_df:
        half_1_15 = group[(group.year == 2015) & (group.month.isin(first_part_2015))]
        half_2_15 = group[(group.year == 2015) & (group.month.isin(second_part_2015))]
        half_1_16 = group[(group.year == 2016) & (group.month.isin(half_year_1))]
        half_2_16 = group[(group.year == 2016) & (group.month.isin(half_year_2))]
        half_1_17 = group[(group.year == 2017) & (group.month.isin(half_year_1))]
        half_2_17 = group[(group.year == 2017) & (group.month.isin(half_year_2))]
        half_1_18 = group[(group.year == 2018) & (group.month.isin(half_year_1))]
        half_2_18 = group[(group.year == 2018) & (group.month.isin(half_year_2))]
        half_1_19 = group[(group.year == 2019) & (group.month.isin(half_year_1))]
        half_2_19 = group[(group.year == 2019) & (group.month.isin(half_year_2))]


        mean_consumption_1_15.append(half_1_15.kWhs.mean())
        mean_consumption_2_15.append(half_2_15.kWhs.mean())
        mean_consumption_1_16.append(half_1_16.kWhs.mean())
        mean_consumption_2_16.append(half_2_16.kWhs.mean())
        mean_consumption_1_17.append(half_1_17.kWhs.mean())
        mean_consumption_2_17.append(half_2_17.kWhs.mean())
        mean_consumption_1_18.append(half_1_18.kWhs.mean())
        mean_consumption_2_18.append(half_2_18.kWhs.mean())
        mean_consumption_1_19.append(half_1_19.kWhs.mean())
        mean_consumption_2_19.append(half_2_19.kWhs.mean())
        meter_serial_number_consumer_id.append(name)

    summary_df = pd.DataFrame()
    summary_df['meter_serial_number_consumer_id'] = meter_serial_number_consumer_id
    summary_df['mean_consumption_1_15'] = mean_consumption_1_15
    summary_df['mean_consumption_2_15'] = mean_consumption_2_15
    summary_df['mean_consumption_1_16'] = mean_consumption_1_16
    summary_df['mean_consumption_2_16'] = mean_consumption_2_16
    summary_df['mean_consumption_1_17'] = mean_consumption_1_17
    summary_df['mean_consumption_2_17'] = mean_consumption_2_17
    summary_df['mean_consumption_1_18'] = mean_consumption_1_18
    summary_df['mean_consumption_2_18'] = mean_consumption_2_18
    summary_df['mean_consumption_1_19'] = mean_consumption_1_19
    summary_df['mean_consumption_2_19'] = mean_consumption_2_19

    summary_df.to_pickle(os.path.join(savepath,filename))
    print('Done with file: ',filename)
    print("--- %s seconds ---" % (time.time() - start_time))
