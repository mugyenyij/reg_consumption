import datetime
import pandas as pd
import numpy as np
import os

start_time = datetime.datetime.now()



def get_low_consumers(end_year, res_meta, df, payts_df):

    combined_low_df = pd.DataFrame()
    year = 2010
    while year < end_year:
        print(f"YEAR: {year}")
        if year == 2010:
            # filter installation years b4 and equal to 2010
            meta_year_filter = res_meta[res_meta.installation_year <= 2010]
        else:
            # filter based off installation year
            meta_year_filter = res_meta[res_meta.installation_year == year]

        df_by_year = df[df.meter_serial_number_consumer_id.isin(
            meta_year_filter.meter_serial_number_consumer_id)]  # create subset dataframe with single year of installation

        if year < 2012:
            # Select transactions couple of months prior to tariff change
            df_period_b4_tariff = df_by_year[(df_by_year.trans_period > before_tariff)
                      & (df_by_year.trans_period < tariff_date.strftime('%Y-%m-%d'))]
            # average consumption in period per customer
            y = df_period_b4_tariff.groupby(['meter_serial_number_consumer_id'])[
                'kWhs'].mean()
            y1 = y.reset_index()
            y1 = y1.rename(columns={'kWhs':'avg_kWh'})
            low_consumers = y[y < 15].index.tolist()
        else:
            # Select transactions couple of months prior to tariff change
            df_period_b4_tariff = df_by_year[(df_by_year.trans_period > before_tariff)
                      & (df_by_year.trans_period < tariff_date.strftime('%Y-%m-%d'))]
            # average consumption in period per customer
            y = df_period_b4_tariff.groupby(['meter_serial_number_consumer_id'])[
                'kWhs'].mean()
            y1 = y.reset_index()
            y1 = y1.rename(columns={'kWhs':'avg_kWh'})
            low_consumers = y[y < 15].index.tolist()

        # create subset of low consumption customers
        low_df_by_year = df[df.meter_serial_number_consumer_id.isin(low_consumers)]


        # create subset with payments and meta data added
        z = low_df_by_year[['meter_serial_number_consumer_id',
                            'kWhs', 'trans_period']].copy()
        z.loc[:, 'installation_year'] = year
        z1 = pd.merge(z, y1, on=['meter_serial_number_consumer_id'], how='left')
        result = pd.merge(z1, payts_df[['meter_serial_number_consumer_id', 'amounts', 'trans_period']],
                          on=['meter_serial_number_consumer_id', 'trans_period'], how='inner')

        # store all low customer transactions in single dataframe
        combined_low_df = pd.concat([combined_low_df, result])
        combined_low_df['consumption_class'] = "LOW"

        year += 1
        
    return combined_low_df

def get_medium_consumers(end_year, res_meta, df, payts_df):

    combined_medium_df = pd.DataFrame()
    year = 2010
    while year < end_year:
        print(f'Year: {year}')
        if year == 2010:
            # filter installation years b4 and equal to 2010
            meta_year_filter = res_meta[res_meta.installation_year <= 2010]
        else:
            # filter based off installation year
            meta_year_filter = res_meta[res_meta.installation_year == year]

        df_by_year = df[df.meter_serial_number_consumer_id.isin(
            meta_year_filter.meter_serial_number_consumer_id)]  # create subset dataframe with single year of installation

        if year < 2012:
            # Select transactions couple of months prior to tariff change
            df_period_b4_tariff = df_by_year[(df_by_year.trans_period > before_tariff)
                      & (df_by_year.trans_period < tariff_date.strftime('%Y-%m-%d'))]
            # average consumption in period per customer
            y = df_period_b4_tariff.groupby(['meter_serial_number_consumer_id'])[
                'kWhs'].mean()
            y1 = y.reset_index()
            y1 = y1.rename(columns={'kWhs':'avg_kWh'})
            medium_consumers = y[(y >= 15) & (y <=50)].index.tolist()
        else:
            # Select transactions couple of months prior to tariff change
            df_period_b4_tariff = df_by_year[(df_by_year.trans_period > before_tariff)
                      & (df_by_year.trans_period < tariff_date.strftime('%Y-%m-%d'))]
            # average consumption in period per customer
            y = df_period_b4_tariff.groupby(['meter_serial_number_consumer_id'])[
                'kWhs'].mean()
            y1 = y.reset_index()
            y1 = y1.rename(columns={'kWhs':'avg_kWh'})
            medium_consumers = y[(y >= 15) & (y <=50)].index.tolist()

        # create subset of medium consumption customers
        medium_df_by_year = df[df.meter_serial_number_consumer_id.isin(medium_consumers)]

        # create subset with payments
        z = medium_df_by_year[['meter_serial_number_consumer_id',
                            'kWhs', 'trans_period']].copy()
        z.loc[:, 'installation_year'] = year
        z1 = pd.merge(z, y1, on=['meter_serial_number_consumer_id'], how='left')
        result = pd.merge(z1, payts_df[['meter_serial_number_consumer_id', 'amounts', 'trans_period']],
                          on=['meter_serial_number_consumer_id', 'trans_period'], how='inner')

        # store all medium customer transactions in single dataframe
        combined_medium_df = pd.concat([combined_medium_df, result])
        combined_medium_df['consumption_class'] = "MEDIUM"  
        year += 1
    return combined_medium_df
def get_high_consumers(end_year, res_meta, df, payts_df):

    combined_high_df = pd.DataFrame()
    year = 2010
    while year < end_year:
        print(f'Year: {year}')
        if year==2010:
            meta_year_filter = res_meta[res_meta.installation_year <= 2010]
        else:
            meta_year_filter = res_meta[res_meta.installation_year == year]
        df_by_year = df[df.meter_serial_number_consumer_id.isin(meta_year_filter.meter_serial_number_consumer_id)]

        if year < 2012:
            # Select transactions couple of months prior to tariff change
            df_period_b4_tariff = df_by_year[(df_by_year.trans_period > before_tariff)
                      & (df_by_year.trans_period < tariff_date.strftime('%Y-%m-%d'))]
            # average consumption in period per customer
            y = df_period_b4_tariff.groupby(['meter_serial_number_consumer_id'])[
                'kWhs'].mean()
            y1 = y.reset_index()
            y1 = y1.rename(columns={'kWhs':'avg_kWh'})
            high_consumers = y[y>50].index.tolist()
        else:  
            # Select transactions couple of months prior to tariff change
            df_period_b4_tariff = df_by_year[(df_by_year.trans_period > before_tariff)
                      & (df_by_year.trans_period < tariff_date.strftime('%Y-%m-%d'))]
            # average consumption in period per customer
            y = df_period_b4_tariff.groupby(['meter_serial_number_consumer_id'])[
                'kWhs'].mean()
            y1 = y.reset_index()
            y1 = y1.rename(columns={'kWhs':'avg_kWh'})
            high_consumers = y[y>50].index.tolist()

        # create subset of low consumption customers
        high_df_by_year = df[df.meter_serial_number_consumer_id.isin(high_consumers)]

        # create subset with payments   
        z = high_df_by_year[['meter_serial_number_consumer_id','kWhs','trans_period']].copy()
        z.loc[:,'installation_year'] = year
        z1 = pd.merge(z, y1, on=['meter_serial_number_consumer_id'], how='left')
        result = pd.merge(z1, payts_df[['meter_serial_number_consumer_id','amounts','trans_period']], 
                      on=['meter_serial_number_consumer_id','trans_period'],how='inner')

        # store all low customer transactions in single dataframe    
        combined_high_df = pd.concat([combined_high_df, result])
        combined_high_df['consumption_class'] = "HIGH"
        year += 1
    return combined_high_df
def get_pcrt_transactions_in_category(df, cat):
  
    original_cat = df.groupby(['meter_serial_number_consumer_id']
                                          )['kWhs'].count().reset_index(
    ).rename(columns={"kWhs": "all_kWhs"})
    if cat == 'low':
        check_df = df[df.kWhs < 15]
        

    elif cat == 'medium':
        check_df = df[(df.kWhs >= 15) & (df.kWhs <= 50)]
        

    elif cat == 'high':
        check_df = df[df.kWhs > 50]
    
    check_cat = check_df.groupby(['meter_serial_number_consumer_id']
                                             )['kWhs'].count().reset_index(
    ).rename(columns={"kWhs": "cat_kWhs"})
    all_cat = pd.merge(original_cat,check_cat, how='inner')
    all_cat[f'percent_kwhs_in_{cat}'] = round(all_cat['cat_kWhs']/all_cat['all_kWhs']*100,0)
    perc_val = len(all_cat[all_cat[f'percent_kwhs_in_{cat}']>80])/len(all_cat)
    original_count = df.meter_serial_number_consumer_id.nunique()
    print(f'{round(perc_val*100,0)}% of customers have 80% of transactions in {cat}\n')
    print(f'Original customer count: {original_count}')
    
    return all_cat, original_count
    
def get_high_confidence_set(all_cat, original_count ,df, cat):
    cat_true_list = all_cat[all_cat[f'percent_kwhs_in_{cat}'
                                        ]>=80].meter_serial_number_consumer_id.unique(
    ).tolist()
    df= df[df.meter_serial_number_consumer_id.isin(cat_true_list)]
    cat_count = df.meter_serial_number_consumer_id.nunique()
    print(f'High confident customer count {cat} category: {cat_count}')
    print(f'{round(((original_count-cat_count)/original_count)*100,0)}% drop in customers')
    
    return df

def get_regression_df(tariff_date, meta_df, df, period=1):
    time_period = datetime.timedelta(days=(period*365))
    before_tariff = (tariff_date - time_period).strftime('%Y-%m-%d')
    after_tariff = (tariff_date + time_period).strftime('%Y-%m-%d')
    dist_meta = meta_df[['meter_serial_number_consumer_id',
                         'district']]
    
    # Select transactions six month prior and after tariff change
    result1 = df[(df.trans_period > before_tariff)
                      & (df.trans_period < after_tariff)]
    
    # Merge with meta data (installation dates and districts)
    tariff_df = pd.merge(result1, dist_meta,
                       on='meter_serial_number_consumer_id', how='left')
    # exclude customers installed in tarrif year
    tariff_df = tariff_df[~(tariff_df['installation_year'].isin([tariff_date.year, tariff_date.year+1]))]
    
    return tariff_df

def get_balanced_set(regression_df):
    balanced_df = regression_df.groupby(['meter_serial_number_consumer_id','trans_period'])['kWhs'].count().unstack()
    print(f'Length of balanced set: {len(balanced_df)}')
    balanced_df.dropna(inplace = True)
    print(f'Customers in balanced set: {len(balanced_df)}')
    balanced_ids = balanced_df.index.values.tolist()
    regression_df_balanced = regression_df[regression_df.meter_serial_number_consumer_id.isin(balanced_ids)]
    tmp = regression_df_balanced[regression_df_balanced.consumption_class=='LOW']
    print(f'LOW customer count: {tmp.meter_serial_number_consumer_id.nunique()}\n')
    
    
    tmp = regression_df_balanced[regression_df_balanced.consumption_class=='MEDIUM']
    print(f'MEDIUM customer count: {tmp.meter_serial_number_consumer_id.nunique()}\n')
    
    tmp = regression_df_balanced[regression_df_balanced.consumption_class=='HIGH']
    print(f'HIGH customer count: {tmp.meter_serial_number_consumer_id.nunique()}\n')
    
    return regression_df_balanced

def write2csv(df, filename, csv_dir = '/Users/joelmugyenyi/Desktop/REG_CSV/_smoothed_data/'):
        
    df.to_csv(csv_dir+filename,index=False)

    
    
if __name__ == "__main__":
    
    #. Initialization
    host = 'local' # 'local or server'
    set_flag = None
    which_tariff = 2    # 1 or 2
    date2day = datetime.datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    tariff_date_1 = datetime.date(2015, 9, 1)  # 1st tariff change
    tariff_date_2 = datetime.date(2017, 1, 1)  # 2nd tariff change
    if which_tariff == 1:
        tariff_date = tariff_date_1
    elif which_tariff == 2:
        tariff_date = tariff_date_2
    else:
        raise('Wrong tariff')

    # customer payments
    if host == 'local':
        payts_dir = '/Users/joelmugyenyi/Desktop/REG_CSV/_smoothed_data'
    elif host =='server':
        payts_dir = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data'
    else:
        raise('Select either local or server')
    payts_datafile = 'smoothed_payments_06142021.pck'
    payts_fp = os.path.join(payts_dir, payts_datafile)
    payts_df = pd.read_pickle(payts_fp)
    payts_df['trans_period'] = pd.to_datetime(payts_df[['year', 'month']].assign(day=lambda x: x['month'].apply(
        lambda y: 31 if y in [1, 3, 5, 7, 8, 10, 12] else (30 if y in [4, 6, 9, 11] else (28 if y == 2 else np.nan)))))

    # customer consumption
    if host == 'local':
        consumption_dir = '/Users/joelmugyenyi/Desktop/REG_CSV/_smoothed_data'
    elif host =='server':
        consumption_dir = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/smoothed_folder'
    else:
        raise('Select either local or server')
    consumption_datafile = 'merged_smoothed_monthly_by_meternumber_consumerid_march032021.pck'
    consumption_fp = os.path.join(consumption_dir, consumption_datafile)
    df = pd.read_pickle(consumption_fp)
    df['trans_period'] = pd.to_datetime(df[['year', 'month']].assign(day=lambda x: x['month'].apply(
        lambda y: 31 if y in [1, 3, 5, 7, 8, 10, 12] else (30 if y in [4, 6, 9, 11] else (28 if y == 2 else np.nan)))))

    # customer meta info
    if host == 'local':
        meta_dir = '/Users/joelmugyenyi/Desktop/REG_CSV/_smoothed_data'
    elif host =='server':
        meta_dir = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/metadata_folder'
    else:
        raise('Select either local or server')
    meta_datafile = 'REG_metadata_March_3_2021.pck'
    meta_fp = os.path.join(meta_dir, meta_datafile)
    meta_df = pd.read_pickle(meta_fp)
    meta_df['meter_serial_number_consumer_id'] = meta_df['meter_serial_number'].astype(
        str) + '_' + meta_df['consumer_id']
    meta_df['installation_year'] = meta_df.installation_date.dt.year
    b = ['10. Residential', '2. T1 Tx FC1 AR STS']
    res_meta = meta_df[meta_df.vending_category_name.apply(
        lambda y: any(x in b for x in y))]
    
    # csv directory
    if host == 'local':
        csv_dir='/Users/joelmugyenyi/Desktop/REG_CSV/_smoothed_data/'
    elif host == 'server':
        csv_dir = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/low_high_customer_ids/consumption_at_tariffs/'
    else:
        raise('Select either local or server')

    # Variables, for selecting consumption categories
  
    if tariff_date == tariff_date_1:
        end_year = tariff_date.year
    elif tariff_date == tariff_date_2:
        end_year = tariff_date.year-1
    period = 1
    time_period = datetime.timedelta(days=(period*365))
    before_tariff = (tariff_date - time_period).strftime('%Y-%m-%d')
    
    low_df = get_low_consumers(end_year, res_meta, df, payts_df)
    all_cat, original_count = get_pcrt_transactions_in_category(df = low_df, cat='low')
    hc_low_df = get_high_confidence_set(all_cat, original_count ,low_df, 'low')
    
    med_df = get_medium_consumers(end_year, res_meta, df, payts_df)
    all_cat, original_count = get_pcrt_transactions_in_category(df = med_df, cat='medium')
    hc_med_df = get_high_confidence_set(all_cat, original_count ,med_df, 'medium')
    
    high_df = get_high_consumers(end_year, res_meta, df, payts_df)
    all_cat, original_count = get_pcrt_transactions_in_category(df = high_df, cat='high')
    hc_high_df = get_high_confidence_set(all_cat, original_count ,high_df, 'high')
    
    if set_flag == 'unfiltered':
        unfiltered_df_list = [low_df, med_df, high_df]  # List of your dataframes
        unfiltered_df = pd.concat(unfiltered_df_list)
        regression_df = get_regression_df(tariff_date, meta_df=meta_df, df=unfiltered_df, period=period) 
        write2csv(regression_df, filename=f'tariff{which_tariff}_consumers_unfiltered_{date2day}.csv', csv_dir=csv_dir)
    elif  set_flag == 'unbalanced':
        hc_df_list = [hc_low_df, hc_med_df, hc_high_df]  # List of your dataframes
        hc_df = pd.concat(hc_df_list)
        regression_df = get_regression_df(tariff_date, meta_df=meta_df, df=hc_df, period=period) 
        write2csv(regression_df, filename=f'tariff{which_tariff}_consumers_unbalanced_{date2day}.csv', csv_dir=csv_dir)
    elif set_flag == 'balanced':
        hc_df_list = [hc_low_df, hc_med_df, hc_high_df]  # List of your dataframes
        hc_df = pd.concat(hc_df_list)
        regression_df = get_regression_df(tariff_date, meta_df=meta_df, df=hc_df, period=period)    
        regression_df_balanced = get_balanced_set(regression_df)
        write2csv(regression_df_balanced, filename=f'tariff{which_tariff}_consumers_balanced_{date2day}.csv', csv_dir=csv_dir)
    else:
        unfiltered_df_list = [low_df, med_df, high_df]  # List of your dataframes
        unfiltered_df = pd.concat(unfiltered_df_list)
        regression_df = get_regression_df(tariff_date, meta_df=meta_df, df=unfiltered_df, period=period) 
        write2csv(regression_df, filename=f'tariff{which_tariff}_consumers_unfiltered_{date2day}.csv', csv_dir=csv_dir)
        
        hc_df_list = [hc_low_df, hc_med_df, hc_high_df]  # List of your dataframes
        hc_df = pd.concat(hc_df_list)
        regression_df = get_regression_df(tariff_date, meta_df=meta_df, df=hc_df, period=period) 
        write2csv(regression_df, filename=f'tariff{which_tariff}_consumers_unbalanced_{date2day}.csv', csv_dir=csv_dir)
        
        regression_df_balanced = get_balanced_set(regression_df)
        write2csv(regression_df_balanced, filename=f'tariff{which_tariff}_consumers_balanced_{date2day}.csv', csv_dir=csv_dir)

    

    

    
    
    
end_time = datetime.datetime.now()
print('Duration: {}\n'.format(end_time - start_time))    