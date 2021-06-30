import os,sys,pickle
import numpy as np
import time
start_time = time.time()

if __name__ == '__main__':
    # get customers in this block based on customer_meter id pairs
    step_size = 19000
    consumption_datafile = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/smoothed_folder/merged_smoothed_monthly_by_meternumber_consumerid_march032021.pck'
    consumption_data = pickle.load(open(consumption_datafile,'rb'))
    # consumption_data = consumption_data[consumption_data.kWhs>=0]

    unique_ids = consumption_data.meter_serial_number_consumer_id.unique().tolist()
    length = (len(unique_ids)//step_size   + 1)* step_size
    for i in np.arange(0,length,step_size):
        start_idx = i
        stop_idx  = start_idx + step_size
        tmp_ids = unique_ids[start_idx:stop_idx]
        tmp_df =  consumption_data[consumption_data.meter_serial_number_consumer_id.isin(tmp_ids)]
        tmp_df.to_pickle(os.path.join('/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/batched_data/smoothed_grouped_meter_consumer_pairs/consumption_file_batch_{}_reg.pck'.format(i)))
    print("--- %s seconds ---" % (time.time() - start_time))
