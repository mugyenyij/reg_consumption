import os,sys,pickle
import numpy as np
import time
start_time = time.time()

if __name__ == '__main__':
    # get customers based on id
    customer = sys.argv[1]
    savepath = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/batched_data'
    transaction_datafile = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/smoothed_folder/REG_smoothed_Feb_27_2021.pck'
    transaction_data = pickle.load(open(transaction_datafile,'rb'))
    # remove regulatory & tva
    transaction_data['consumer_id'] = transaction_data['consumer_id'].astype('str')
    transaction_data['consumer_id'] = transaction_data['consumer_id'].str.strip()
    tmp_df =  transaction_data[transaction_data.consumer_id.isin([customer])]
    filename = customer+'_smoothed'+'.pck'
    tmp_df.to_pickle(os.path.join(savepath,filename))


    print("--- %s seconds ---" % (time.time() - start_time))
