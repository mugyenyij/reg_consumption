import os,sys,pickle
import numpy as np
import time
start_time = time.time()

if __name__ == '__main__':
    # get customers based on id
    customer = sys.argv[1]
    savepath = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/batched_data'
    transaction_datafile = '/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/transactions_folder/REG_transaction_data.pck'
    transaction_data = pickle.load(open(transaction_datafile,'rb'))
    # remove regulatory & tva
    transaction_data = transaction_data[~transaction_data.tariff_name.isin(['TVA tax','Regulatory_Fee','Rura_fee'])]
    transaction_data = transaction_data[transaction_data.kWh_sold>=0]
    print('There are {} entries in the transaction file after dropping TVA tax, Regulatory_Fee, Rura_fee'.format(transaction_data.shape[0]))
    transaction_data['consumer_id'] = transaction_data['consumer_id'].astype('str')
    transaction_data['consumer_id'] = transaction_data['consumer_id'].str.strip()
    tmp_df =  transaction_data[transaction_data.consumer_id.isin([customer])]
    filename = customer+'_transaction'+'.pck'
    tmp_df.to_pickle(os.path.join(savepath,filename))

  
    print("--- %s seconds ---" % (time.time() - start_time))
