import pandas as pd
import numpy as np
from sklearn.preprocessing import PowerTransformer
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

from utils.bq_data_extraction import get_sent_for_date, get_received_for_date


def transform(raw_data, date, end_date):

    features = [ 'sent_trx_number', 'received_trx_number', 'sent_total', 'sent_min', 'sent_avg',
                 'sent_max', 'received_total', 'received_min', 'received_avg', 'received_max', 'min_inputs',
                 'avg_inputs', 'max_inputs', 'min_outputs', 'avg_outputs', 'max_outputs',
                 'has_coinbase']
    df = raw_data[features]
    df.fillna(0, inplace=True)
    pt = PowerTransformer(method = 'yeo-johnson', standardize = True)
    trans_df = pd.DataFrame(pt.fit_transform(df), columns = features)
    print('data transformed')

    pca = PCA(n_components = 5)
    reduced_df = pd.DataFrame(pca.fit_transform(trans_df))
    print('data reduced')

    km = KMeans(n_clusters=6).fit(reduced_df)
    labels = km.labels_
    print('clustering done')

    raw_data[ 'label' ] = labels
    raw_data.groupby ('label')
    trans_df[ 'labels' ] = labels

    wallets = pd.DataFrame ()
    wallets[ 'address' ] = raw_data[ 'address' ]
    wallets[ 'label' ] = labels
    print('wallet data is ready...')

    sent = get_sent_for_date(date, end_date)
    received = get_received_for_date(date, end_date)

    labeled_sent = sent.merge (wallets, left_on = 'address', right_on = 'address')
    labeled_received = received.merge (wallets, left_on = 'address', right_on = 'address')

    pivot_sent = pd.pivot_table (labeled_sent, values = 'sum', index = [ 'date_hour' ], columns = [ 'label' ],
                                 aggfunc = np.sum)
    pivot_sent = pivot_sent*(-1)
    pivot_received = pd.pivot_table (labeled_received, values = 'sum', index = [ 'date_hour' ], columns = [ 'label' ],
                                     aggfunc = np.sum)

    nice_data = pivot_received.merge (pivot_sent, how = 'outer', left_on = 'date_hour', right_on = 'date_hour',
                                       suffixes = ('_rec', '_sent'))
    print('all data transformations done!')
    return nice_data