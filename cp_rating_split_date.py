import os
import pandas as pd
import numpy as np
import operator
import matplotlib.pyplot as plt
import time
import sklearn
from datetime import datetime
from datetime import timedelta
from sklearn.cross_validation import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import SGDRegressor
from sklearn import preprocessing
from sklearn.cross_validation import cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn import datasets, linear_model
from sklearn import metrics
from sklearn.model_selection import cross_val_predict
from matplotlib import pyplot as pl

df = pd.read_csv('xsc_mta_pre_data_xie_update.csv',header=0,sep='\t')
column_names = df.columns
column_update = []
for column in column_names:
    column = column.replace('xsc_mta_pre_data_xie.','')
    column_update.append(column)
df.columns = column_update

#select date for train and test
df_test = df[df['date1'].isin(['2018-08-04','2018-08-05','2018-08-06'])]
df = df[~df['date1'].isin(['2018-08-04','2018-08-05','2018-08-06'])]
#sampling
sample_1=df[df['is_ordered']==1]
print(len(sample_1))
sample_2=df[df['is_ordered']==0]
print(len(sample_2))
others, sample_3 = train_test_split(sample_2, random_state=1,test_size=0.1)
sample_1=sample_1.append(sample_3)



from sklearn import ensemble, cross_validation
from sklearn.model_selection import cross_val_predict

feature=['stay_time_cut', 'geohash_6_nums', 'geohash_5_nums', 'geohash_6_hours'
         , 'geohash_6_max_hour', 'geohash_6_avg_hour','geohash_6_min_hour','geohash_5_hours'
         , 'geohash_5_max_hour', 'geohash_5_min_hour','geohash_5_avg_hour','ranks','rank_geo_6'
         ,'rank_geo_5'
,'driver_type'
,'driver_join_model'
,'driver_verify_status'
,'is_auth_driver'
,'order_finish_count_1d'
,'order_finish_count_1w'
,'order_finish_count_1m'
,'order_finish_distance_1d'
,'order_finish_distance_1w'
,'order_finish_distance_1m'
,'morning_peak_order_finish_distance_1d'
,'morning_peak_order_finish_distance_1w'
,'morning_peak_order_finish_distance_1m'
,'night_peak_order_finish_distance_1d'
,'night_peak_order_finish_distance_1w'
,'night_peak_order_finish_distance_1m'
,'num_store']
# train_and_valid, test = cross_validation.train_test_split(sample_1, test_size=0.2, random_state=10)
# train, valid = cross_validation.train_test_split(train_and_valid, test_size=0.01, random_state=10)
train = sample_1
test = df_test
train_feature, train_target = train[feature],train['is_ordered']
test_feature, test_target = test[feature],test['is_ordered']

import xgboost as xgb
dtrain=xgb.DMatrix(train_feature,label=train_target)
dtest=xgb.DMatrix(test[feature])
params={'booster':'gbtree',
    'objective': 'binary:logistic',
    'eval_metric': 'auc',
    'learning_rate':0.01,
    'max_depth':4,
    'lambda':10,
    'n_estimators':100000,
    'subsample':0.9,
    'colsample_bytree':0.9,
    'min_child_weight':0.1,
    'scale_pos_weight' :5,
    'seed':1,
    'nthread':4,
     'silent':1,
    'max_delta_step':10}

watchlist = [(dtrain,'train')]
bst=xgb.train(params,dtrain,num_boost_round=1000,evals=watchlist,early_stopping_rounds=300)
bst.save_model('xgb_model.model')

ypred=bst.predict(dtest)

#evaluate
y_pred = (ypred >= 0.4)*1

from sklearn import metrics
print ('AUC: %.4f' % metrics.roc_auc_score(test['is_ordered'],ypred))
print ('ACC: %.4f' % metrics.accuracy_score(test['is_ordered'],y_pred))
print ('Recall: %.4f' % metrics.recall_score(test['is_ordered'],y_pred))
print ('F1-score: %.4f' %metrics.f1_score(test['is_ordered'],y_pred))
print ('Precesion: %.4f' %metrics.precision_score(test['is_ordered'],y_pred))
print(metrics.confusion_matrix(test['is_ordered'],y_pred))

y_pred = (ypred >= 0.985)*1

from sklearn import metrics
print ('AUC: %.4f' % metrics.roc_auc_score(test['is_ordered'],ypred))
print ('ACC: %.4f' % metrics.accuracy_score(test['is_ordered'],y_pred))
print ('Recall: %.4f' % metrics.recall_score(test['is_ordered'],y_pred))
print ('F1-score: %.4f' %metrics.f1_score(test['is_ordered'],y_pred))
print ('Precesion: %.4f' %metrics.precision_score(test['is_ordered'],y_pred))
print(metrics.confusion_matrix(test['is_ordered'],y_pred))

#feature importance
fig, ax = plt.subplots(figsize=(12,18))
xgb.plot_importance(bst, max_num_features=50, height=0.8, ax=ax)
plt.show()







#evaluate ranking
df_test['pred']=bst.predict(xgb.DMatrix(df_test[feature]))


df_temp = df_test.copy()
# df_temp = df_temp.groupby('mta_haixiu_store_geohash_6').uid.nunique()
df_temp = df_temp.groupby('mta_haixiu_store_geohash_6')['is_ordered'].sum()
df_temp = df_temp.sort_values(ascending=False)

df_pred = df_test.copy()
df_pred['pred'] = (df_pred['pred'] >= 0.95)*1
df_pred = df_pred.groupby('mta_haixiu_store_geohash_6')['pred'].sum()
df_pred = df_pred.sort_values(ascending=False)

df_temp = df_temp.reset_index()
df_pred = df_pred.reset_index()
df_merge = df_temp.merge(df_pred,on='mta_haixiu_store_geohash_6')


ax = df_merge.plot(kind='line')
ax.set_xticklabels(df_merge['mta_haixiu_store_geohash_6'])
plt.show()