#Library imports
#################################################
import RWredshift as RW
import pandas as pd
import os.path
import os
import random
import MySQLdb as ms
import datetime as dt
import pickle as pk
import nltk
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer as ss
import re
import sys
import nltk
import math
import numpy as np
from sklearn.cross_validation import train_test_split as tts
from sklearn import preprocessing
from sklearn.naive_bayes import MultinomialNB as MNB
from scipy.stats import mode
from pandas.io import sql
#################################################


#Functions:
def raw_data_pull(START_DATE, END_DATE, GOBACK_TIME):
	'''Gets raw data from atlas and creates the merged dataset to split'''
	#---------VARIABLES
	#@database connectors
	redshift = RW.redshift_connect()
	print ('Redshift connection:\t\t\t[OK]')						
	db_conn = ms.connect("localhost","root","root","churn_model")
	print ('MySQL connection:\t\t\t[OK]')
	db_curs = db_conn.cursor()
	#@path variables
	parent = os.path.split(os.getcwd())[0]
	
	#@queries
	SL_QUERY = open(parent + '/Queries/Sl_account_info','r').read()
	TK_QUERY = open(parent +'/Queries/ticket_query.txt','r').read()
	TT_QUERY = open(parent + '/Queries/ticket_type_query.txt','r').read()
	MS_SL_CREATE_QUERY = open(parent + '/Queries/mssql_create_sldata.txt','r').read()
	MS_TK_CREATE_QUERY = open(parent + '/Queries/mssql_create_ticketdata.txt','r').read()
	MS_TT_CREATE_QUERY = open(parent + '/Queries/mssql_create_typedata.txt','r').read()

	
	#---------PROCESS STEP:- MYSQL TABLE CREATION
	try:
		db_curs.execute(MS_SL_CREATE_QUERY)
		db_curs.execute(MS_TK_CREATE_QUERY)
		db_curs.execute(MS_TT_CREATE_QUERY)
		print ('Table created successfully')
	except ms.OperationalError:
		print ('Table already exists in the local db. Creating new table')
		MS_SL_DROP_QUERY = open(parent + '/Queries/mssql_drop_sldata.txt','r').read()
		MS_TK_DROP_QUERY = open(parent + '/Queries/mssql_drop_ticketdata.txt','r').read()
		MS_TT_DROP_QUERY = open(parent + '/Queries/mssql_drop_typedata.txt','r').read()				
		db_curs.execute(MS_SL_DROP_QUERY)
		db_curs.execute(MS_TK_DROP_QUERY)
		db_curs.execute(MS_TT_DROP_QUERY)		
		db_curs.execute(MS_SL_CREATE_QUERY)
		db_curs.execute(MS_TK_CREATE_QUERY)
		db_curs.execute(MS_TT_CREATE_QUERY)
		print ('Table created successfully')

		
	#---------PROCESS STEP:- QUERY DATA + STORAGE IN MSSQL
	SL_QUERY = SL_QUERY.replace('%MOD_START_DATE%',START_DATE)
	SL_QUERY = SL_QUERY.replace('%MOD_END_DATE%',END_DATE)
	SL_QUERY = SL_QUERY.replace('%GOBACK_TIME%',GOBACK_TIME)		
	TK_QUERY = TK_QUERY.replace('%MOD_START_DATE%',START_DATE)
	TK_QUERY = TK_QUERY.replace('%MOD_END_DATE%',END_DATE)
	TK_QUERY = TK_QUERY.replace('%GOBACK_TIME%',GOBACK_TIME)		
	TT_QUERY = TT_QUERY.replace('%MOD_START_DATE%',START_DATE)
	TT_QUERY = TT_QUERY.replace('%MOD_END_DATE%',END_DATE)
	TT_QUERY = TT_QUERY.replace('%GOBACK_TIME%',GOBACK_TIME)

	
	sl_data = redshift.query(SL_QUERY)
	tk_data = redshift.query(TK_QUERY)	
	tt_data = redshift.query(TT_QUERY)

	device_switch = sl_data['device_switch']
	temp_dev_switch = []
	for i in device_switch:
		if np.isnan(i):
			temp_dev_switch.append(0)
		else:
			temp_dev_switch.append(i)
	sl_data['device_switch'] = temp_dev_switch
	tt_data = tt_data.dropna(subset=['churn_flag','last_bill_date'], how='all')
	tt_data = tt_data.dropna(subset=['resolution_time'], how='all')	
	
	tt_data['group_name'].fillna('Other', inplace=True)
	print tt_data['group_name'].unique()
	tt_data['request_type'].fillna('Other', inplace=True)
	tt_data['ticket_class'].fillna('Other', inplace=True)
	tt_data['ticket_type'].fillna('Other', inplace=True)
		
	sl_data.to_sql(con=db_conn, name='customer_sldata', if_exists='append', flavor='mysql')
	tk_data.to_sql(con=db_conn, name='customer_tkdata', if_exists='append', flavor='mysql')
	tt_data.to_sql(con=db_conn, name='customer_ttdata', if_exists='append', flavor='mysql')	
	
	print ('MySQL DB write:\t\t[OK]')
	#---------PROCESS STEP:- MERGE (SL + TK + TT) + CREATE DUMMY VARIABLES -> MODEL DATA
	#model_data = pd.merge(sl_data, tk_data, on=['sl_uuid'], how='left')
	#print model_data.columns
	return_dict = { 'sl_data':sl_data, 'tk_data':tk_data, 'tt_data':tt_data }
	return(return_dict)	
	

def data_load():
	'''
	Loads data from local DB to the working environment to be merged,
	and split for modeling
	'''
	#@local Variables
	try:
		db_conn = ms.connect("localhost","root","root","churn_model")
		print ('MySQL connection:\t\t\t[OK]')
	except:
		print ('MySQL connection:\t\t\t[Fail]')
	
	try:
		db_curs = db_conn.cursor()
		print ('Cursor:\t\t\t[OK]')
	except:
		print ('Cursor:\t\t\t[Fail]')
	
	
	#@fetch data
	sl_data = db_curs.execute('SELECT * FROM customer_sldata;')
	sl_data = db_curs.fetchall()
	sl_data = [x for x in sl_data]
	sl_data = pd.DataFrame(sl_data, columns = ['sl_uuid', 
												'last_invoice_date', 
												'churn_flag', 
												'customer_life', 
												'sl_activated_date',
												'sl_deactivated_date',
												'multiline_flag',
												'device_switch',
												'device_type'], index = range(len(sl_data)))
	
												
	tk_data = db_curs.execute('SELECT * FROM customer_tkdata;')
	tk_data = db_curs.fetchall()
	tk_data = [x for x in tk_data]
	tk_data = pd.DataFrame(tk_data, columns = ['sl_uuid', 'churn_flag', 'num_tickets'], index = range(len(tk_data)))
	
	tt_data = db_curs.execute('SELECT * FROM customer_ttdata;')
	tt_data = db_curs.fetchall()
	tt_data = [x for x in tt_data]
	tt_data = pd.DataFrame(tt_data, columns = ['sl_uuid',
												'ticket_id',
												'group_name',
												'request_type',
												'solved_at',
												'ticket_class',
												'ticket_type',
												'resolution_time',
												'tags',
												'last_bill_date',
												'created_at',
												'cutoff_date',
												'diff_date',
												'churn_flag'], index = range(len(tt_data)))
	return_dict = { 'sl_data':sl_data, 'tk_data':tk_data, 'tt_data':tt_data }
	return(return_dict)	


def data_transform(sl_data, tk_data, tt_data):
	'''
	Merge serviceline data with ticket data
	Take distinct values in tags, group names, request types
	Create dummy variables for each 
	'''
	#@local Variables
	try:
		db_conn = ms.connect("localhost","root","root","churn_model")
		print ('MySQL connection:\t\t\t[OK]')
	except:
		print ('MySQL connection:\t\t\t[Fail]')
	
	try:
		db_curs = db_conn.cursor()
		print ('Cursor:\t\t\t[OK]')
	except:
		print ('Cursor:\t\t\t[Fail]')

	sl_data = sl_data[['sl_uuid',
						'churn_flag',
						'device_switch',
						'customer_life',
						'device_type',
						'multiline_flag']]
	tk_data = tk_data[['sl_uuid', 'num_tickets']]
	tt_data = tt_data[['sl_uuid', 
						'group_name', 
						'request_type', 
						'ticket_class', 
						'ticket_type', 
						'tags', 
						'resolution_time',
						'diff_date']]
	
	
	#---------PROCESS STEP:- MERGE DATA:: service line + number of tickets		
	model_data = pd.merge(sl_data, tk_data, on=['sl_uuid'], how='left')
	print model_data.columns
	#---------PROCESS STEP:- Get distinct values of dummy variables
	group_name = tt_data['group_name'].unique().tolist()
	request_type = tt_data['request_type'].unique().tolist()
	ticket_class = tt_data['ticket_class'].unique().tolist()
	ticket_type = tt_data['ticket_type'].unique().tolist()
	tags = tt_data['tags'].tolist()
	tags_distinct = []
	for i in tags:
		split_strings = i.split(' ')
		for j in split_strings:
			if j not in tags_distinct:
				tags_distinct.append(j)
	
	#@substep: write the unique values to files
	group_file = open('Dummy variables/Group_name.txt','w')
	request_file = open('Dummy variables/Request_type.txt','w')
	tclass_file = open('Dummy variables/Ticket_class.txt','w')
	tag_file = open('Dummy variables/Tag_name.txt','w')
	ttype_file = open('Dummy variables/Ticket_type.txt','w')
	
	for g in group_name:
		group_file.write(str(g) + '')
		group_file.write('\n')
	
	for r in request_type:
		request_file.write(str(r) + '')
		request_file.write('\n')

	for tc in ticket_class:
		tclass_file.write(str(tc) + '')
		tclass_file.write('\n')

	for t in tags_distinct:
		tag_file.write(str(t) + '')
		tag_file.write('\n')

	for tt in ticket_type:
		ttype_file.write(str(tt) + '')
		ttype_file.write('\n')
	print ("Writing to files:\t\t\t[OK]")
	
	group_file.close()
	tclass_file.close()
	tag_file.close()
	ttype_file.close()
	request_file.close()
	dummy_var_header = group_name + request_type + ticket_class + ticket_type
	dummy = {}
	sl_uuid = model_data['sl_uuid'].unique()
	for i in sl_uuid:
		subset = tt_data[tt_data['sl_uuid'] == i]
		dummy_indiv = []
		if len(subset) > 0:
			#@comment - has some tickets and can be worked on creating dummy
			#group_name
			for x in group_name:
				if x in subset['group_name'].tolist():
					dummy_indiv.append(subset['group_name'].tolist().count(x))
				else:
					dummy_indiv.append(0)
			
			for x in request_type:
				if x in subset['request_type'].tolist():
					dummy_indiv.append(subset['request_type'].tolist().count(x))
				else:
					dummy_indiv.append(0)

			for x in ticket_class:
				if x in subset['ticket_class'].tolist():
					dummy_indiv.append(subset['ticket_class'].tolist().count(x))
				else:
					dummy_indiv.append(0)

			for x in ticket_type:
				if x in subset['ticket_type'].tolist():
					dummy_indiv.append(subset['ticket_type'].tolist().count(x))
				else:
					dummy_indiv.append(0)
			'''		
			sl_tags = []
			temp = subset['tags'].tolist()
			for y in temp:
				sl_tags += y.split(' ')
			sl_tags = list(set(sl_tags))	
			for x in tags_distinct:
				if x in sl_tags:
					dummy_indiv.append(1)
				else:
					dummy_indiv.append(0)
			'''
			dummy[i] = dummy_indiv								
		else:
			dummy[i] = [0] * len(dummy_var_header)
	model_df = pd.DataFrame.from_dict(dummy, 'index')
	model_df.columns = dummy_var_header	
	model_df['sl_uuid'] = model_df.index
	model_df = pd.merge(model_df, model_data, on=['sl_uuid'], how='left')
	
	#@sql write: to local db -> model dataset
	try:
		db_curs.execute('DROP TABLE model_data;')
	except:
		print ('Table not present')
	pk.dump(model_df, open('/home/rsrash1990/Ravi Files/Work Projects/Churn Model/Pickle data/model_data.pk','w'))
	#model_df.to_sql(con=db_conn, name='model_data', if_exists='append', flavor='mysql')
	#print model_df.head()
	#print model_df.describe()
	'''
	for i in model_df.columns:
		temp = model_df[i].tolist()
		print (i + '\t:' + str(sum(x is None for x in temp)))
	'''
	print model_df.columns
	model = model_df[['sl_uuid', 'churn_flag']]
	print model.describe()
	return model_df	
	

def train_test_split(model_df, percent, num_bootstraps):
	'''
	Splits the model dataset into the required train and test data sets
	Use 'percent' to first split train and test
	Then use the train data set to understand how much churned subs are
		present
	Split unchurned subs into multiple random selections equivalent in 
		size to the churned subs set
	Return each dataset as train set in a dictionary
	'''
	master_train, test_data = tts(model_df, test_size=percent)
	train_churn = master_train[master_train['churn_flag'] == 1]
	train_uchurn = master_train[master_train['churn_flag'] == 0]
	
	train_subsample_size = int(len(train_churn) * 0.8)
	sub_uchurn_percent = float(train_subsample_size)/float(len(train_uchurn))
	
	train_dsamples = {}
	
	for i in range(num_bootstraps):
		down_train_uchurn, dummy = tts(train_uchurn, test_size= 1.0 - sub_uchurn_percent)
		down_train_churn, dummy = tts(train_churn, test_size = 0.2)
		
		
		
def main():
	#---------PROCESS STEP:- CREATING VARIABLES FROM COMM LINE ARGS
	arguments = sys.argv[1:]
	EXECUTE_PHASE = arguments[0]
	START_DATE = arguments[1]
	END_DATE = arguments[2]
	GOBACK_TIME = arguments[3]
	print type(arguments[0])
	print arguments[1]
	print arguments[2]
	
	#---------PROCESS STEP:- PROCESS WHICH PHASE TO EXECUTE:
	if EXECUTE_PHASE == '1':
		#Start from data pull
		raw_data_dict = raw_data_pull(START_DATE, END_DATE, GOBACK_TIME)
		sl_data = raw_data_dict['sl_data']
		tk_data = raw_data_dict['tk_data']
		tt_data = raw_data_dict['tt_data']
		model_df = data_transform(sl_data, tk_data, tt_data)
	
	elif EXECUTE_PHASE == '2':
		#Skip data pull
		raw_data_dict = data_load()
		sl_data = raw_data_dict['sl_data']
		tk_data = raw_data_dict['tk_data']
		tt_data = raw_data_dict['tt_data']
		model_df = data_transform(sl_data, tk_data, tt_data)
		
		

#Main Execution:
if __name__ == '__main__':
	main()
	'''
	python model_run.py 2 2013-12-01 2015-01-01 60
	python model_run.py 1 2013-12-01 2015-01-01 60
	'''
