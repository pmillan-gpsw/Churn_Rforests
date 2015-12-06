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
	pk.dump(sl_data, open('sl_data.pk','w'))
	tk_data = redshift.query(TK_QUERY)
	pk.dump(tk_data, open('tk_data.pk','w'))	
	tt_data = redshift.query(TT_QUERY)
	pk.dump(tt_data, open('tt_data.pk','w'))	
	'''		
	sl_data.to_sql(con=db_conn, name='customer_sldata', if_exists='append', flavor='mysql')
	tk_data.to_sql(con=db_conn, name='customer_tkdata', if_exists='append', flavor='mysql')
	tt_data.to_sql(con=db_conn, name='customer_ttdata', if_exists='append', flavor='mysql')	
	'''
	
	#---------PROCESS STEP:- MERGE (SL + TK + TT) + CREATE DUMMY VARIABLES -> MODEL DATA
	model_data = pd.merge(sl_data, tk_data, on=['sl_uuid'], how='left')
	print model_data.columns


def main():
	#---------PROCESS STEP:- CREATING VARIABLES FROM COMM LINE ARGS
	arguments = sys.argv[1:]
	START_DATE = arguments[0]
	END_DATE = arguments[1]
	GOBACK_TIME = arguments[2]
	print type(arguments[0])
	print arguments[1]
	print arguments[2]
	raw_data_pull(START_DATE, END_DATE, GOBACK_TIME)
	


#Main Execution:
if __name__ == '__main__':
	main()
