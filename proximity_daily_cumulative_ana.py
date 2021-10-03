#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
__author__ = "Shiva Shankar Aradi"
__copyright__ = "Copyright 2017, shiva shankar aradi"
__credits__ = []
__os__ == "Ubuntu 16.x"
__license__ = "MIT"
__version__ = "1.0.0"
__maintainer__ = "Shiva Shankar Aradi"
__github__ = "shiva-aradi"
__email__ = "developer.aradi@gmail.com"
__status__ = "Production"
__important-modules__ = ["pandas","matplotlib","multiprocessing","numpy","datetime","dateutil","pymongo"]
__language__ = "Python2.7"

"""

import os, json, signal
from datetime import datetime
from dateutil import tz
from pymongo import MongoClient as mc
from pymongo import errors as mongoerror
import multiprocessing as multip
import calendar
from pymongo import MongoClient
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import logging 

#mongo_host="sundry.wifitics.com"
mongo_host="3.6.162.156"

mongo_port=27017

#mongo_proximity1_username="proxiana"
#mongo_proximity1_password="proximfiltereddata"

mongo_proximity1_username="nmswifitics"
mongo_proximity1_password="nms221308"

mongo_proximity1_db="proximity_ana_filtered"
url1="mongodb://{}:{}@{}:{}/?authSource={}"
url1=url1.format(mongo_proximity1_username,mongo_proximity1_password,mongo_host,mongo_port,mongo_proximity1_db)
mongo_client1=mc(url1,appname="wt_mongo_client",connect=False)
mongo_conn1=mongo_client1.proximity_ana_filtered



#mongo_proximity_username="proxiwifitics"
#mongo_proximity_password="prxiwifitics221308"
mongo_proximity_username="nmswifitics"
mongo_proximity_password="nms221308"

mongo_proximity_db="proximity_macids"
url="mongodb://{}:{}@{}:{}/?authSource={}"
url=url.format(mongo_proximity_username,mongo_proximity_password,mongo_host,mongo_port,mongo_proximity_db)
mongo_client=mc(url,appname="wt_mongo_client",connect=False)
mongo_conn=mongo_client.proximity_macids

mongo_client_fork=mc(url,appname="wt_mongo_client",connect=False)
mongo_conn_after_fork=mongo_client_fork.proximity_macids
logging.basicConfig(filename='/var/log/proximity_analytics/proximity_daily_cumulative_ana',level=logging.DEBUG)


def compute_module(unique):
	ids_0M_5M=0
        ids_5M_30M=0
        ids_30M_1H=0
	ids_1H_2H=0
        ids_2H_6H=0
        ids_6H_12H=0
	ids_12H_24H=0

	bigdata=[]
        bigdata_once=mongo_conn_after_fork[maci].find({"created_at":{'$gte':start_datetime_epoch,'$lte':end_datetime_epoch}}).sort("created_at",1)

        for bd in bigdata_once:
                bigdata.append(bd)

	
	for macli in unique:
        	#out=mongo_conn_after_fork[maci].find({'$and':[{"callingstationid":unique[macli]},\
                #                                           {'$and':[{"created_at":{'$gte':start_datetime_epoch}},\
                #                                           {"created_at":{'$lte':end_datetime_epoch}}]}]})\
                #                                           .sort("created_at",1)
                #out=mongo_conn["18:A6:F7:7C:5B:1E"].find({"callingstationid":unique[macli]}).sort("created_at",1)
                out=[]
                total_time_spent=0

		count=0
                mongo_list=[]
                data_list=[]
                slope_data=[]
                slope_data_refined=[]
                slope_data_odd_values=[]
                x_axis=[]
                y_axis=[]
                data01={}
                for c_data in bigdata:
                    if macli == c_data["callingstationid"]:
                        out.append(c_data)

                for p in out:
                    mongo_list.append(p)
                    x_axis.append(p['rss'])
                    y_axis.append(p['created_at'])
                    count=count+1
#print mongo_list
                data01['DateTime_Spent']=y_axis
                data01['RSSI_Distance']=x_axis
                p_g=pd.DataFrame(data01,columns=["RSSI_Distance","DateTime_Spent"],index=data01["DateTime_Spent"])
  #BELOW is slope based on rssi... +ve=coming in , -ve=going out
                slope_data=p_g.diff(periods=1,axis=0)
# convert timedelta datetime to seconds
                #slope_data['DateTime_Spent']=slope_data['DateTime_Spent'].astype('timedelta64[s]')
                slope_data['dt_std']=pd.rolling_std(slope_data['DateTime_Spent'],2)
#print "when dviation is more",slope_data.dt_std > 2000
                slope_data_refined=slope_data[ slope_data.dt_std < 2000 ]
                #slope_data_odd_values=slope_data[ slope_data.dt_std > 2000 ]

                total_time_spent=(slope_data_refined['DateTime_Spent'].sum())/60
                if total_time_spent < 5:
                    ids_0M_5M = ids_0M_5M + 1
                if total_time_spent > 5 and total_time_spent < 30:
                    ids_5M_30M = ids_5M_30M + 1
                if total_time_spent > 30 and total_time_spent < 60:
                    ids_30M_1H = ids_30M_1H +1
		if total_time_spent > 60 and total_time_spent < 120:
		    ids_1H_2H = ids_1H_2H + 1
                if total_time_spent > 120 and total_time_spent < 360:
                    ids_2H_6H = ids_2H_6H + 1
		if total_time_spent > 360 and total_time_spent < 720:
                    ids_6H_12H = ids_6H_12H + 1
                if total_time_spent > 720:
                    ids_12H_24H = ids_12H_24H + 1

         # insert to mongo date-hourwise
	return ids_0M_5M,ids_5M_30M,ids_30M_1H,ids_1H_2H,ids_2H_6H,ids_6H_12H,ids_12H_24H

#### call the below every end of the day
def scan_every_hour_proxi_ana(date,start_hour,end_hour):
	crazy_hour=1
        if crazy_hour:
	  ids_0M_5M=0
          ids_5M_30M=0
          ids_30M_1H=0
          ids_1H_2H=0
          ids_2H_6H=0
          ids_6H_12H=0
          ids_12H_24H=0
	  global maci
          global start_datetime_epoch
	  global end_datetime_epoch
	  global start_date
	  global today_date_epoch
	  start_date="{} {:02}:00:00".format(date,start_hour)
	  end_date="{} {:02}:{:02}:00".format(date,end_hour,59)
	  today_date="{} 00:00:00".format(date)
          #print start_date, end_date	  
        #start_d=datetime.strptime('2018-04-23 00:00:00', '%Y-%m-%d %H:%M:%S')
          start_d=datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
          start_datetime_epoch=calendar.timegm(start_d.timetuple()) - 19800
          #print "epoch start: ",start_datetime_epoch
        #end_d=datetime.strptime('2018-04-23 17:05:00', '%Y-%m-%d %H:%M:%S')
          end_d=datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
          end_datetime_epoch=calendar.timegm(end_d.timetuple()) - 19800

	  today_date = datetime.strptime(today_date, '%Y-%m-%d %H:%M:%S')
	  today_date_epoch = calendar.timegm(today_date.timetuple()) - 19800

          #print "epoch end: ",end_datetime_epoch
	  collection_name="mactable"
	  proxi_router_mac_list=mongo_conn1.mactable.distinct("mac",{"disable_probe":{'$eq':0}})
	  for maci in proxi_router_mac_list:
	    ids_0M_5M=0
            ids_5M_30M=0
            ids_30M_1H=0
            ids_1H_2H=0
            ids_2H_6H=0
            id_6H_12H=0
            id_12H_24H=0
	    unique=mongo_conn[maci].distinct("callingstationid",{'$and':[{"created_at":{'$gte':start_datetime_epoch}},\
{"created_at":{'$lte':end_datetime_epoch}}]})
	
	  #print "unique mac_ids : ",len(unique)
	    if len(unique):
		uni=[unique[i:i+300] for i in range(0, len(unique), 300)]
	    	mpool=multip.Pool(processes=7)
		li=mpool.map_async(compute_module,uni)
		for i in li.get():
    			ids_0M_5M = ids_0M_5M + i[0]
    			ids_5M_30M = ids_5M_30M + i[1]
    			ids_30M_1H = ids_30M_1H + i[2]
    			ids_1H_2H = ids_1H_2H + i[3]
			ids_2H_6H = ids_2H_6H + i[3]
			ids_6H_12H = ids_6H_12H + i[3]
			ids_12H_24H = ids_12H_24H + i[3]

		daily_j={}
        	daily_j['unique_mac_ids']=len(unique)
        	daily_j['ids_0M_5M']=ids_0M_5M
        	daily_j['ids_5M_30M']=ids_5M_30M
        	daily_j['ids_30M_1H']=ids_30M_1H
        	daily_j['ids_1H_2H']=ids_1H_2H
		daily_j['ids_2H_6H']=ids_2H_6H
		daily_j['ids_6H_12H']=ids_6H_12H
		daily_j['ids_12H_24H']=ids_12H_24H
        	daily_j['_id']=start_date
        	daily_j['epoch']=today_date_epoch
        	try:
              		daily_collection="DAILY:{}".format(maci)
              		mongo_conn1[daily_collection].insert_one(daily_j)
        	except (AttributeError, mongoerror.OperationFailure):
                #print "Already Entered Date_Time_Hour Data"
			pass
              		#return None
        	except (AttributeError, mongoerror.ConnectionFailure):
                  #print "Unable to Connect to MongoDB"
			pass
              		#return None
		mpool.close()
                mpool.join()
	    else:
                daily_j={}
                daily_j['unique_mac_ids']=0
                daily_j['ids_0M_5M']=0
                daily_j['ids_5M_30M']=0
                daily_j['ids_30M_1H']=0
                daily_j['ids_1H_2H']=0
                daily_j['ids_2H_6H']=0
                daily_j['ids_6H_12H']=0
                daily_j['ids_12H_24H']=0
                daily_j['_id']=start_date
                daily_j['epoch']=today_date_epoch
                try:
                        daily_collection="DAILY:{}".format(maci)
                        mongo_conn1[daily_collection].insert_one(daily_j)
                except (AttributeError, mongoerror.OperationFailure):
                #print "Already Entered Date_Time_Hour Data"
			pass
                        #return None
                except (AttributeError, mongoerror.ConnectionFailure):
                  #print "Unable to Connect to MongoDB"
			pass
                        #return None


zone=tz.tzoffset('IST',19800)
date_time = datetime.utcnow().replace(microsecond=0,tzinfo=tz.tzutc())
date_time=date_time.astimezone(zone)
this_date=str(date_time.date())
start_hour=0
end_hour=23
logging.debug("Started: {}".format(datetime.now()))
### run the CRON command at every 23:58 PM
scan_every_hour_proxi_ana(this_date,start_hour,end_hour)
logging.debug("Ended: {}".format(datetime.now()))

