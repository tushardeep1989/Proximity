#!/usr/bin/env python

import os, json, signal
from datetime import datetime
from dateutil import tz
from pymongo import MongoClient as mc
from pymongo import errors as mongoerror
import calendar
from pymongo import MongoClient
import matplotlib.pyplot as plt
from matplotlib.pyplot import *
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import multiprocessing as multip
mongo_host="3.6.162.156"
#mongo_host="sundry.wifitics.com"
mongo_port=27017


mongo_proximity1_username="nmswifitics"
#mongo_proximity1_username="proxiana"
mongo_proximity1_password="nms221308"
#mongo_proximity1_password="proximfiltereddata"
mongo_proximity1_db="proximity_ana_filtered"
url1="mongodb://{}:{}@{}:{}/?authSource={}"
url1=url1.format(mongo_proximity1_username,mongo_proximity1_password,mongo_host,mongo_port,mongo_proximity1_db)
mongo_client1=mc(url1,appname="wt_mongo_client",connect=False)
mongo_conn1=mongo_client1.proximity_ana_filtered

#mongo_proximity_username="proxiwifitics"
mongo_proximity_username="nmswifitics"
#mongo_proximity_password="prxiwifitics221308"
mongo_proximity_password="nms221308"
mongo_proximity_db="proximity_macids"
url="mongodb://{}:{}@{}:{}/?authSource={}"
url=url.format(mongo_proximity_username,mongo_proximity_password,mongo_host,mongo_port,mongo_proximity_db)
mongo_client=mc(url,appname="wt_mongo_client",connect=False)
mongo_conn=mongo_client.proximity_macids


mongo_client=mc(url,appname="wt_mongo_client",connect=False)
mongo_conn_after_fork=mongo_client.proximity_macids

#maci=""
#start_date=""
#end_date=""

def compute_module(unique):
	passerby=0
        sticky_passerby=0
        customer=0
        sticky_customer=0
	for macli in unique:
                #print "N:",macli
            #out=mongo_conn[maci].find({"callingstationid":unique[macli]}).sort("created_at",1)
	    out=mongo_conn_after_fork[maci].find({'$and':[{"callingstationid":macli},\
                                                           {'$and':[{"created_at":{'$gte':start_datetime_epoch}},\
                                                           {"created_at":{'$lte':end_datetime_epoch}}]}]})\
                                                           .sort("created_at",1)
            count=0
            mongo_list=[]
            data_list=[]
            slope_data=[]
            slope_data_refined=[]
            slope_data_odd_values=[]
            x_axis=[]
            y_axis=[]
            data01={}
            for p in out:
                mongo_list.append(p)
                x_axis.append(p['rss'])
                y_axis.append(p['created_date'])
                count=count+1
    #print "\nT_count: "+str(count)
#print mongo_list
            data01['DateTime_Spent']=y_axis
            data01['RSSI_Distance']=x_axis
            p_g=pd.DataFrame(data01,columns=["RSSI_Distance","DateTime_Spent"],index=data01["DateTime_Spent"])
  #BELOW is slope based on rssi... +ve=coming in , -ve=going out
            slope_data=p_g.diff(periods=1,axis=0)
# convert timedelta datetime to seconds
            slope_data['DateTime_Spent']=slope_data['DateTime_Spent'].astype('timedelta64[s]')
            slope_data['dt_std']=pd.rolling_std(slope_data['DateTime_Spent'],2)
#print "when dviation is more",slope_data.dt_std > 2000
            slope_data_refined=slope_data[ slope_data.dt_std < 2000 ]
                #slope_data_odd_values=slope_data[ slope_data.dt_std > 2000 ]
            total_time_spent=(slope_data_refined['DateTime_Spent'].sum())/60
            if total_time_spent < 5:
                passerby = passerby + 1
            if total_time_spent > 5 and total_time_spent < 30:
                sticky_passserby = sticky_passerby + 1
            if total_time_spent > 30 and total_time_spent < 60:
                customer = customer +1
            if total_time_spent > 60:
                sticky_customer = sticky_customer + 1
            mac_footprint={}
            mac_footprint['total_time_spent']=total_time_spent
            mac_footprint['_id']=str(start_datetime_epoch)+str(macli)
            mac_footprint['epoch']=start_datetime_epoch
            mac_footprint['date_time']=start_date
            mac_footprint['callingstationid']=macli
            try:
                    mac_id_format="IMF:{}".format(maci)
                    mongo_conn_after_fork[mac_id_format].insert_one(mac_footprint)
            except (AttributeError, mongoerror.OperationFailure):
                    return None
            except (AttributeError, mongoerror.ConnectionFailure):
                    return None



#### call the below every end of the day
def scan_every_day_once_proxi_ana(date,from_hour,end_hour):
	#last_entered_date_time=mongo_conn1["18:A6:F7:7C:5B:1E:test"].find().sort("epoch",1).limit( 1 )
	#for i in last_entered_date_time
	#	print i['epoch']
	crazy_hour=1
	global maci
        global start_datetime_epoch
	global end_datetime_epoch
	global start_date

        if crazy_hour:
	  start_date="{} {:02}:00:00".format(date,from_hour)
	  end_date="{} {:02}:{:02}:00".format(date,end_hour,59)
	  
        #start_d=datetime.strptime('2018-04-23 00:00:00', '%Y-%m-%d %H:%M:%S')
          start_d=datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
          start_datetime_epoch=calendar.timegm(start_d.timetuple()) - 19800
          #print "epoch start: ",start_datetime_epoch
        #end_d=datetime.strptime('2018-04-23 17:05:00', '%Y-%m-%d %H:%M:%S')
          end_d=datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
          end_datetime_epoch=calendar.timegm(end_d.timetuple()) - 19800
          #print "epoch end: ",end_datetime_epoch
	  proxi_router_mac_list=mongo_conn1["mactable"].distinct("mac",{"proximity_status":{'$eq':1}})
          for maci in proxi_router_mac_list:
	    passerby=0
            sticky_passerby=0
            customer=0
            sticky_customer=0

	    unique=mongo_conn[maci].distinct("callingstationid",{'$and':[{"created_at":{'$gte':start_datetime_epoch}},\
{"created_at":{'$lte':end_datetime_epoch}}]})
	    uni=[unique[i:i+3] for i in range(0, len(unique), 3)]
	    print "unique mac_ids : ",len(unique)
	    if len(unique):
		print unique[0]
		mpool=multip.Pool(processes=4)
		li=mpool.map(compute_module,uni)


#print "starting ana"
zone=tz.tzoffset('IST',19800)
date_time = datetime.utcnow().replace(microsecond=0,tzinfo=tz.tzutc())
date_time=date_time.astimezone(zone)
#this_date="2018-05-07"
this_date="2020-07-22"
this_hour=date_time.hour
from_hour=00
end_hour=23
scan_every_day_once_proxi_ana(this_date,from_hour,end_hour)
