
from pymongo import MongoClient as mc
from pymongo import errors as mongoerror
from pymongo import *

#mongo_host="sundry.wifitics.com"
mongo_port=27017

#mongo_proximity1_username="proxiana"
#mongo_proximity1_password="proximfiltereddata"
mongo_host="3.6.162.156"
mongo_proximity2_username="tushar"
mongo_proximity2_password="tushar422"




mongo_proximity_db="proximity_macids"
url="mongodb://{}:{}@{}:{}/?authSource={}"
url=url.format(mongo_proximity2_username,mongo_proximity2_password,mongo_host,mongo_port,mongo_proximity_db)
mongo_client=mc(url,appname="wt_mongo_client",connect=False,maxPoolSize=500)
mongo_conn=mongo_client.proximity_macids


url2="mongodb://{}:{}@{}:{}/?authSource=proximity_distance_filter&authMechanism=SCRAM-SHA-256"
url2=url2.format(mongo_proximity2_username,mongo_proximity2_password,mongo_host,mongo_port)
mongo_client2=mc(url2,appname="wt_mongo_client2")
db=mongo_client2['proximity_distance_filter']
collection = db['18:A6:F7:72:D5:CA']
#mongo_conn=mongo_client2.proximity_distance
#mongo_conn = mongo_client2['proximity_distance_filter']

#mongo_proximity1_username="proxiana"
#mongo_proximity1_password="proximfiltereddata"
mongo_proximity2_username="nmswifitics"
mongo_proximity2_password="nms221308"




a=list(mongo_conn.list_collection_names())


prefixes = ('IMF', 'RU','ping_')
newlist = [x for x in a if not x.startswith(prefixes)]


cursor=mongo_conn['18:A6:F7:72:D5:CA'].find()

docs=list(cursor)
rss=[]
callingstationid=[]
for i in range(len(docs)):
    rss.append((docs[i]['rss']))
    callingstationid.append((docs[i]['callingstationid']))

ans = []

for x in rss:
    def res(x):
        return 10 ** ((-69-(x))/(10 * 2))
    ans.append(res(x))
mydictionary = dict(zip(callingstationid, ans))
print(mydictionary)
for items in mydictionary :
    collection.insert_one(items)

D_0M_5M=0
D_5M_10M=10
for value in ans :
    if value >=1 and value <5 :
        D_0M_5M=D_0M_5M+1
    elif 5 <= value < 10:
        D_5M_10M = D_5M_10M + 1
print(D_0M_5M)

#add a dictionary for distance corresponding to calling station id's - i am working on this
#dump the data in the table proximity_distance_filter
# this is being done for a single router which is a table(collection) in the DB.
# do this for all the tables -- follow the code in space 2


