
from pymongo import MongoClient as mc
from pymongo import errors as mongoerror

#mongo_host="sundry.wifitics.com"
mongo_port=27017

#mongo_proximity1_username="proxiana"
#mongo_proximity1_password="proximfiltereddata"
mongo_host="3.6.162.156"
mongo_proximity1_username="nmswifitics"
mongo_proximity1_password="nms221308"

mongo_proximity_db="proximity_macids"
url="mongodb://{}:{}@{}:{}/?authSource={}"
url=url.format(mongo_proximity1_username,mongo_proximity1_password,mongo_host,mongo_port,mongo_proximity_db)
mongo_client=mc(url,appname="wt_mongo_client",connect=False,maxPoolSize=500)
mongo_conn=mongo_client.proximity_macids
rss=[]
a=list(mongo_conn.list_collection_names())
print(a)

prefixes = ('IMF', 'RU','ping_')
newlist = [x for x in a if not x.startswith(prefixes)]
print(newlist)
newlist=newlist[:2]

for j in range(len(newlist)) :
    cursor=mongo_conn[newlist[j]].find()

    docs=list(cursor)

    for i in range(len(docs)):
        rss.append((docs[i]['rss']))
print("rss=", rss)






