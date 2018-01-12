# Sample script to show how to use MongoDB Aggregation Framework
# Retrieves top 10 users that sent email during off hours

import pymongo
import time
from datetime import datetime

# searches off hours stats for a specific user
def findUserOH(tab,user):
    value=0
    for dict in tab:
        if dict['_id']==user:
            value=dict['count']
    return value

# calc elapsed time since start
def elapsedTime(timeSet):
    elapsed = time.time()-timeSet
    return round(elapsed*1000)/1000

chrono = time.time()

# connect to MongoDB
c = pymongo.MongoClient(host=["mongodb://localhost:27017"])
db = c.enron
coll = db.messages
print "Mongodb connected (%s)" % elapsedTime(chrono)

# set variables for aggregation
start = datetime(2000, 3, 1, 0, 0, 0)
end = datetime(2003, 4, 1, 0, 0, 0)

matchDate = { 
		"$match":{
			"headers.Date" : {
				"$gte" : start,
				"$lt" : end
				}
			}
	}

# filter on Enron employee senders only
enronSource = {
		"$match" : { "headers.From": {"$regex": "@enron.com$" }},
}

# collect send time and sender per email
project = {
		"$project" : {
		"sender" : "$headers.From",
		"hour" : {"$hour" : "$headers.Date"}
		}
}

# define off hours
offHours = {
	"$match" : {
                   "$or" : [
                           {"hour" : {"$lt" : 6}},
                           {"hour" : {"$gt" : 20}}
                         ]
        	}
}

ohCountSort = {
		"$sortByCount" : "$sender"
}

limit = {
        "$match" : {
                    "count" : {"$gt":50}
                    }
}

# facets both results (total sent mails, off hours sent mail)
facetMailUsage = {
		    "$facet" : {
				"numberOfMailSent" : [ matchDate,
                                                        enronSource,
							project,
							ohCountSort,
                                                        limit],
				"mailsOffHours" : [ matchDate,
                                                        enronSource,
                        				project,
							offHours,
							ohCountSort]
		    }
}

# run aggregation pipeline
cursor = coll.aggregate([facetMailUsage])
print "Aggregate ran (%s)" % elapsedTime(chrono)

# parse results and fill result list
facet = list(cursor)
total = facet[0]['numberOfMailSent']
offHours = facet[0]['mailsOffHours']
result=[]
count = 0

for doc in total:
    user = doc['_id']
    sentTotal = doc['count']
    sentOH = findUserOH(offHours,user)
    resultuser = {"user" : user, "sent" : sentTotal, "sentOH" : sentOH, "ratio" : sentOH*100/sentTotal }
    result.append(resultuser.copy())
    count=count+1

print "result list created with {} objects ({})".format(count,elapsedTime(chrono))

# sort result list
sortedResult = sorted(result, None,lambda k: k['ratio'],True)
print "result list sorted (%s)" % elapsedTime(chrono)
print

# display top 10 off-hours mail senders
print "Here's the top 10"
for user in sortedResult[:10]:
    print "user : {}  total sent : {} Off-hours ratio : {} %".format(user['user'], user['sent'],user['ratio'])

print
print "Exec done (%s)" % elapsedTime(chrono)
