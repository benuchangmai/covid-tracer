import pandas as pd
from tqdm import tqdm
from neo4j_driver import DBHelper


uri = "bolt://localhost:7687"
password = 'siso@123'
# uri = "bolt://34-68-238-91.gcp-neo4j-sandbox.com:7687"
# password = 'MfGCmB2upGq4ku7F'
db = DBHelper(uri, password)
node_counts = []
f = open("data/nyc_infected_2.csv",'r')
infectedDict = {}
notifDict = {}
infectors = set()
j = 0
lines = f.readlines()
for line in tqdm(lines):
	line = line.strip().split(',')
	user = int(line[0])
	timestamp = int(line[1])
	infectors.add(int(line[2]))
	infectedDict[user] = timestamp
	#node_count = db.find_neighbors_count(user, 2, timestamp)
	
	nodes = db.find_neighbors(user, 2, timestamp)
	
	for node in nodes:
	 	#print(node[0]['id'])
	 	if(node[0]['id'] not in notifDict.keys()):
	 		notifDict[node[0]['id']] = timestamp
	nodes2 = db.find_neighbors_next(user, 2, timestamp)
	for node in nodes2:
	 	#print(node[0])
	 	if(node[0]['id'] not in notifDict.keys()):
	 		notifDict[node[0]['id']] = timestamp
		#node_counts.append(node[0])
pos = 0.0
total = 0.0
avoids = 0
useful_avoids = 0
for i in range(1,1084):
	if(i not in infectedDict.keys()):
		continue
	total+=1
	if(i in notifDict.keys()):
		if(i in list(infectors)):
			avoids+=1
		if(notifDict[i]<infectedDict[i]):
			pos+=1
			if(i in list(infectors)):
				useful_avoids+=1
			

# for i in notifDict.keys():
# 	#print(i)
# 	if(i in list(infectors)):
# 		avoids+=1
#print(infectors)
#print(notifDict.keys())
print("Infectors (People notified who would go on to infect others later): ", avoids)
print("Useful avoids: ", useful_avoids)
print("Total #Infectors: ", len(infectors))
print("Total infected: ", total)
print("Notified in time: ", pos)
print("Recall: ", pos/total)
#print(node_counts)