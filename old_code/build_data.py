import os
import haversine

import time
from dateutil.parser import parse

f = open('data/nyc_infected_total_3.csv','r')
w = open('data/nyc_infected_3.csv','w')
persons = set()

min = 10
max = -1
j = 0
for line in f.readlines():
	line = line.strip().split(',')
	if(j==0):
		j+=1
		continue
	
	#print(line[9])
	if(line[1] not in persons and int(line[9])!=0):
		persons.add(line[1])
		w.write("{:},{:},{:}\n".format(line[1],line[12],line[11]))

	# if(line[0] not in persons):
	# 	persons.add(line[0]) 'Jun 1 2005  1:33PM' '%b %d %I:%M%p %Y' 
	# dt = line[7]
	# dtime = parse(dt)
	# dtime = time.mktime(dtime.timetuple())
	# w.write("{:},{:},{:}\n".format(line[0],line[1],dtime))
	# if(line[1] not in places):
	# 	places.add(line[1])
	# 	w.write("{:},{:}\n".format(line[1],line[3]))
		
		# timestamp = dt.timestamp()
		# print(timestamp)
w.close()
#print(len(places))
# places = list(places)

# # i = 1
# for i in range(1,max+1):
# 	w.write("{:},{:},{:}\n".format(i,i,"person"))
#print(haversine.Haversine([-84.412977,39.152501],[-84.412946,39.152505]).feet)