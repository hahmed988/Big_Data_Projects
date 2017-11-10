#Apply/Deploy the previously saved/built model on streaming data and make cluster predictions for each of the 
streaming record.

## To execute this as batch script.
## Run the spark programme to apply the model to test data.
## spark-submit ./Scripts/3_uber_test.py
## Make sure set the appropriate path in the program to save results before executing the script.

#Create RDD uber_test_data, for streaming data
uber_test_data = sc.textFile('/user/1289B29/uber/StreamData/0/part-00000')

#Split the fields as a comma separator
uber_fea_test_data = uber_test_data.map(lambda line : line.split(','))

#Define a function convertDataFloat, to generate features To cast Longitude and Latitude as float and return these
features as arrays
def convertDataFloat(line):
    return array([float(line[1]),float(line[2])])

#Using a map transformation and apply the above function to each line
parsedTestData = uber_fea_test_data.map(lambda line : convertDataFloat(line)) 

#Load the previously saved model
savedModel = KMeansModel.load(sc, "/user/1289B29/uber/kmeanModel/")

#Verify Cluster Centers
savedModel.centers

#Predict clusters for the parsed streaming data
outputRdd = savedModel.predict(parsedTestData)

#Verify the first few records from the above predictions RDD
outputRdd.take(10)

#Add an unique index to the each data point to the base RDD and Result RDD - Required for joins
uber_fea_test_data_1 = uber_fea_test_data.zipWithIndex()
outputRdd_1 = outputRdd.zipWithIndex()

#Verify the base RDD and result RDD
uber_fea_test_data_1.take(10)
outputRdd_1.take(2)


#Find the no. of data points in each cluster - Alternative way of count and reduceByKey
predictionsValue = outputRdd.collect()
predictionsValue
#cl_cnt = outputRdd.map(lambda point:(point,1)).reduceByKey(lambda a,b:a+b) 
#cl_cnt.collect()

from collections import Counter
valsCount = Counter(predictionsValue) ## Returns counter type from collections
valsCount

countDict = dict((x,predictionsValue.count(x)) for x in set(predictionsValue))
countDict ## Returns dictionary 

#Flip Key and Values - Key - Index , Value - Data point and cluster numbers
uber_fea_test_data_2 = uber_fea_test_data_1.map(lambda x : (x[1],(x[0][0],x[0][1],x[0][2],x[0][3])))
outputRdd_2 = outputRdd_1.map(lambda x : (x[1],x[0]))

#Join the RDDs based on index
finalWriteRdd = uber_fea_test_data_2.join(outputRdd_2)
finalWriteRdd.take(2)

finalWriteRdd = finalWriteRdd.map(lambda x : x[1]) 
finalWriteRdd.take(2)

#Define a function to extract only hour from the date and time
def convertTSToHour(value):
    if value and not value.isspace():
        #value = "2/28/2017 0:00:00"
        time = value.split(" ")[1].split(":")
        return int(time[0])
        
#Using Map transformation apply the above function on each line of the RDD     
finalWriteRdd = finalWriteRdd.map(lambda y : (convertTSToHour(y[0][0]),y[0][1],y[0][2],y[0][3],y[1]))

#Define a function to add comma as separator
def toCSVLine(data):
    return ','.join(str(d) for d in data)

#Define a function for csv conversion
def toCSVLine1(data):
    return ','.join(str(d) for d in data)
	
#Using Map transformation apply the above function on each line of the RDD
lines = finalWriteRdd.map(toCSVLine1)

#Write the results to local file system
lines.coalesce(1).saveAsTextFile("file:///home/1289B29/Uber/Results")