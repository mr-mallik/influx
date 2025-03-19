##################################################
## Author: Ashley Cusack
## Email: A.Cusack2@hud.ac.uk
##################################################
## File Name: dashboardQuery.py
## File Type: Python Executable
## Package Dependancies: influxdb-client
## Description: Used to get data from InfluxDB for displaying on a dashboard
## Usage: Runs as main function, or using getInfluxData is calling from an external source
##################################################
## Version: 1.1.0
## Last Updated: 28/10/2024
##################################################


import csv
import pytz
from datetime import datetime, timedelta, date,timezone
from influxdb_client import InfluxDBClient
import os
import types

INFLUX_TOKEN = "pRoemIh7JfC7cn1HG2VyZcx1BSIguLN-gqhQyKl8775tpvDV4o9NNf1FuLMDKKvKaVEj1wDKiPouKsbvSS6s5Q=="
MACHINE_DATA_BUCKET = "Machines"
MACHINE_SUMMARY_DATA_BUCKET = "Machines_Summary"
DB_LINK = "http://10.101.23.23:8086"
DB_ORG = "ECMPG"

VALID_PROPERTIES = ["Min","Max", "Mean", "STD", "value"]
VALID_AGGREGATES = ["min","max","mean"]
VALID_NODES = ["A","C","T","1"]
VALID_TAGS = {"A": "Axis","C": "Phase","T": "Name"}
VALID_MEASUREMENTS = {"A": "Acceleration","C": "Current","T": "Temperature"}
INTERVAL_POINTS = [1,2,5,10,20,30,60]
MAX_POINTS = 3000

class _requestProperties:
	def __init__(self,firstColumn):
		self.columns = []
		self.data = []
		self.columns.append(firstColumn)
	
	def __repr__(self):
		from pprint import pformat
		return pformat(vars(self), indent=4)
	
def _connect_influxDB(timeout=30_000):
    ########## InfluxDB Initialisation ##########

	client = None
	health = None
	while(True):
		client = InfluxDBClient(url=DB_LINK, token=INFLUX_TOKEN, org=DB_ORG, timeout=timeout)
		health = client.health()

		if(health.status == "pass"):
			return client.query_api()
		
def _getTagName(nodeLetter):
	"""
    Returns the tag name corresponding to the given node letter.
    Args:
        nodeLetter (str): The letter representing the measurement type (e.g., "A", "C", "T").
    Returns:
        str: The full name of the tag.
    Raises:
        ValueError: If the node letter does not correspond to a known tag.
    """
	nodeLetter = nodeLetter.upper()
        
	if nodeLetter in VALID_TAGS:
		return VALID_TAGS[nodeLetter]
	else:
		raise ValueError("Tag name could not be determined from the node letter: {0}".format(nodeLetter))

def _getMeasurementType(nodeLetter):
	"""
    Returns the measurement type corresponding to the given node letter.
    Args:
        nodeLetter (str): The letter representing the measurement type (e.g., "A", "C", "T").
    Returns:
        str: The full name of the measurement type.
    Raises:
        ValueError: If the node letter does not correspond to a known measurement type.
    """
	nodeLetter = nodeLetter.upper()
    
	if nodeLetter in VALID_MEASUREMENTS:
		return VALID_MEASUREMENTS[nodeLetter]
	else:
		raise ValueError("Measurement type could not be determined from the node letter: {0}".format(nodeLetter))

def _validateProperty(property):
	"""
    Validates and transforms the property string based on specific rules.

    - If the input is "std", return it as "STD".
    - If the input is "value", return it as "value".
    - Otherwise, return the input with the first letter capitalized.

    Args:
        property (str): The property string to validate.

    Returns:
        str: The transformed property string.
    """
	property= property.lower()

	if(property == "std"): #Incase STD is requsted, revert back to all uppercase
		return "STD"
	elif(property == "value"): #Incase value is requsted, revert back to all lowercase
		return "value"
	else:
		return property.capitalize() #Capatalise the property field

def _buildQuery(nodes,start,end,property,summary,aggregate, aggregateProperty, tags):
	"""
	Constructs a query for retrieving data from an InfluxDB bucket.
	Args:
		nodes (str/list): The node identifier.
		start (datetime): The start time for the query range.
		end (datetime): The end time for the query range.
		property (str): The field to retrieve.
		summary (bool): Whether to use the summary data bucket.
		aggregate (bool): Whether to aggregate data over time.
		ggregate_property (str): The aggregation function to use.
		tags (list): List of tag values to filter by.
	Returns:
		str: The constructed InfluxDB query string.  
	Raises:
		ValueError: If a required value (e.g., tag name or measurement type) cannot be determined.
	"""
	#Get the 'first' node regardless of list/str
	if(isinstance(nodes,str)):
		firstNode = nodes
	else:
		firstNode = nodes[0]
	
	#Select the correct data bucket
	if not summary or firstNode[0] == "T": 
		bucket = MACHINE_DATA_BUCKET
	else:
		bucket = MACHINE_SUMMARY_DATA_BUCKET

	#Select the correct measurement and tag name, from node
	print(firstNode)
	tagName = _getTagName(firstNode[0])
	measurement = _getMeasurementType(firstNode[0])
	
	#Build a node filter string.
	nodeString = ""
	if(isinstance(nodes,str)):
		nodeString = f'|> filter(fn: (r) => r["Node"] == "{nodes}")'
	else:
		nodeStringStart = '|> filter(fn: (r) => '
		nodeStrings = []
		for node in nodes:
			nodeStrings.append(f'r["Node"] == "{node}"')
		nodeString = ' or '.join(nodeStrings)
		nodeString = nodeStringStart + nodeString + ")"
		
	#Build a tag keypair string from the tags provided.
	tagString = ""
	if(len(tags) >0):
		tagStringStart = '|> filter(fn: (r) => '
		tagStrings = []
		for tagValue in tags:
			tagStrings.append('r["{0}"] == "{1}"'.format(tagName,tagValue))
		tagString = ' or '.join(tagStrings)
		tagString = tagStringStart + tagString + ")"

	# Handle aggregation if requested
	aggregateText = ""
	if(aggregate):
		duration = (end-start).total_seconds()
		intervalUnits = "s"
		rough_interval = duration / MAX_POINTS
		if(rough_interval >60):
			rough_interval /= 60
			intervalUnits = "m"
		chosen_interval = 1
		for interval in INTERVAL_POINTS:
			if(rough_interval < interval):
				chosen_interval = interval
				break
		aggregateText = f"|> aggregateWindow(every: {chosen_interval}{intervalUnits}, fn: {aggregateProperty}, createEmpty: false)"

	startString =start.isoformat()
	endString = end.isoformat()
	

	query = (
        f'from(bucket: "{bucket}")'
        f'|> range(start: {startString}, stop: {endString})'
        f'|> filter(fn: (r) => r["_measurement"] == "{measurement}")'
        f'{nodeString}'
		f'{tagString}'
        f'|> filter(fn: (r) => r["_field"] == "{property}")'
        f'{aggregateText}'
        f'|> drop(columns: ["_start", "_stop", "_field"])'
        f'|> group(columns: ["_time"])'
    )
	return query

def _buildTagQuery(nodes,summary=True):
	"""
    Constructs a query to retrieve tag values based on the node and summary status.
    Args:
        nodes (str/list): The node identifier.
        summary (bool): Indicates whether to use the summary data bucket.
    Returns:
        str: The constructed tag query.
    Raises:
        ValueError: If the node is not valid or the tag name cannot be determined.
    """
	#Get the 'first' node regardless of list/str
	if(isinstance(nodes,str)):
		firstNode = nodes
		
	else:	
		firstNode = nodes[0]

	#Select the correct data bucket
	if not summary or firstNode[0] == "T":
		bucket = MACHINE_DATA_BUCKET
	else:
		bucket = MACHINE_SUMMARY_DATA_BUCKET

	#Select the correct measurement and tag name, from node
	tagName = _getTagName(firstNode[0])
	
	nodeString = ""
	if(isinstance(nodes,str)):
		nodeString = f'r.Node == "{firstNode}"'
	else:
		nodeStrings = []
		for node in nodes:
			nodeStrings.append(f'r.Node == "{node}"')
		nodeString = ' or '.join(nodeStrings)

	tag_query = (
        f'import "influxdata/influxdb/schema" '
        f'schema.tagValues('
        f'bucket: "{bucket}", '
        f'tag: "{tagName}", '
        f'predicate: (r) => {nodeString})'
    )
	return tag_query
	
def _runQuery(query):
	read_api = _connect_influxDB()
	getRecords =  read_api.query(org="ECMPG",query=query)
	return getRecords

def _runTagQuery(query):
	tags = []
	data = _runQuery(query)
	for record in data[0].records:
		tags.append(record.get_value())
	return tags

def _convertToCSV(nodes,tags,data):
	if(isinstance(nodes,str)):
		firstNode = nodes
	else:	
		firstNode = nodes[0]
	tagName = _getTagName(firstNode[0])
	timeHeader = "Time (GMT)"
	csvData = _requestProperties(timeHeader)
	for tag in tags:
		csvData.columns.append(tag)
	
	for table in data:
		singleRow = {}
		for record in table.records:
			if(timeHeader not in singleRow):
				singleRow[timeHeader] = record.get_time().strftime("%y-%m-%dT%H:%M:%S.%f")
			singleRow[record.values.get(tagName)] = record.get_value()
		csvData.data.append(singleRow)
	print("Found {0} results".format(len(csvData.data)))
	return csvData

def _convertToList(data):
	listData = []
	for table in data:
		for record in table.records:
			listData.append(record)
	print("Found {0} results".format(len(listData)))
	return listData
	
def _saveCSV(filePath,csvData):
	print("Saving to CSV")
	with open(filePath, 'w', newline='') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=csvData.columns)
		writer.writeheader()

		for row in csvData.data:
			# Create a dictionary with blank values for missing keys
			csv_row = {header: row.get(header, '') for header in csvData.columns}
			writer.writerow(csv_row)

	
def _validateNode(node,property):
	if(node[0] == "T" and property != "value"):
		raise Exception("The only temperature property is 'value'")
	if(node[0] != "T" and property == "value"):
		raise Exception("'value' is only used for temperature")
	if(node[0] not in VALID_NODES):
		raise Exception("{0} is not a valid node".format(node))
	

def _validateArguments(nodes,start,end,property,aggregate,aggregateProperty,summary):
	if(property not in VALID_PROPERTIES):
		raise Exception("This is not a valid property")
	
	if(isinstance(nodes,str)):
		_validateNode(nodes, property)

	elif(isinstance(nodes,list)):	
		firstNodeType = nodes[0][0]
		if(not(all(s[0] == firstNodeType for s in nodes if s))):
			raise Exception("You can not mix node types")
		for node in nodes:
			_validateNode(node, property)
		
		if(aggregate == False or summary == False):
			print("!!! Warning: Using multiple nodes without aggregation/summary will cause result in data desyncronisation, do you wish to continue? Y/N")
			getAnswer = input()
			if(not(getAnswer == "Y")):
				raise Exception("User cancelled")

	if(aggregate == True and aggregateProperty not in VALID_AGGREGATES):
		raise Exception("'aggregateProperty' is not valid")
	
	if(not isinstance(start, datetime)):
		raise Exception("Start time not a valid datetime")
	
	if(not isinstance(end, datetime)):
		raise Exception("End time not a valid datetime")
	
	if(end < start):
		raise Exception("End can not be before the start")



def getInfluxData(nodes,start,end,property,summary=True,aggregate=False, aggregateProperty="mean", tags=[],csv=False):
	'''
	Required:
	Nodes: Name of the node(s) to get data from: eg. C1-CIN, A2-GEI.
	Start: Datetime for the start date/time
	End: Datetime for the end date/time
	Property: The data property requested

	Optional:
	Summary: Whether to pull HF or LF data. Defaults to True
	Aggregate: Whether to add further downsampling to the data. Defaults to False
	Aggregate Property: When downsampling, what metric to use (Min/Max/Mean)
	Tags: A List of tag values to downselect data streams: eg. L1, X-axis.
	CSV: Should the data be returned as CSV. Defaults to False
	'''
	
	print("Validating Input Arguments...")

	#Node Parsing
	if(isinstance(nodes,str)):
		nodes = nodes.upper() #Convert this to uppercase
	elif(isinstance(nodes,list)):
		for node in nodes:
			node = node.upper() #Convert this to uppercase
	else:
		raise Exception("Incorrect type for node parameter")
	
	aggregateProperty = aggregateProperty.lower() #Convert this to lowercase
	property = _validateProperty(property) #Validate the property for capitalisation
	_validateArguments(nodes,start,end,property,aggregate,aggregateProperty,summary)

	
	#Convert Start/End times to have correct tiemzone
	uk_tz = pytz.timezone('Europe/London')
	start = start.astimezone(uk_tz)
	end = end.astimezone(uk_tz)
	
	print("Building Query...")
	query = _buildQuery(
		nodes=nodes,
		start=start,
		end=end,
		property=property,
		summary=summary,
		aggregate=aggregate,
		aggregateProperty=aggregateProperty,
		tags=tags
		)

	print("Getting Data...")
	data = _runQuery(query)
	if not data:
		raise Exception("No data was returned")

	if(csv):
		if not tags:
			print("Building Tag Query...")
			tagQuery = _buildTagQuery(nodes=nodes,summary=summary)
			print(tagQuery)
			tags = _runTagQuery(tagQuery)
		print("Converting to CSV...")
		csvData = _convertToCSV(nodes=nodes,tags=tags, data=data)
		return csvData
	else:
		print("Converting to List...")
		listData = _convertToList(data)
		
		return listData


def getCycleStarts(machine,variable,start,end):

	nodeName = machine.upper()[0:3]
	#Convert Start/End times to have correct tiemzone
	uk_tz = pytz.timezone('Europe/London')
	start = start.astimezone(uk_tz)
	end = end.astimezone(uk_tz)

	startString = start.isoformat()
	endString = end.isoformat()

	query = f'from(bucket: "Machines")\
    |> range(start: {startString}, stop: {endString})\
  |> filter(fn: (r) => r["_measurement"] == "BFC")\
  |> filter(fn: (r) => r["Node"] == "B1-{nodeName}")\
  |> filter(fn: (r) => r["_field"] == "{variable}")'
	
	print("Getting Data...")
	data = _runQuery(query)
	if not data:
		raise Exception("No data was returned")
	cleanData= _cleanCycleData(data)
	return cleanData

def _cleanCycleData(data):
	#print(data)
	cycles = []
	for table in data:
		rows = []
		for record in table.records:
				ts = record.get_time().strftime("%y-%m-%dT%H:%M:%S.%f")
				value = record.get_value()
				rows.append([ts,value])
	rowsR = rows
	for i in range(1,len(rowsR)-1):
		if(not(rowsR[i][1] ==0) and rowsR[i+1][1] - rowsR[i][1] == 1):
			cycleStart = datetime.strptime(rowsR[i][0],"%y-%m-%dT%H:%M:%S.%f")
			cycleEnd = datetime.strptime(rowsR[i+1][0],"%y-%m-%dT%H:%M:%S.%f")
			cycle = {"start": cycleStart, "end": cycleEnd,"cycleDuration":(cycleEnd-cycleStart).total_seconds(), "cycleID": rowsR[i][1]}
			cycles.append(cycle)
	return cycles


""" Example Queries

	cycleData = getCycleStarts("Cincinnati",
					   "R20",
					   start=datetime(year=2024,month=11,day=10,hour=0,minute=0,second=0),		
					   end=datetime(year=2024,month=11,day=10,hour=2,minute=59,second=0)
					   )

	data = getInfluxData(nodes="A1-ROB",
						start=cycle["start"],
						end=cycle["end"],
						tags=[],
						property="Max",
						summary=False,
						aggregate=False,
						aggregateProperty="mean",
						csv=False)
"""



