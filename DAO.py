import json
import configparser
import mysql.connector
from mysql.connector import errorcode
import unittest 


class MessageDAO:
	
	def __init__(self, test_mode=False):
		self.test_mode=test_mode
		self.Mysql_connector = Mysql_connector()
		
	#function that inserts a batch of AIS Messages
	#can be either position_report or static_data
	def insert_messages(self, batch):
		try:
			
			array = json.loads(batch)
			
		except Exception as e:
			print(e)
			return -1
			
		if self.test_mode:
			return len(array)
		else:
			cnx = Mysql_connector.getConnection()
			
			statements = self.get_insert_statement(array)
			
			staticDataStatement = statements[0]
			positionReportStatement = statements[1]
			cursor = cnx.cursor(prepared=True)
			
			#cursor.execute(staticDataStatement)
			cursor.execute(positionReportStatement)
			
			cursor.executemany("INSERT INTO AIS_MESSAGE VALUES(%s, %s, %s)")
						
			cnx.commit()
			
			return cursor.rowcount
	
	
	#helper function to create mysql statements
	#accepts a loaded json array
	def get_insert_statement(self, array):

		#create a list of for each type of table entry
		ais_message_list = []
		static_data_list = []
		position_report_list = []
		
		for message in array:
			#extract all the AIS_MESSAGE  and add them to the ais_message list
			timeStamp = message['Timestamp']
			shipClass = message['Class']
			mmsi = message['MMSI']
			msgType = message['MessageType']
	
			ais_message_list.append(list(timeStamp, shipClass, mmsi, msgType))
			
			#if the message type is static data, extract all of the fields for static data
			if msgType.equals("static_data"):
				imo = message['IMO']
				callsign = message['Callsign']
				destination_id = message['Destination']
				
				static_data_list.append(list(imo, callsign, destination_id))
				
			#if the message type is position report extract all the fields and add them to associated list
			elif msgType.equals("position_report"):
				position = message['Position']
				coordinates = position['Coordinates']
				longitude = coordinates[0]
				lattitude = coordinates[1]
				
				#how do we get these values?
				lastStaticData_id = None
				mapview1 = None
				mapview2 = None
				mapview3 = None
				
				position_report_list.append(list(longitude, lattitude))
				

				
			#beginning of sql statement for each table
			ais_statement = """INSERT INTO AIS_MESSAGE VALUES( """
			positionReportStatement = """INSERT INTO POSITION_REPORT VALUES( """
			staticDataStatement = """INSERT INTO TABLE STATIC_DATA VALUES ("""
			
			#add static data to complete statement
			if "static_data" in rowsArray
				for value in range(4): 
					ais_statement = ais_statement + value + ','
					
				for value in range(4,len(rowsArray)-1)
						staticDataStatement = staticDataStatement + value + ', '
						
			#add position report data to complete statement
			if "position_report" in rows:
				for value in range(4): 
					ais_statement = ais_statement + value + ','
				positionReportStatement = positionReportStatement + value + ', '
				
			ais_statement = ais_statement[:-2] + ')'	
			positionReportStatement = positionReportStatement[:-2] + ')'
			staticDataStatement = staticDataStatement[:-2] + ')'	
		
	
		return ais_statement, staticDataStatement, positionReportStatement
			
		
		
	def delete_msg_timestamp (self, currentTime, timeStamp):
		pass
		
	def read_most_recent_positions(self):
		pass
		
	def read_most_recent_positions_MMSI(self, MMSI):
		pass
		
	def read_permanent_info(self, MMSI):
		if self.test_mode:
			try:
				return int(MMSI)
			except:
				return -1
		
		try:
			cnx = Mysql_connector.getConnection()
			cnx = cnx.cursor(prepared=True)
			cursor.execute("""SELECT * FROM 'STATIC_DATA' WHERE""MMSI"=%s""",MMSI)
			cnx.commit()
		except:
			return -1

    	

#connection class
class Mysql_connector():
	
	def __init__(self):
		pass
		
	#get connection currently works as is
	#setting aside the config reading functionality for later
	def getConnection():
		config = configparser.ConfigParser()
		config.read('config.ini')
		try: 
			return mysql.connector.connect(host = '127.0.0.1', 
			user = 'jack', 
			password = 'drum',
			db = 'Datastore',
			port = 3306)
		
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("Something is wrong with your user name or password")
			elif err.erno == errorcode.ER_BAD_DB_ERROR:
				print("Database does not exist")
			else:
				print(err)
			
			
	def execute (self, query):
		cursor = self.cnx.cursor()
		cursor.execute(query)
		result = cursor.fetchall()
		cursor.close()
		
		return result
	
	
	
	
	
	
##################TESTS#####################


class DAOTest (unittest.TestCase):

	batch = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111923,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"BALTIC2 WINDFARM SW\",\"VesselType\":\"Undefined\",\"Length\":8,\"Breadth\":12,\"A\":4,\"B\":4,\"C\":4,\"D\":8},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257385000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.219403,13.127725]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":12.3,\"CoG\":96.5,\"Heading\":101},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

	#tests correct insertion type
	def test_insert_messages (self):
		dao = MessageDAO(True)
		
		inserted_messages = dao.insert_messages(self.batch)
		self.assertTrue(type(inserted_messages) is int and inserted_messages > 0)
		
	#tests correct insertion amount
	def test_insert_messages2 (self):
		dao = MessageDAO(True)
		array = json.loads( self.batch )
		inserted_count = dao.insert_messages( array )
		self.assertEqual( inserted_count, -1)
		
	#tests correct statment is created for sql execution
	def test_get_AIS_insert_statement (self):
		dao = MessageDAO()
		realQuery = "INSERT INTO TABLE AIS_MESSAGE VALUES (2020-11-18T00:00:00.000Z, AtoN, 992111840, static_data)
		array = json.loads(self.posRep)
		statements = dao.get_insert_statement(array)
		self.assertEqual(realQuery, statements[1])
		
	def test_delete_msg_timeStamp (self):
		dao = MessageDAO(True)
		deleted = dao.delete(msg)
		
	def test_read_most_recent_positions(self):
		dao = MessageDao(True)
		result = dao.read_most_recent_positions()
		self.assertEqual()
		
		
	def test_read_permanent_info(self):
		dao = MessageDAO(True)
		result = dao.read_permanent_info(3048580000)
		self.assertTrue(result>0)
		
	
########Integration Tests###########
		
	def test_insert_messages3 (self):
		dao = MessageDAO()
		statements = dao.insert_messages(self.batch)
		self.assertEqual(statements, 7)
	
	
unittest.main()


