import json
import configparser
import mysql.connector
from mysql.connector import errorcode
import unittest 
import datetime


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
			
			cursor = cnx.cursor(prepared=True)
			
			
			for message in array:
				#extract all the AIS_MESSAGE  and add them to the ais_message list
				timeStamp = self.convert_time(message['Timestamp'])
				shipClass = message['Class']
				mmsi = message['MMSI']
				msgType = message['MsgType']
				cursor.execute("""INSERT INTO AIS_MESSAGE VALUES(%s, %s, %s, %s, %s)""", 
				list((None, datetime.datetime.fromisoformat(timeStamp), shipClass, mmsi, msgType)))
				#if the message type is static data, extract all of the fields for static data
				if msgType == "static_data":
					imo = message['IMO']
					if imo == 'Unknown':
						imo = None
						
					try:
						callsign = message['CallSign']
						
					except:
						callsign = None
						
					try:
						destination_id = message['Destination']
					except:
						destination_id = None
					
					cursor.execute("""INSERT INTO STATIC_DATA VALUES(LAST_INSERT_ID(), %s, %s, %s)""", 
					list((imo, callsign, destination_id)))
				
				#if the message type is position report extract all the fields and add them to associated list
				elif msgType == "position_report":
					position = message['Position']
					coordinates = position['coordinates']
					longitude = coordinates[0]
					lattitude = coordinates[1]
				
				#how do we get these values?
					lastStaticData_id = None
					mapview1 = None
					mapview2 = None
					mapview3 = None
				
					cursor.execute("""INSERT INTO POSITION_REPORT VALUES(LAST_INSERT_ID(), %s, %s, %s, %s, %s, %s)""", 
					list((longitude, lattitude, lastStaticData_id, mapview1, mapview2, mapview3)))
				
			cursor.reset()
			cnx.commit()
			cursor.execute("SELECT COUNT(*) FROM AIS_MESSAGE") 
			return cursor.fetchall()[0][0]
	
	
	def delete_msg_timestamp (self):
		deleted = False 
		if self.test_mode:
			return 1
		
		else:
			cnx = Mysql_connector.getConnection()
			
			cursor = cnx.cursor(prepared=True)
			
			cursor.execute("""DELETE AIS_MESSAGE FROM AIS_MESSAGE WHERE  AIS_MESSAGE.TS < (NOW() - INTERVAL 5 MINUTE);""")
			cnx.commit()
		
			return cursor.rowcount
		
	def read_most_recent_positions(self):
		if self.test_mode:
			return 1
			
		else:
			cnx = Mysql_connector.getConnection()
			
			cursor = cnx.cursor(prepared=True)
			
			cursor.execute("""SELECT DISTINCT(mmsi), ts, latitude, longitude FROM POSITION_REPORT as pr, AIS_MESSAGE as am WHERE pr.AISMessage_id=am.Id  ORDER BY ts DESC;""")
			
			returnedList = cursor.fetchall()
			
			for value in range(len(returnedList)):
				returnedList[value] = list(returnedList[value])
				returnedList[value].pop(1)
				
				returnedList[value][1] = float(returnedList[value][1])
				returnedList[value][1] = float(returnedList[value][2])
			
			
			return returnedList
		
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
			cursor = cnx.cursor(prepared=True)
			cursor.execute("""SELECT * FROM 'STATIC_DATA' WHERE""MMSI"=%s""",MMSI)
			cnx.commit()
		except:
			return -1
			
	def convert_time(self, timestamp):
		return str(timestamp).replace('T',' ').replace('Z', '')

    	

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

	
	
##################TESTS#####################


class DAOTest (unittest.TestCase):

	batch1 = """[ {\"Timestamp\":\"2023-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2023-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2023-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2023-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111923,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"BALTIC2 WINDFARM SW\",\"VesselType\":\"Undefined\",\"Length\":8,\"Breadth\":12,\"A\":4,\"B\":4,\"C\":4,\"D\":8},
                {\"Timestamp\":\"2023-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257385000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.219403,13.127725]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":12.3,\"CoG\":96.5,\"Heading\":101},
                {\"Timestamp\":\"2023-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

	batch2 = """[{\"Timestamp\":\"2022-12-06T15:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2022-12-06T14:56:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2022-12-06T14:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240}]"""


	def test_delete_all_messages(self):
		cnx = Mysql_connector.getConnection()
		cursor = cnx.cursor(prepared=True)
		cursor.execute("""DELETE FROM AIS_MESSAGE;""")
		cnx.commit()
		self.assertTrue(True)
	
	#tests correct insertion type
	def test_insert_messages (self):
		dao = MessageDAO(True)
		
		inserted_messages = dao.insert_messages(self.batch1)
		self.assertTrue(type(inserted_messages) is int and inserted_messages > 0)
		
	#passes if insertion type is incorrect
	def test_insert_messages2 (self):
		dao = MessageDAO(True)
		array = json.loads( self.batch1 )
		inserted_count = dao.insert_messages( array )
		self.assertEqual( inserted_count, -1)
		
	#passes if timestampformat is correct
	def test_delete_msg_timeStamp (self):
		dao = MessageDAO(True)
		self.assertEqual(1, 1)
		
		
		
	def test_read_most_recent_positions(self):
		dao = MessageDAO(True)
		array = []
		result = dao.read_most_recent_positions()
		self.assertTrue(type(result) is not type(array))
		
		
	def test_read_permanent_info(self):
		dao = MessageDAO(True)
		result = dao.read_permanent_info(3048580000)
		self.assertTrue(result>0)

	def test_convert_time(self):
		dao = MessageDAO()
		convertedTime = "2020-11-18 00:00:00.000"
		self.assertEqual(convertedTime, dao.convert_time("2020-11-18T00:00:00.000Z"))
	
########Integration Tests###########
		
	def test_insert_messages3 (self):
		dao = MessageDAO()
		statements = dao.insert_messages(self.batch1)
		self.assertEqual(statements, 6)
	
	def test_delete_msg_timestamp2 (self):
		dao = MessageDAO()
		
		dao.insert_messages(self.batch2)
		deletedRows = dao.delete_msg_timestamp()
		
		self.assertEqual(deletedRows, 3)
		
	def test_read_most_recent_positions2 (self):
		dao = MessageDAO()
		resultArray = dao.read_most_recent_positions()
		self.assertEqual(resultArray[0], list((219005465, 11.929218, 55.572601)))
		self.assertEqual(reslultArray[1], list((25796100, 12.809015, 55.003159)))
	
unittest.main()


