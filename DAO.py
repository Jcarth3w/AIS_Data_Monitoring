import json
import configparser
import mysql.connector
from mysql.connector import errorcode
import unittest 

id_counter = 1

class MessageDAO:
	
	def __init__(self, test_mode=False):
		self.test_mode=test_mode
		self.Mysql_connector = Mysql_connector()
		
		
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
			
			cnx.commit()
			
			return cursor.rowcount
	
	def get_insert_statement(self, array):
		rowsArray = []
		for message in array:
			timeStamp = message['Timestamp']
			shipClass = message['Class']
			mmsi = message['MMSI']
			msgType = message['MessageType']
	
			if msgType.equals("static_data"):
				imo = message['IMO']
				callsign = message['Callsign']
				destination_id = message['Destination']

			elif msgType.equals("position_report"):
				try:
					position = message['Position']
					coordinates = position['Coordinates']
					longitude = coordinates[0]
					lattitude = coordinates[1]
					lastStatic_id = 
			
			
			singleRow = "ROW("
			singleRow = singleRow + str(id_counter) + ','
			for v in element:
				singleRow = singleRow + str(element[v]) + ', '
			singleRow = singleRow[:-2] + ')'
			rowsArray.append(singleRow)
			id_counter++			

		
			ais_statement = """INSERT INTO AIS_MESSAGE VALUES( """
			positionReportStatement = """INSERT INTO POSITION_REPORT VALUES( """
			staticDataStatement = """INSERT INTO TABLE STATIC_DATA VALUES ("""
			
			if "static_data" in rowsArray
				for value in range(4): 
					ais_statement = ais_statement + value + ','
					
				for value in range(4,len(rowsArray)-1)
						staticDataStatement = staticDataStatement + value + ', '
						

			if "position_report" in rows:
				for value in range(4): 
					ais_statement = ais_statement + value + ','
				positionReportStatement = positionReportStatement + value + ', '
				
					
			positionReportStatement = positionReportStatement[:-2] + ''
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
			print("""SELECT * FROM 'STATIC_DATA' WHERE""MMSI"=%s""",MMSI)
		except:
			return -1

    	


class Mysql_connector():
	
	def __init__(self):
		pass
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

	batch = """[ {\"AISMEssage_Id\":\"1\",\"NavigationalStatus\":\"Under way using engine\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111923,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"BALTIC2 WINDFARM SW\",\"VesselType\":\"Undefined\",\"Length\":8,\"Breadth\":12,\"A\":4,\"B\":4,\"C\":4,\"D\":8},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257385000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.219403,13.127725]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":12.3,\"CoG\":96.5,\"Heading\":101},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

	posRep = json.dumps((1,'Under way using engine',11.107765,54.947323,'NULL',0.0,293.1,'NULL','NULL',1,5428,54281))
	
	def test_insert_messages (self):
		dao = MessageDAO(True)
		
		inserted_messages = dao.insert_messages(self.batch)
		self.assertTrue(type(inserted_messages) is int and inserted_messages > 0)
		
		
	def test_insert_messages2 (self):
		dao = MessageDAO(True)
		array = json.loads( self.batch )
		inserted_count = dao.insert_messages( array )
		self.assertEqual( inserted_count, -1)
		
	
	def test_get_insert_statement (self):
		dao = MessageDAO()
		realQuery = "INSERT INTO TABLE POSITION_REPORT VALUES (ROW(2020-11-18T00:00:00.000Z, Class A, 304858000, position_report, {'type': 'Point', 'coordinates': [55.218332, 13.371672]}, Under way using engine, 10.8, 94.3, 97))"
		print(self.posRep)
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
		
#########MySQL Connection Tests############

		
	#def test_connection (self):
		#con = Mysql_connector()
		#cnx = con.getConnection
		#self.assertTrue(cnx)
		
	#def test_execute():
		#testQuery = "SELECT IMO FROM VESSEL;"
		#result = self.sql.exeute(testQuery)
		#self.assertTrue(len(result) > 0)
	
########Integration Tests###########
		
	def test_insert_messages3 (self):
		dao = MessageDAO()
		statements = dao.insert_messages(self.batch)
		self.assertEqual(statements, 7)
	
	
unittest.main()


