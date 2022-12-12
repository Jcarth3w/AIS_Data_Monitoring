import json
import configparser
import mysql.connector
from mysql.connector import errorcode
import unittest 
import datetime
import csv



def main():
	dl = dataLoader()
	dl.load_vessel_data()
	dl.load_map_data()
	dl.load_port_data()
	dl.load_ais_messages()
	dl.load_static_data()
	dl.load_position_reports()
	

class dataLoader:

	def load_vessel_data(self):
			with open('VESSEL.csv', 'r') as object:
				reader = csv.reader(object)
				vesselList = list((reader))
				for i in range(1, len(vesselList)):
					vesselList[i][0] = int(vesselList[i][0])
					for j in range(len(vesselList[i])):
						if vesselList[i][j] == '\\N':
							vesselList[i][j] = None
						
			cnx = Mysql_connector.getConnection()
			cursor = cnx.cursor(prepared=True)
			print("\nLoading VESSEL data into database, please wait.")

			cursor.executemany("""INSERT INTO VESSEL VALUES
			(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
			vesselList[1:])

			cnx.commit()
			cnx.close()
		
	def load_map_data(self):
			with open('MAP_VIEW.csv', 'r') as object:
				reader = csv.reader(object)
				mapTileList = list((reader))
				for i in range(1, len(mapTileList)):
					for j in range(len(mapTileList[i])):
						if mapTileList[i][j] == '\\N':
							mapTileList[i][j] = None

			cnx = Mysql_connector.getConnection()
			cursor = cnx.cursor(prepared=True)
			print("\nLoading MAP data into database, please wait.")

			cursor.executemany("""INSERT INTO MAP_VIEW VALUES
			(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
			mapTileList[1:])

			cnx.commit()
			cnx.close()
		
	def load_port_data(self):
			with open('PORT.csv', 'r') as object:
				reader = csv.reader(object, delimiter = ';')
				portDataList = list((reader))
				for i in range(1, len(portDataList)):
					for j in range(len(portDataList[i])):
						if portDataList[i][j] == '\\N':
							portDataList[i][j] = None
							
			cnx = Mysql_connector.getConnection()
			cursor = cnx.cursor(prepared=True)
			print("\nLoading Port data into database, please wait.")

			cursor.executemany("""INSERT INTO PORT VALUES
			(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
			portDataList[1:])
			

			cnx.commit()
			cnx.close()

	def load_ais_messages(self):
			with open('AIS_MESSAGE.csv', 'r') as object:
				reader = csv.reader(object, delimiter =';')
				aisDataList = list((reader))
				insertedList = []
				for i in range(1, 5000):
					aisDataList[i].insert(4, None)
					tempList = []

					for j in range(len(aisDataList[i])):
						if aisDataList[i][j] =='\\N':
							aisDataList[i][j] = None
						tempList.append(aisDataList[i][j])
					insertedList.append(tempList)
			cnx = Mysql_connector.getConnection()
			cursor = cnx.cursor(prepared=True)
			print("\nLoading AIS Message data into database, please wait.")

			cursor.executemany("""INSERT INTO AIS_MESSAGE VALUES (
			%s, %s, %s, %s, %s, %s)""", insertedList[1:])

			cnx.commit()
			cnx.close()


	def load_static_data(self):
			with open('STATIC_DATA.csv', 'r') as object:
				reader = csv.reader(object, delimiter =';')
				staticDataList = list((reader))
				insertedList = []
				for i in range(1, 50):
					staticDataList[i].pop(3)
					staticDataList[i].pop(4)
					staticDataList[i].pop(3)
					staticDataList[i].pop(3)
					staticDataList[i].pop(3)
					staticDataList[i].pop(3)
					staticDataList[i].pop(3)
					staticDataList[i].pop(3)
				
					tempList = []
					for j in range(len(staticDataList[i])):
						if staticDataList[i][j] =='\\N':
							staticDataList[i][j] = None
						tempList.append(staticDataList[i][j])
					insertedList.append(tempList)

			#print(insertedList)
			cnx = Mysql_connector.getConnection()
			cursor = cnx.cursor(prepared=True)
			print("\nLoading Static Data into database, please wait.")
	
			cursor.executemany("""INSERT INTO STATIC_DATA VALUES
			(%s, %s, %s, %s)""", insertedList[1:])

			cnx.commit()


	def load_position_reports(self):
			with open('POSITION_REPORT.csv', 'r') as object:
				reader = csv.reader(object, delimiter =';')
				posReportDataList = list((reader))
				insertedList = []
				for i in range(1, 4782):
					posReportDataList[i].pop(8)
					tempList = []
					for j in range(len(posReportDataList[i])):
						if posReportDataList[i][j] =='\\N':
							posReportDataList[i][j] = None
						tempList.append(posReportDataList[i][j])
					insertedList.append(tempList)

			print(insertedList)
			cnx = Mysql_connector.getConnection()
			cursor = cnx.cursor(prepared=True)
			print("\nLoading Position Report data into database, please wait.")
	
			cursor.executemany("""INSERT INTO POSITION_REPORT VALUES
			(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", insertedList[1:])

			cnx.commit()

			
			
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
			user = config['mysqlDB']['user'],
			password = config['mysqlDB']['password'],
			db = config['mysqlDB']['db'],
			port = config['mysqlDB']['port'])

		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("Something is wrong with your user name or password")
			elif err.erno == errorcode.ER_BAD_DB_ERROR:
				print("Database does not exist")
			else:
				print(err)



main()