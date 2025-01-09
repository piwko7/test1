import datetime
import csv
import glob
import os
import time
import pyodbc
import snap7
from snap7.util import *
import sys
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
    

def getNewestCsvFileInfo(csvFilesPath):
    listOfFiles = glob.glob(csvFilesPath)
    if listOfFiles != []:
        latestFileName = max(listOfFiles, key=os.path.getmtime)
        modifiedTimeFloat = os.path.getmtime(latestFileName)
        return latestFileName, modifiedTimeFloat
    else:
        return '', 0.0

def readCsvFile(csvFilePath, rowNumber):
    with open(csvFilePath, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        rows = list(csv_reader)
    lastRow = rows[rowNumber]
    lastRowSql = rows[rowNumber]
    for i, item in enumerate(lastRow):
        if i == 0 or i == 1:
            lastRowSql[i] = str(lastRow[i])
        elif i > 1:
            if lastRow[i] == 'Ok':
                lastRowSql[i] = '1'
            elif lastRow[i] == 'Bad':
                lastRowSql[i] = '0'
            else:
                lastRowSql[i] = 'NULL'
    return lastRowSql[0], lastRowSql[1], lastRowSql[2], lastRowSql[3], lastRowSql[4], lastRowSql[5], lastRowSql[6], lastRowSql[7], lastRowSql[8]

def getCsvRowCount(csvFilePath):
    rowCount = 0;
    with open(csvFilePath, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        for row in csv_reader:
            rowCount += 1
        return rowCount

def sqlConnect(connectionString):
    while True:
        try:
            conn = pyodbc.connect(connectionString)
            return conn
        except pyodbc.Error as ex:
            logger.info(f"Connection to DB failed: {ex}")
            # print(f"Connection failed: {ex}")
            time.sleep(5)  # Wait for 5 seconds before retrying

def sqlExecuteInsert(conn, cursor, command):
    try:
        rowcount = cursor.execute(command).rowcount
        if rowcount != -1:
            return True
        else:
            return False
    except pyodbc.DatabaseError as ex:
        logger.info(f"Database insert error: {ex}")
        # print(f"Database error: {ex}")
        return None

def connectToPLC():
    global plcConnected, plc, plcIP, plcRack, plcSlot;
    if plc.get_connected():
        plc.disconnect()
    while not plcConnected:
        try:
            # print(f"Connecting to PLC ({plcIP})")
            plc.connect(plcIP, plcRack, plcSlot)
            # plc.connect('192.168.0.151', 0, 1)
            if plc.get_connected() and plc.get_cpu_state()!= 'S7CpuStatusUnknown':
                plcConnected = True
                logger.info('PLC connected succesfully')
                # print('PLC connected succesfully')
        except Exception as e:
            logger.info(f"PLC Connection failed: {e}")
            # print(f"PLC Connection failed: {e}")
            time.sleep(5)  # Wait for 5 seconds before retrying


#-----------------------------------------------------------------------------
# Path to logs folder
logFilesFolder = 'C:/Python/Hutchinson/CSV_database/Logs/'


formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                              "%Y-%m-%d %H:%M:%S")
logname = logFilesFolder + 'info.log'
handler = TimedRotatingFileHandler(logname, when="midnight", backupCount=30)
handler.suffix = "%Y%m%d"
handler.setFormatter(formatter)
logger = logging.getLogger('my_app')
if (logger.hasHandlers()):
    logger.handlers.clear()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info("Starting service")

lastModifiedTimeFloat = 0.0
lastCsvFileName = ''
lastCsvRowCount = 0

firstRead = True;


# Path to measurement csv files
csvFilesPath = '//PC_COGNEX_6830/Archive_Data/'
# csvFilesPath = '//LAPTOP-88LDEG7H/Archive_Data/'
sharedFolderPath = Path(csvFilesPath)
csvFilesPath = csvFilesPath + '*.csv' # * means all if need specific format then *.csv


# SQL Server
# Replace the placeholders with your actual server name, database name, username, and password
server = 'DESKTOP-VAH2QUR\SQLEXPRESS'
database = 'HS2303_DB'
username = 'SQL_USER'
password = 'hs2303'
driver =  '{ODBC Driver 17 for SQL Server}'

# Create the connection string
connectionString = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

sqlConn = sqlConnect(connectionString)
sqlConn.autocommit = True
sqlCursor = sqlConn.cursor()

#-----------------------------------------------------------------------------
plcConnected = False
plcWatchdogCounter = 0

# plc = snap7.client.Client()
# plcIP = '192.168.0.151'
# plcRack = 0
# plcSlot = 1
# connectToPLC()


#-----------------------------------------------------------------------------

try:
    
    
    
    while True:
        
        if sharedFolderPath.is_dir():   # check shared folder connection
            
            newestCsvFileName, newestFileModifiedTimeFloat = getNewestCsvFileInfo(csvFilesPath)
            
            if newestCsvFileName != '':
                newestCsvRowCount = getCsvRowCount(newestCsvFileName)
            
            if firstRead and newestFileModifiedTimeFloat != 0:
                lastModifiedTimeFloat = newestFileModifiedTimeFloat
                lastCsvFileName = newestCsvFileName
                lastCsvRowCount = getCsvRowCount(newestCsvFileName)
                firstRead = False
            
            if lastModifiedTimeFloat != newestFileModifiedTimeFloat and newestFileModifiedTimeFloat != 0:
                lastModifiedTimeFloat = newestFileModifiedTimeFloat
                if lastCsvFileName == newestCsvFileName:
                    while newestCsvRowCount - lastCsvRowCount > 0:
                        a1, a2, a3, a4, a5, a6, a7, a8, a9 = readCsvFile(newestCsvFileName, lastCsvRowCount - newestCsvRowCount)
                        sqlInsert = 'insert into Seals (Time, Datamatrix, State, WeldingL, EndcapL, CameraL, WeldingR, EndcapR, CameraR, FinalStatus)'
                        sqlValues = " values ('" + a1 + "', '" + a2 + "', " + a3 + ', ' + a4 + ', ' + a5 + ', ' + a6 + ', ' + a7 + ', ' + a8 + ', ' + a9 + ', NULL);'
                        sqlCommand = sqlInsert + sqlValues
                        sqlResult = sqlExecuteInsert(sqlConn, sqlCursor, sqlCommand)
                        # print("Insert into result:", sqlResult)
                        lastCsvRowCount += 1
                else:
                    lastCsvRowCount = 1
                    lastCsvFileName = newestCsvFileName
                    while newestCsvRowCount - lastCsvRowCount > 0:
                        a1, a2, a3, a4, a5, a6, a7, a8, a9 = readCsvFile(newestCsvFileName, lastCsvRowCount - newestCsvRowCount)
                        sqlInsert = 'insert into Seals (Time, Datamatrix, State, WeldingL, EndcapL, CameraL, WeldingR, EndcapR, CameraR, FinalStatus)'
                        sqlValues = " values ('" + a1 + "', '" + a2 + "', " + a3 + ', ' + a4 + ', ' + a5 + ', ' + a6 + ', ' + a7 + ', ' + a8 + ', ' + a9 + ', NULL);'
                        sqlCommand = sqlInsert + sqlValues
                        sqlResult = sqlExecuteInsert(sqlConn, sqlCursor, sqlCommand)
                        # print("Insert into result:", sqlResult)
                        lastCsvRowCount += 1
            
                
            # a1, a2, a3, a4, a5, a6, a7, a8, a9 = readCsvFile(newestCsvFileName)
            # sqlInsert = 'insert into Seals (Time, Datamatrix, State, WeldingL, EndcapL, CameraL, WeldingR, EndcapR, CameraR, FinalStatus)'
            # sqlValues = " values ('" + a1 + "', '" + a2 + "', " + a3 + ', ' + a4 + ', ' + a5 + ', ' + a6 + ', ' + a7 + ', ' + a8 + ', ' + a9 + ', NULL);'
            # sqlCommand = sqlInsert + sqlValues
            # print(sqlCommand)
            # sqlResult = sqlExecuteInsert(sqlConn, sqlCursor, sqlCommand)
            # print("Insert into result:", sqlResult)


            
#-----------------------------------------------------------------------------
        # if plc.get_connected() and plc.get_cpu_state()!= 'S7CpuStatusUnknown':  #PLC connection ok
        #     plcWatchdogByte = plc.db_read(1, 0, 1)
        #     plcWatchdogBit = plcWatchdogByte != b'\x00'
        #     if plcWatchdogBit:
        #         buffer1 = bytearray(1)
        #         snap7.util.set_bool(buffer1, 0, 0, False)
        #         plc.db_write(1, 0, buffer1)
        #         plcWatchdogCounter = 0
        #     else:
        #         plcWatchdogCounter += 1
        # elif not plc.get_connected() or plc.get_cpu_state() == 'S7CpuStatusUnknown':    #PLC disconnected
        #     plcConnected = False;
        #     print("blabla")
        #     connectToPLC()
                
                
        # if plcWatchdogCounter >= 5:
        #     plcWatchdogCounter = 0
#-----------------------------------------------------------------------------



        # print(plcWatchdogCounter)
        time.sleep(1)
    
    
    
except KeyboardInterrupt:
    # print('KeyboardInterrupted!')
    logger.info("KeyboardInterrupted!")
                
except Exception as err:
    # print(f"Exception: {err}")
    logger.info(f"Exception: {err}")
    raise

finally:
    logger.info("Closing service")
    # plc.disconnect()
    # print("PLC disconnected")
    sqlCursor.close()
    sqlConn.close()
    # print("SQL Server disconnected")
    logger.info("SQL Server disconnected")