# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time
from datetime import datetime

import pyodbc as pyodbc


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    ######################################################
def isEarlyForData(fk):
    idFull=fk*10
    min= idFull%100
    hour= int((idFull-min)/100)%100
    day= int(idFull/10000)%100
    month= int((idFull/10000)/100)%100
    year= int(((idFull/10000)/100)/100)+2000
    dt = datetime(year,month,day,hour,min)
    now= datetime.now()
    early = now < dt
    print(year, month, day, hour, min, early)

    if early : return True
    else:return False
    print (year,month,day,hour,min)

    ################################################
def getNFromStatusTable(num,condition,order):
    result = []

    req= f"""SELECT TOP ({num}) [id]
      ,[TableName]
      ,[FK]
      ,[DataState]
      ,[FileTime]
      ,[VldState]
      ,[SendState]
      ,[R]
  FROM [agr-dcontrol].[dbo].[VLDstat] where {condition} order by {order}"""
    result= execSelectData(req)
    return result

    #cursor.commit()

    ################
def execSelectData(req):
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=DESKTOP-3BJPAFM\\SQLEXPRESS;"
                          "Database=agr-dcontrol;"
                          "Trusted_Connection=yes;")
    cursor = cnxn.cursor()

    try:
        print("execSelelect says:exequte sql query:\n", req)
        cursor.execute(req)
        rows = cursor.fetchall()
    except pyodbc.OperationalError as msg:

        print("Command skipped: ", msg)
    return rows

def execListOfUpdateReq(reqList):




        cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                              "Server=DESKTOP-3BJPAFM\\SQLEXPRESS;"
                              "Database=agr-dcontrol;"
                              "Trusted_Connection=yes;")
        cursor = cnxn.cursor()
        for req in reqList:
            # This will skip and report errors
            # For example, if the tables do not yet exist, this will skip over
            # the DROP TABLE commands
            try:
                print("exequte sql query:\n", req)
                cursor.execute(req)
            except pyodbc.OperationalError as msg:
                print("Command skipped: ", msg)
        cursor.commit()
###################################################
def areAllNotNullInDataFile(tab,id):
    result = []

    req = f"""SELECT      
      [monitors]      
  FROM [agr-dcontrol].[dbo].[stations] where tag= '{tab}'"""
    res=execSelectData(req)
    res=res[0][0]

    enabledMonitorList = res.split(";")
    req= f"""SELECT top(1)  id FROM [{tab}] WHERE """

    print(enabledMonitorList)
    req = req +enabledMonitorList[0] + " IS NOT NULL "
    for j in range(1,len(enabledMonitorList)):
      req= req +" AND "+ enabledMonitorList[j]+" IS NOT NULL"
    req= req + f" AND id = {id}"
    res=execSelectData(req)
    yesAllNotNull= len(res)>0
   # print ("res:::",res)
    return yesAllNotNull

##############################################
def isNoData(tab,id):
    result = []

    req = f"""SELECT      
      [monitors]      
  FROM [agr-dcontrol].[dbo].[stations] where tag= '{tab}'"""
    res=execSelectData(req)
    res=res[0][0]

    enabledMonitorList = res.split(";")
    req= f"""SELECT top(1)  id FROM [{tab}] WHERE """

    print(enabledMonitorList)
    req = req +enabledMonitorList[0] + " IS  NULL "
    for j in range(1,len(enabledMonitorList)):
      req= req +" AND "+ enabledMonitorList[j]+" IS  NULL"
    req= req + f" AND id = {id}"
    res=execSelectData(req)
    yesThisIDisNODATA= len(res)>0
   # print ("res:::",res)
    return yesThisIDisNODATA
#############################################
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')


    for k in  range (62000):
        reqList = []
        stlst = getNFromStatusTable(1000,"datastate=-10"," FK desc")
        stlst2= getNFromStatusTable(1000,"datastate=0" , " FK asc")
        stlst = stlst2
        for dataSession in stlst:
            id = dataSession[0]
            fk = dataSession[2]
            tab = dataSession[1]
            stationExists = True
            if stationExists:
                if isEarlyForData(fk):
                    dataState = 0
                elif areAllNotNullInDataFile(tab, fk):
                    dataState = 1
                elif isNoData(tab, fk):
                    dataState = 200
                else:
                    dataState = 100
            req = f"UPDATE [dbo].[VLDstat]  SET  [DataState] = {dataState} WHERE id = {id}"
            reqList.append(req)
        execListOfUpdateReq(reqList)
        print("update completed", len(reqList))
        time.sleep(10)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
  #for j in range(10):
  #    statLst= getNFromStatusTable()

