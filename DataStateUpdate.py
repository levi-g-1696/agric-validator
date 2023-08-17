# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time
from datetime import datetime, timedelta
from tools import getIDbyTime,execSelectDataFor10mDB,getNFromStatusTableOf10mDB,execListOfUpdateReqFor10mDB,isStationOf10mDBCanceled
from tools import execSelectDataFor24hDB,execListOfUpdateReqFor24hDB,isStationOf24hDBCanceled,getNFromStatusTableOf24hDB
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
  #  print(year, month, day, hour, min, early)

    if early : return True
    else:return False
  #  print (year,month,day,hour,min)

    ################################################




###################################################
def areAllNotNullInDataFileFor10mDB(tab, id):
    result = []

    req = f"""SELECT      
      [monitors]      
  FROM [agr-dcontrol].[dbo].[stations] where tag= '{tab}'"""
    res=execSelectDataFor10mDB(req)
    res=res[0][0]

    enabledMonitorList = res.split(";")
    req= f"""SELECT top(1)  id FROM [{tab}] WHERE """

  #  print(enabledMonitorList)
    req = req +enabledMonitorList[0] + " IS NOT NULL "
    for j in range(1,len(enabledMonitorList)):
      req= req +" AND "+ enabledMonitorList[j]+" IS NOT NULL"
    req= req + f" AND id = {id}"
    res=execSelectDataFor10mDB(req)
    yesAllNotNull= len(res)>0
   # print ("res:::",res)
    return yesAllNotNull

##############################################
def areAllNotNullInDataFileFor24hDB(tab, id):
    result = []

    req = f"""SELECT      
      [monitors]      
  FROM [agr-penman].[dbo].[stations] where tag= '{tab}'"""
    res=execSelectDataFor24hDB(req)
    res=res[0][0]

    enabledMonitorList = res.split(";")
    req= f"""SELECT top(1)  id FROM [{tab}] WHERE """

  #  print(enabledMonitorList)
    req = req + "[" +enabledMonitorList[0] + "] IS NOT NULL "
    for j in range(1,len(enabledMonitorList)):
      req= req +" AND "+ "["+enabledMonitorList[j]+"] IS NOT NULL"
    req= req + f" AND id = {id}"
    res=execSelectDataFor24hDB(req)
    yesAllNotNull= len(res)>0
   # print ("res:::",res)
    return yesAllNotNull

###########################################################
def isNoDataFor10mDB(tab, id):
    result = []

    req = f"""SELECT      
      [monitors]      
  FROM [agr-dcontrol].[dbo].[stations] where tag= '{tab}'"""
    res=execSelectDataFor10mDB(req)
    res=res[0][0]

    enabledMonitorList = res.split(";")
    req= f"""SELECT top(1)  id FROM [{tab}] WHERE """

   # print(enabledMonitorList)
    req = req +enabledMonitorList[0] + " IS  NULL "
    for j in range(1,len(enabledMonitorList)):
      req= req +" AND "+ enabledMonitorList[j]+" IS  NULL"
    req= req + f" AND id = {id}"
    res=execSelectDataFor10mDB(req)
    yesThisIDisNODATA= len(res)>0
   # print ("res:::",res)
    return yesThisIDisNODATA

###########################################################
def isNoDataFor24hDB(tab, id):
    result = []

    req = f"""SELECT      
      [monitors]      
  FROM [agr-penman].[dbo].[stations] where tag= '{tab}'"""
    res=execSelectDataFor24hDB(req)
    res=res[0][0]

    enabledMonitorList = res.split(";")
    req= f"""SELECT top(1)  id FROM [{tab}] WHERE """

   # print(enabledMonitorList)
    req = req + "["+ enabledMonitorList[0] +"]" + " IS  NULL "
    for j in range(1,len(enabledMonitorList)):
      req= req +" AND "+ "["+ enabledMonitorList[j]+ "]"+ " IS  NULL"
    req= req + f" AND id = {id}"
    res=execSelectDataFor24hDB(req)
    yesThisIDisNODATA= len(res)>0
   # print ("res:::",res)
    return yesThisIDisNODATA
#############################################
#############################################

# Press the green button in the gutter to run the script.
def runDataStateUpdateFor10mTables(num):
        print_hi('  ##  datastate field update tool is runnig  ##')

        delta22m= timedelta(minutes=22)
        delta12h=timedelta(hours=12)

        reqList = []
        now= datetime.now()
        idNowMinus12h=getIDbyTime(now - delta12h)
        idNowPlus22= getIDbyTime(now + delta22m)
        idNowMinus36h = getIDbyTime(now - delta12h - delta12h - delta12h)
        idNowMinus60h = getIDbyTime(now - delta12h - delta12h - delta12h - delta12h - delta12h)

        stlst1 = getNFromStatusTableOf10mDB(num, f" FK< {idNowPlus22} AND datastate = 0 ", " FK desc")# all -10 0 last
        stlst2 = getNFromStatusTableOf10mDB(num, f" FK< {idNowPlus22} AND datastate = -10 ", " FK desc")# new only
        stlst3 = getNFromStatusTableOf10mDB(num, f" FK< {idNowPlus22} AND  datastate = 200 ", " FK desc")# hole of nodata last
        stlst4 = getNFromStatusTableOf10mDB(num, f" FK< {idNowMinus12h} AND  datastate = 200 ", " FK desc")
        stlst5 = getNFromStatusTableOf10mDB(num, f" FK< {idNowMinus36h} AND  datastate = 200 ", " FK desc")
        stlst6 = getNFromStatusTableOf10mDB(num, f" FK< {idNowMinus60h} AND  datastate = 200 ", " FK desc")
        print ("hole complete:",stlst3,stlst4)
        stlst= stlst1+stlst2 + stlst3+ stlst4+ stlst5 +stlst6
        idList=[]
        optimizedList=[]
        for dataSession in stlst:
            id = dataSession[0]
            if not id in idList:
                idList.append(id)
                optimizedList.append(dataSession)
        print("stlst:",len(stlst),"   optimized:", len(optimizedList))
        for dataSession in optimizedList:
            id = dataSession[0]
            fk = dataSession[2]
            tab = dataSession[1]
            print (f"runDataStateUpdate says: processing for {tab} id= {fk}")
            stationExists = not isStationOf10mDBCanceled(tab)

            if stationExists:
                if isEarlyForData(fk):
                    dataState = 0
                elif areAllNotNullInDataFileFor10mDB(tab, fk):
                    dataState = 1
                elif isNoDataFor10mDB(tab, fk):
                    dataState = 200
                else:
                    dataState = 100
            else: dataState =-1
            req = f"UPDATE [dbo].[VLDstat]  SET  [DataState] = {dataState} WHERE id = {id}"
            reqList.append(req)
        execListOfUpdateReqFor10mDB(reqList)
        print("update completed", len(reqList))
        print ('  ##  datastate field update tool is runnig  ##')
 #       time.sleep(10)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
  #for j in range(10):
  #    statLst= getNFromStatusTable()
#runDataStateUpdate()
###################################################
def runDataStateUpdateFor24hTables(num):
    print_hi('  ##  datastate field update tool is runnig  ##')

    delta7d = timedelta(days=7)
    delta12h = timedelta(hours=12)

    reqList = []
    now = datetime.now()

    nowModified= (now - timedelta(days=1)).replace(hour=23,minute=50)
    nowPlus1day = nowModified + timedelta(days=1)
    idNowPlus1day= getIDbyTime(nowPlus1day)
    idNowMinus7d = getIDbyTime(nowModified - delta7d)


    stlst1 = getNFromStatusTableOf24hDB(num, f" FK< {idNowPlus1day} AND datastate = 0 ", " FK desc")  # all -10 0 last
    stlst2 = getNFromStatusTableOf24hDB(num, f" FK< {idNowPlus1day} AND datastate = -10 ", " FK desc")  # new only
    stlst3 = getNFromStatusTableOf24hDB(num, f" FK< {idNowPlus1day} AND  datastate = 200 ", " FK desc")  # hole of nodata last


    stlst = stlst1 + stlst2 + stlst3
    idList = []
    optimizedList = []
    for dataSession in stlst:
        id = dataSession[0]
        if not id in idList:
            idList.append(id)
            optimizedList.append(dataSession)
    print("stlst:", len(stlst), "   optimized:", len(optimizedList))
    for dataSession in optimizedList:
        id = dataSession[0]
        fk = dataSession[2]
        tab = dataSession[1]

        stationExists = not isStationOf24hDBCanceled(tab)

        if stationExists:
            if isEarlyForData(fk):
                dataState = 0
            elif areAllNotNullInDataFileFor24hDB(tab, fk):
                dataState = 1
            elif isNoDataFor24hDB(tab, fk):
                dataState = 200
            else:
                dataState = 100
        else:
            dataState = -1
        req = f"UPDATE [dbo].[VLDstat]  SET  [DataState] = {dataState} WHERE id = {id}"
        print(f"runDataStateUpdate says: processing for {tab} id= {fk}  dataState={dataState}")
        reqList.append(req)
    execListOfUpdateReqFor24hDB(reqList)
    print("update completed", len(reqList))
    print('  ##  datastate field update tool is runnig  ##')