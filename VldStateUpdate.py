import math
import time
from collections import namedtuple
from datetime import datetime, timedelta
from vldCodeMetods import resetVldResCodeDict,appendVldCode, getVldResCodeDict,updateVldTabOf10mDB,updateStatusTabOf10mDB
from chainValidator import vldClient,getValueFrom10mDB
from tools import isStationOf24hDBCanceled
from tools import isStationOf10mDBCanceled,getMonListFromStationsTableOf10mDB,getMonListFromStationsTableOf24hDB,execListOfUpdateReqFor10mDB
from tools import  getIDbyTime,execSelectDataFor10mDB,getNFromStatusTableOf10mDB
import pyodbc as pyodbc
from vldCodeMetods import vldCodeDict

nodataCode = 255
reqArgs = namedtuple("reqArgs", "table mon id")
#delta22m = timedelta(minutes=22)
#now = datetime.now()
#print("from:", now)
#idNowPlus22 = getIDbyTime(now + delta22m)

def doVldProcessForDataSessionsOf10mDB(list):
        stlst=list
        reqList = []
        print("stlst:" , stlst)
        for dataSession in stlst:
            print (dataSession)

        #    print (datetime.now())
          #  time.sleep(3)
            id = dataSession[0]
            fk = dataSession[2]
            tab = dataSession[1]
            stationExists = not isStationOf10mDBCanceled(tab)
       #     print  (dataSession)
            monList=  getMonListFromStationsTableOf10mDB(tab)

            argsForVldClient=[]

            for mon in monList:
                   val= getValueFrom10mDB(tab, mon, fk)

                   if val is None:
                       appendVldCode(tab,mon,id,nodataCode)
                       resCodeDict = getVldResCodeDict()
                   else:
                       argsForVldClient.append(reqArgs(tab,mon,fk))
                       vldClient(argsForVldClient)
                       resCodeDict = getVldResCodeDict()
            updateVldTabOf10mDB(resCodeDict)
            updateStatusTabOf10mDB(resCodeDict)


         #   reqList.append(getUpdateVldTabReq(resCodeDict))
         #   reqList.append(getUpdateStatusTabReq(resCodeDict))
            print ("runVldStateUpdate says: this is vldresCodedict in 1 iteration\n",resCodeDict)
   #         time.sleep(2)
            resetVldResCodeDict()
  #####################################################################
def doVldProcessForDataSessionsOf24hDB(list):
    stlst = list
    reqList = []
    print("stlst:", stlst)
    for dataSession in stlst:
        print(dataSession)

        #    print (datetime.now())
        #  time.sleep(3)
        id = dataSession[0]
        fk = dataSession[2]
        tab = dataSession[1]
        stationExists = not isStationOf24hDBCanceled(tab)
        #     print  (dataSession)
        monList = getMonListFromStationsTableOf24hDB(tab)

        argsForVldClient = []

        for mon in monList:
            val = getValueFrom10mDB(tab, mon, fk)

            if val is None:
                appendVldCode(tab, mon, id, nodataCode)
             #   resCodeDict = getVldResCodeDict()
            else:
                argsForVldClient.append(reqArgs(tab, mon, fk))
                vldClient(argsForVldClient)
           #     resCodeDict = getVldResCodeDict()
        resCodeDict = getVldResCodeDict()
        updateVldTabOf10mDB(resCodeDict)
        updateStatusTabOf10mDB(resCodeDict)

        #   reqList.append(getUpdateVldTabReq(resCodeDict))
        #   reqList.append(getUpdateStatusTabReq(resCodeDict))
        print("runVldStateUpdate says: this is vldresCodedict in 1 iteration\n", resCodeDict)
        #         time.sleep(2)
        resetVldResCodeDict()
#stlst = getNFromStatusTable(10,f" FK < {idNowPlus22} AND (datastate = 1 OR datastate = 100) and  vldState =2  "," FK  desc")
#doVldProcessForDataSessions(stlst)
#print ("Vld Process For 10 partial validated data sessions is finished ")