import math
import time
from collections import namedtuple
from datetime import datetime, timedelta
from vldCodeMetods import resetVldResCoodeDict,appendVldCode, getVldResCodeDict,updateVldTab,updateStatusTab
from chainValidator import vldClient,getValueFromDB
from tools import getIDbyTime,execSelectData,getNFromStatusTable,isStationCanceled,getMonListFromStationsTable,execListOfUpdateReq
import pyodbc as pyodbc
from vldCodeMetods import vldCodeDict

nodataCode = 255
reqArgs = namedtuple("reqArgs", "table mon id")
#delta22m = timedelta(minutes=22)
#now = datetime.now()
#print("from:", now)
#idNowPlus22 = getIDbyTime(now + delta22m)

def doVldProcessForDataSessions(list):
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
            stationExists = not isStationCanceled(tab)
       #     print  (dataSession)
            monList=  getMonListFromStationsTable(tab)

            argsForVldClient=[]

            for mon in monList:
                   val= getValueFromDB(tab,mon,fk)

                   if val is None:
                       appendVldCode(tab,mon,id,nodataCode)
                       resCodeDict = getVldResCodeDict()
                   else:
                       argsForVldClient.append(reqArgs(tab,mon,fk))
                       vldClient(argsForVldClient)
                       resCodeDict = getVldResCodeDict()
            updateVldTab(resCodeDict)
            updateStatusTab(resCodeDict)


         #   reqList.append(getUpdateVldTabReq(resCodeDict))
         #   reqList.append(getUpdateStatusTabReq(resCodeDict))
            print ("runVldStateUpdate says: this is vldresCodedict in 1 iteration\n",resCodeDict)
   #         time.sleep(2)
            resetVldResCoodeDict()
  #####################################################################

#stlst = getNFromStatusTable(10,f" FK < {idNowPlus22} AND (datastate = 1 OR datastate = 100) and  vldState =2  "," FK  desc")
#doVldProcessForDataSessions(stlst)
#print ("Vld Process For 10 partial validated data sessions is finished ")