import time
from collections import namedtuple
from datetime import datetime, timedelta
from mainVld import vldClient
from tools import getIDbyTime,execSelectData,getNFromStatusTable,isStationCanceled,getMonListFromStationsTable
import pyodbc as pyodbc
from vldCodeMetods import vldCodeDict


reqArgs = namedtuple("reqArgs", "table mon id" )
delta22m= timedelta(minutes=22)
for k in  range (2):
        reqList = []
        now= datetime.now()
        idNowPlus22= getIDbyTime(now + delta22m)

        stlst = getNFromStatusTable(50,f" FK< {idNowPlus22} AND (datastate = 1 OR datastate = 100) and vldState !=1 and vldState <80 "," FK desc")


        for dataSession in stlst:
            id = dataSession[0]
            fk = dataSession[2]
            tab = dataSession[1]
            stationExists = not isStationCanceled(tab)
       #     print  (dataSession)
            monList=  getMonListFromStationsTable(tab)
            argsForVldClient=[]
            for mon in monList:
                argsForVldClient.append(reqArgs(tab,mon,fk))
            resCodeDict=  vldClient(argsForVldClient)
            #make vldCodeDict dict with as this
            print (resCodeDict)

