import math
import time
from collections import namedtuple
from datetime import datetime, timedelta
from vldCodeMetods import resetVldResCodeDict,appendVldCode, getVldResCodeDict,updateVldTabOf10mDB,updateStatusTabOf10mDB
from chainValidator import vldClient,getValueFrom10mDB
from tools import getIDbyTime,execSelectDataFor10mDB,getNFromStatusTableOf10mDB,isStationOf10mDBCanceled,getMonListFromStationsTableOf10mDB
import pyodbc as pyodbc
from vldCodeMetods import vldCodeDict

nodataCode=255
reqArgs = namedtuple("reqArgs", "table mon id" )
delta22m= timedelta(minutes=22)
for k in  range (1):
        reqList = []
        now= datetime.now()
        idNowPlus22= getIDbyTime(now + delta22m)

        stlst = getNFromStatusTableOf10mDB(10, f" FK < {idNowPlus22} AND (datastate = 1 OR datastate = 100) and vldState !=1 and vldState !=2 and vldState <80 ", " FK  desc")
      #  stlst = getNFromStatusTable (1, f" id = 37559"," FK  desc")
        print("stlst:" , stlst)
        stlst = getNFromStatusTableOf10mDB(10, f" FK < {idNowPlus22} AND (datastate = 1 OR datastate = 100) and vldState !=1 and vldState !=2 and vldState <80 ", " FK  desc")
        stlst = getNFromStatusTableOf10mDB(10, f" FK < {idNowPlus22} AND (datastate = 1 OR datastate = 100) and vldState !=1 and vldState !=2 and vldState <80 ", " FK  desc")