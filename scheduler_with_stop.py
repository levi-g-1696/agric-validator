
import json,os
import time
from collections import namedtuple
from datetime import datetime, timedelta
from tools import getIDbyTime,execSelectData,getNFromStatusTable,isStationCanceled,getMonListFromStationsTable,execListOfUpdateReq
from VldStateUpdate import doVldProcessForDataSessions
from DataStateUpdate import runDataStateUpdate

statusFile=".\\runStatus.json"
workDirectory= r"C:\Users\office22\PycharmProjects\validator"
##############################################################
def setRunFlagON():
    with open(statusFile, 'r') as f:
        json_data = json.load(f)
    json_data["runFlag"] = 'run'  # On this line you needed to add ['embed'][0]
    with open(statusFile, 'w') as f:
        json.dump(json_data, f,indent=2)
 ##################################################################
def setLastRunDate():
    with open(statusFile, 'r') as f:
        json_data = json.load(f)
    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    json_data["lastRunDate"] = date_string # On this line you needed to add ['embed'][0]
    with open(statusFile, 'w') as f:
        json.dump(json_data, f, indent=2)
 ###################################################
def setLastExecTime(t):
    with open(statusFile, 'r') as f:
        json_data = json.load(f)

    json_data["lastExecTime"] = str(t)  # On this line you needed to add ['embed'][0]
    with open(statusFile, 'w') as f:
        json.dump(json_data, f, indent=2)
 #########################################
def getRunFlag():

    with open(statusFile, 'r') as f:
        json_data = json.load(f)
        if json_data["runFlag"] == 'run':
              return True
        else:return False
        ############################################################
def getNormalExecTime():

    with open(statusFile, 'r') as f:
                json_data = json.load(f)

                return json_data["normalExecTime"]

        #####################################################
def setRunFlagOFF():
    import json
    statusFile = ".\\runStatus.json"
    with open(statusFile, 'r') as f:
        json_data = json.load(f)
    json_data["runFlag"] = 'stop'  # On this line you needed to add ['embed'][0]
    with open(statusFile, 'w') as f:
        json.dump(json_data, f,indent=2)
        ###########################################################################


if __name__ == '__main__':
    os.chdir(workDirectory)
    setRunFlagON()
    while getRunFlag():

 #tool must be here
    ################### operation body  #######################
         nodataCode = 255
         reqArgs = namedtuple("reqArgs", "table mon id")
         delta22m = timedelta(minutes=22)
         now = datetime.now()
         print("from:", now)
         idNowPlus22 = getIDbyTime(now + delta22m)
         runDataStateUpdate(500)
         print ("datastate update for 500 rec is finished")

         stlst = getNFromStatusTable(50, f" FK < {idNowPlus22} AND (datastate = 1 OR datastate = 100) and vldState =0 ",
                                     " FK  desc")
         print("list for vld-process:", stlst)
         time.sleep(3.3)
         doVldProcessForDataSessions(stlst)
         print("\nVld Process For 50 new data sessionsis finished ")
         now2 = datetime.now()
         #print("elapsed time:", now2 - now,"\n")

      #######################################

         dt= datetime.now()
         setLastRunDate()
         setLastExecTime(now2-now)
         t=5
         normaltime= getNormalExecTime()
         print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
         print(f"      {dt}  \n      python scheduller with stop option is running .\n  the main task for update validation tables  is activated every {t} sec  ")
         print (f"   normal execution time: {normaltime} ")
         print ("   last execution time: ",now2-now)
         print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
         time.sleep(t)
print ("normal exit")


