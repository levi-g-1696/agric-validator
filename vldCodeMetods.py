import pyodbc as pyodbc
from tools import  execSelectDataFor10mDB,execListOfUpdateReqFor10mDB,execListOfUpdateReqFor24hDB
from DataStateUpdate import  isNoDataFor10mDB
vldCodeDict= dict()
vldResCodeDict= dict()
tst={}
#
def appendVldCode (tab,mon,id,vldVal):
      key= str(tab)+"."+str(mon)+"."+str(id)
      if key in vldCodeDict:
        vldCodeDict[key].append (vldVal)
        vldResCodeDict[key]= calcResCode(vldCodeDict[key])
      else :
          vldCodeDict[key]= []
          vldCodeDict[key].append(vldVal)
          vldResCodeDict[key] = calcResCode(vldCodeDict[key])

   ################################################

def calcResCode(lst):
    ok = 1
    partialOK5 = 5
    partialOK4 = 4
    partialOK3 = 3
    rangeErr = 200
    seqSusp223141 = 91
    seqRegsusp223241 = 92
    seqRegSanSusp223242 = 93
    regsusp213241 = 94
    regSanSusp213242 = 95
    sanSusp213142 = 96
    seqSusp223142 = 97
    nodata = 255

    # the lst have to contain 11 or 13
    if 255 in lst: return nodata
    if (13 in lst) or (11 not in lst):
        return rangeErr
    else:
        if 22 in lst and 32 not in lst and 42 not in lst: return seqSusp223141
        if 22 in lst and 32 in lst and 42 not in lst: return seqRegsusp223241
        if 22 in lst and 32 not in lst and 42 in lst: return seqRegSanSusp223242
        if 22 not in lst and 32 in lst and 42 not in lst: return regsusp213241
        if 22 not in lst and 32 in lst and 42 in lst: return regSanSusp213242
        if 22 not in lst and 32 not in lst and 42 in lst: return sanSusp213142
        if 22 in lst and 32 not in lst and 42 in lst: return seqSusp223142
        if 21 not in lst: return partialOK5
        if 31 not in lst: return partialOK4
        if 41 not in lst: return partialOK3

        return ok
    ############################################################
def updateVldTabOf10mDB(vldResDict):
    vldState=0
    id = 0
    setStr = ""
    tab = ""

    anykey = next(iter(vldResDict))
    anykeyArr = anykey.split(".")
    tab = "[" + anykeyArr[0] + "v]"
    id = anykeyArr[2]
    for key in vldResDict:
        keyArr = key.split(".")
        mon = keyArr[1]
        setStr = setStr + f" [{mon}] = {vldResDict[key]},"
    setStr = setStr[:-1]
    req = f"UPDATE {tab} SET {setStr}  WHERE id= {id}"
    print("make update: ", req)
    execListOfUpdateReqFor10mDB([req])
##################################################################
def updateVldTabOf24hDB(vldResDict):
    vldState=0
    id = 0
    setStr = ""
    tab = ""

    anykey = next(iter(vldResDict))
    anykeyArr = anykey.split(".")
    tab = "[" + anykeyArr[0] + "v]"
    id = anykeyArr[2]
    for key in vldResDict:
        keyArr = key.split(".")
        mon = keyArr[1]
        setStr = setStr + f" [{mon}] = {vldResDict[key]},"
    setStr = setStr[:-1]
    req = f"UPDATE {tab} SET {setStr}  WHERE id= {id}"
    print("make update: ", req)
    execListOfUpdateReqFor24hDB([req])
    ###########################################################################
def updateStatusTabOf10mDB(vldResDict):
        ok = 1
        cannotValidate=0
        partialOKexists=2
        partialOK5 = 5
        partialOK4 = 4
        partialOK3 = 3
        invalidExist = 200
        invalidValue = 200
        nodataExist=210
        suspValueExists= 90
        seqSusp223141 = 91
        seqRegsusp223241 = 92
        seqRegSanSusp223242 = 93
        regsusp213241 = 94
        regSanSusp213242 = 95
        sanSusp213142 = 96
        seqSusp223142 = 97
        nodata = 255
        statusVal=0
        anykey = next(iter(vldResDict))
        anykeyArr = anykey.split(".")
        tab = anykeyArr[0]
        id = anykeyArr[2]
        vldResultSet= set(vldResDict.values())
        if nodata in vldResultSet and len(vldResultSet)==1: statusVal=cannotValidate
        elif ok in vldResultSet and  len(vldResultSet)==1: statusVal=ok
        elif nodata in vldResultSet and len(vldResultSet) > 1: statusVal= nodataExist
        elif invalidValue in vldResultSet : statusVal= invalidExist
        elif seqSusp223141 in vldResultSet or seqRegsusp223241 in vldResultSet or seqRegSanSusp223242 in vldResultSet or regsusp213241 in vldResultSet or regSanSusp213242 in vldResultSet or sanSusp213142  in vldResultSet or seqSusp223142  in vldResultSet:
            statusVal= suspValueExists
        elif partialOK5 in vldResultSet or partialOK4  in vldResultSet or  partialOK3  in vldResultSet: statusVal=partialOKexists

        req = f"UPDATE [VLDstat] SET [VldState] ={statusVal}  WHERE  [TableName] ='{tab }' and FK= {id}"
        print("make update: ", req)
        execListOfUpdateReqFor10mDB([req])
##########################################################

    ############################################################
"""def calcVldState(tab,id):
    invalid=200
    suspVal=90
    partialOk=2
    ok=1
    cannotValidate=0
    req= f"select * from [{tab}]where id= {id}"
    res= execSelectData(req)
    if isNoData(tab,id) : return
    respSet= set(res)
    if 13 in respSet: return nodata  



            return ok  """
##########################################################

def getVldResCodeDict():
    return vldResCodeDict

def removeFromVldCodeDict(tab,mon,id):
    return
def resetVldResCodeDict():
    global vldResCodeDict
    vldResCodeDict = dict()
    return

def getScriptFromDB(mon,type):
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=DESKTOP-3BJPAFM\\SQLEXPRESS;"
                          "Database=agr-dcontrol;"
                          "Trusted_Connection=yes;")

    cursor = cnxn.cursor()
  #  print(f"select script from vldscripts where id= (select [scriptid] from MonToVldScript where  mon  COLLATE Latin1_General_CS_AS = '{mon} and type={type}) ")
    cursor.execute(f"select script from vldscripts where id= (select [scriptid] from MonToVldScript where  mon  COLLATE Latin1_General_CS_AS = '{mon}' and type={type}) ")

    row = cursor.fetchall()
    result = []
    if len(row)==0 : print(f"getScriptFromDB says: cannot find script for {mon} type {type}on DB")
    for scr in row:
        result.append(scr[0])
   #     print("rrr  ",scr[0])

    return result[0]
def execScript(mon,vldType,val):
  script= getScriptFromDB(mon,vldType)
 # print (script)
  exec (script,globals())
  res = vldFunc(val)

 # print (res)
  return res
#execScript("monWD",545)
