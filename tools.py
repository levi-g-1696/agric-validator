import time

import pyodbc


def getIDbyTime(dt):
# dt is datetime type

  y= dt.year-2000
  m= dt.month
  d= dt.day
  h=dt.hour
  min= dt.minute
  id= int ( y* 10000000 + m * 100000 + d*1000 + h*10 + min/10)
  return id


def getNFromStatusTable(num, condition, order):
  result = []

  req = f"""SELECT TOP ({num}) [id]
    ,[TableName]
    ,[FK]
    ,[DataState]
    ,[FileTime]
    ,[VldState]
    ,[SendState]
    ,[R]
FROM [agr-dcontrol].[dbo].[VLDstat] where {condition} order by {order}"""
  result = execSelectData(req)
  return result

  # cursor.commit()

  ######################################

def execSelectData(req):
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=DESKTOP-3BJPAFM\\SQLEXPRESS;"
                          "Database=agr-dcontrol;"
                          "Trusted_Connection=yes;")
    cursor = cnxn.cursor()

    try:
    #  print("execSelelect says:exequte sql query:\n", req)
      cursor.execute(req)
      rows = cursor.fetchall()
    except pyodbc.OperationalError as msg:

      print("Command skipped: ", msg)
    return rows
##############################################
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
        #        print("exequte sql query:\n", req)
                cursor.execute(req)
            except pyodbc.OperationalError as msg:
                print("Command skipped: ", msg)
            except pyodbc.Error as deadlockMsg: # try after 1s after deadlock
                print("Try second time : ", deadlockMsg)
                time.sleep(1)
                temporaryList= [req]
                execListOfUpdateReq(temporaryList)
        cursor.commit()
#####################################
def getMonListFromStationsTable(tabName):
   ## print ("getmonlist says: trying connect to db.  table ",tabName)
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=DESKTOP-3BJPAFM\\SQLEXPRESS;"
                          "Database=agr-dcontrol;"
                          "Trusted_Connection=yes;")

    cursor = cnxn.cursor()

 #   d = cursor.execute("select tag from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tabName +"'")
   # row = cursor.fetchall()
  #  result = []
  #  for t in row:
   #     result.append(t[0])
 #   cnxn.close()
   # return result

#####################################################

def getValueFromDB(tab,mon,id):

        cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                              "Server=DESKTOP-3BJPAFM\\SQLEXPRESS;"
                              "Database=agr-dcontrol;"
                              "Trusted_Connection=yes;")

        cursor = cnxn.cursor()
      #  print(f"select {mon} from [dbo].[{tab}] where id= {id}")
        cursor.execute(
            f"select {mon} from [dbo].[{tab}]where id= {id} ")

        row = cursor.fetchall()
        result = []
        if len(row)==0: return None
        else:
          for val in row:
            result.append(val[0])
          return result[0]
    ################################

  ##############################################################
def getMonListFromStationsTable(tabName):
    ## print ("getmonlist says: trying connect to db.  table ",tabName)
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=DESKTOP-3BJPAFM\\SQLEXPRESS;"
                          "Database=agr-dcontrol;"
                          "Trusted_Connection=yes;")

    cursor = cnxn.cursor()
    req = f"select monitors from [dbo].[stations] where tag='{tabName}'"
    cursor.execute(req)
    row = cursor.fetchone()
    #  print (row)
    monstring = row[0]

    monlist = monstring.split(";")
    return monlist


############################################
def isStationCanceled(tab):
    req= f"SELECT  [status]  FROM [agr-dcontrol].[dbo].[stations] where tag= '{tab}'"
    res = execSelectData(req)
    yesCanceled = True
    if len(res) ==0 : return yesCanceled
    else:
      res = res[0][0]
    #print("is station exist says:", res)
      yesCanceled= res==0
      return yesCanceled

"""
d={'a37.monWS.230612142': 5, 'a37.monWD.230612142': 5, 'a37.monWDSTc.230612142': 5, 'a37.monWSMax.230612142': 5, 'a37.monT.230612142': 5, 'a37.monRH.230612142': 5, 'a37.monT12m.230612142': 5, 'a37.monT10m.230612142': 5, 'a37.monRAD.230612142': 5, 'a37.monPREC10.230612142': 5, 'a37.monBV.230612142': 5}
id=0
setStr=""
tab=""
anykey= next(iter(d))
anykeyArr= anykey.split(".")
tab = "["+ anykeyArr[0] + "v]"
id =  anykeyArr[2]
for key in d:
    keyArr= key.split(".")
    mon = keyArr[1]
    setStr=setStr+ f" [{mon}] = {d[key]},"
setStr= setStr[:-1]
req=f"UPDATE {tab} SET {setStr}  WHERE id= {id}"
print (req)
"""
