"""
Паттерн Цепочка обязанностей

Назначение: Позволяет передавать запросы последовательно по цепочке
обработчиков. Каждый последующий обработчик решает, может ли он обработать
запрос сам и стоит ли передавать запрос дальше по цепи.
"""

from __future__ import annotations
from collections import namedtuple
from abc import ABC, abstractmethod
from tools import  getValueFromDB
from vldCodeMetods import execScript,appendVldCode,getVldResCodeDict
import random
from math import nan
from typing import Any, Optional

import pyodbc

class Handler(ABC):
    """
    Интерфейс Обработчика объявляет метод построения цепочки обработчиков. Он
    также объявляет метод для выполнения запроса.
    """

    @abstractmethod
    def set_next(self, handler: Handler) -> Handler:
        pass

    @abstractmethod
    def handle(self, request) -> Optional[str]:
        pass


class AbstractHandler(Handler):
    """
    Поведение цепочки по умолчанию может быть реализовано внутри базового класса
    обработчика.
    """

    _next_handler: Handler = None

    def set_next(self, handler: Handler) -> Handler:
        self._next_handler = handler
        # Возврат обработчика отсюда позволит связать обработчики простым
        # способом, вот так:
        # monkey.set_next(squirrel).set_next(dog)
        return handler

    @abstractmethod
    def handle(self, request: Any) -> str:
        if self._next_handler:
            return self._next_handler.handle(request)

        return None


"""
Все Конкретные Обработчики либо обрабатывают запрос, либо передают его
следующему обработчику в цепочке.
"""


class RangeValidator(AbstractHandler):
    def handle(self,request: Any) -> str:
        vldCode=11

        if self.vldFunc( request.table,request.mon, request.id):
         #   print ( f"in {request.table} range of {request.mon} is ok , where id= {request.id }\n go to next check")
            appendVldCode(request.table,request.mon,request.id,vldCode)
            return super().handle(request)
        else:
            vldCode = 13
            appendVldCode(request.table, request.mon, request.id, vldCode)
            return(f"in {request.table} range of {request.mon} is BAD , where id= {request.id }\n stop check chain")

    def vldFunc(self,tab, mon, id):

      val =  getValueFromDB(tab,mon,id)

      vldResult= execScript(mon,1,val)
    #  print ("range says:",vldResult)
      return vldResult
class SuspSequenceValidator(AbstractHandler):
    def handle(self, request: Any) -> str:
        vldCode = 21
        if self.vldFunc(request.mon, request.table, request.id):
 #           print ( f"in {request.table} value sequnce of {request.mon} is ok , where id= {request.id}")
            appendVldCode(request.table, request.mon, request.id, vldCode)
            return super().handle(request)

        else:
            vldCode = 22
            appendVldCode(request.table, request.mon, request.id, vldCode)
            return super().handle(request)
    def vldFunc(self,mon, tab, id):
        val = getValueFromDB(tab, mon, id)
        vldResult = execScript(mon, 2, val)
  #      print("sequence says:", vldResult)
        return vldResult

class RegionalCheckValidator(AbstractHandler):
    def handle(self, request: Any) -> str:
        vldCode = 31
        if self.vldFunc(request.mon, request.table, request.id):
 #           print(f"in {request.table} value sequnce of {request.mon} is ok , where id= {request.id}")
            appendVldCode(request.table, request.mon, request.id, vldCode)
            return super().handle(request)

        else:
            vldCode = 32
            appendVldCode(request.table, request.mon, request.id, vldCode)
            return super().handle(request)

    def vldFunc(self,mon, tab, id):
        a=  random.randint(0,100)
        if a > 40:
            return True
        else:
            return False

class DataSanityValidator(AbstractHandler):
    #######################################################
    def handle(self, request: Any) -> str:
        vldCode = 41
        if self.vldFunc( request.table, request.mon,request.id):
    #        print(f"in {request.table} value sequnce of {request.mon} is ok , where id= {request.id}")
            appendVldCode(request.table, request.mon, request.id, vldCode)
            return super().handle(request)

        else:
            vldCode = 42
            appendVldCode(request.table, request.mon, request.id, vldCode)
            return super().handle(request)
###################################################
    def vldFunc(self,mon, tab, id):
        a=  random.randint(0,100)
        if a > 40:
            return True
        else:
            return False

########################################################
def vldClient(arglist) :
    checkRange = RangeValidator()
    checkSequnce = SuspSequenceValidator()

 #   checkRange.set_next(checkSequnce)
 #example for chain monkey.set_next(squirrel).set_next(dog)
    for arg in arglist:
     #   print(f"\nClient: processing VLD check for {arg}?")
        checkRange.handle(arg)

    return

"""
if __name__ == "__main__":
    arg1 = reqArgs("a13", "monWD", 230609181)
    arg2 = reqArgs("a13", "monWD", 230609182)
    arg3 = reqArgs("a13", "monWD", 230609183)
    args = [arg1, arg2, arg3]
    checkRange = RangeValidator()
    checkSequnce = SuspSequenceValidator()
    vldClient(args)
    print("\n")
    print(getVldCodeDict())"""

 #   checkRange.set_next(checkSequnce)


    # Клиент должен иметь возможность отправлять запрос любому обработчику, а не
    # только первому в цепочке.
   # print("Chain: range > SuspSequence > ")

   # a= getValueFromDB("z13","monWD",230509181)
  #  print(a)

