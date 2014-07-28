# -*- coding: cp1252 -*-.
from rThinkFunctions import *
import logging
from rThinkDb import *
from rThinkFunctions import *
from rThinkNames import *
import difflib
import pypyodbc
import datetime
import re

# init var
RunDate         = SetNow()
GmapNumcalls    = 0
count           = 0
i               = 0
sourcebaseurl   = ""
assetbaseurl    = ""
MySql           = None
cSql            = None   #
SqLite          = None   # connection
cLite           = None   # cursore sqlite
RunId           = 0

# database flag YES/NO
YES             = -1
NO              = 0

preposizioni  = ["il","lo","l'","la","i","gli","le", "di","del","dello","dell'","della","dei","degli","delle","a","al",\
                 "allo","all'","alla","ai","agli","alle","da","dal","dallo","dall'","dalla","dai","dagli","dalle","in","nel",\
                 "nello","nell'","nella","nei","negli","nelle","su","sul","sullo","sull'","sulla","sui","sugli","sulle"]
pronomi       = ["io", "me", "mi", "tu", "te", "ti","egli","ella","lui","lei","lo","la","esso","essa","esso","essa","gli",\
                 "le","ne","noi","ci","voi","vi","essi","esse","loro","li","le","essi","esse","ne","ne","quale", "che", "quali", "cui"]
punteggiatura = [".", ",", ";", ":", "?", "!", "?!", "...", "\"", "'", "<", ">", "-", "(", ")", "[", "]", "*"]
congiunzioni =  ["&","pure", "inoltre", "ancora", "altres�", "ma", "per�", "pure", "mentre", "anzi", "invece", "tuttavia", "dunque", "per�",\
                 "quindi", "ondeperci�", "pertanto", "ebbene", "laondee", "pure", "n�","inoltre", "ancora", "neppure", "neanche", "nemmeno", \
                 "e", "n�", "o", "come", "cos�","sia", "che","quanto", "quale", "difatti", "cio�", "invero", "ossiaossia", "ovvero", "oppure"]
