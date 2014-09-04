# -*- coding: cp1252 -*-.
import logging
from rThinkDb import *
from rThinkFunctions import *
from rThinkNames import *
from rThinkParse import *
import difflib
import pypyodbc
import datetime
import re
import inspect
import traceback
import logging
import sys
import pprint
import traceback
import sys
import linecache
import locale
locale.setlocale(locale.LC_ALL, '')

# init var
GoogleSource    = 5 # la codifica fissa (in tabella)
N_Ass           = 0
testrun         = False
Frasi           = []
Drive           = []
Parole          = []
Proxies         = []  # proxy list
Useproxy        = False
CountryISO      = {}
gL.Funzioni     = []
RunDate         = SetNow()
GmapNumcalls    = 0
count           = 0
i               = 0
DsnProd         = 'DSN=rThink'
DsnTest         = 'DSN=rThinkTest'
Dsn             = ''
testurl         = ''
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

restart         = False
currency        = ""

preposizioni  = ["il","lo","l'","la","i","gli","le", "di","del","dello","dell'","della","dei","degli","delle","a","al",\
                 "allo","all'","alla","ai","agli","alle","da","dal","dallo","dall'","dalla","dai","dagli","dalle","in","nel",\
                 "nello","nell'","nella","nei","negli","nelle","su","sul","sullo","sull'","sulla","sui","sugli","sulle"]
pronomi       = ["io", "me", "mi", "tu", "te", "ti","egli","ella","lui","lei","lo","la","esso","essa","esso","essa","gli",\
                 "le","ne","noi","ci","voi","vi","essi","esse","loro","li","le","essi","esse","ne","ne","quale", "che", "quali", "cui"]
punteggiatura = [".", ",", ";", ":", "?", "!", "?!", "...", "\"", "'", "<", ">", "-", "(", ")", "[", "]", "*"]
congiunzioni =  ["&","pure", "inoltre", "ancora", "altresì", "ma", "però", "pure", "mentre", "anzi", "invece", "tuttavia", "dunque", "però",\
                 "quindi", "ondeperciò", "pertanto", "ebbene", "laondee", "pure", "né","inoltre", "ancora", "neppure", "neanche", "nemmeno", \
                 "e", "né", "o", "come", "così","sia", "che","quanto", "quale", "difatti", "cioè", "invero", "ossiaossia", "ovvero", "oppure"]
   
INFO     = logging.INFO
CRITICAL = logging.CRITICAL
FATAL    = logging.FATAL
DEBUG    = logging.DEBUG
ERROR    = logging.ERROR
CRITICAL = logging.CRITICAL
WARN     = logging.WARN
WARNING  = logging.WARNING

def SetLogger(RunId, restart):    
    
    logger = logging.getLogger()  # root logger
    if len (logger.handlers) > 0:  # remove all old handlers        
        logger.handlers = []
    
    logger.setLevel(logging.DEBUG)   # default level
     
    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
 
    # create error file handler and set level to error
    #handler = logging.FileHandler(os.path.join(output_dir, "error.log"),"w", encoding=None, delay="true")
    if restart:
        handler = logging.FileHandler("C:\\rThink\\Log\\"+str(RunId)+'.err','a', encoding=None, delay="true")
    else:
        handler = logging.FileHandler("C:\\rThink\\Log\\"+str(RunId)+'.err','w', encoding=None, delay="true")
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('[%(levelname)-8s] [%(asctime)s] [%(message)s]', "%d-%m %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
 
    # create debug file handler and set level to debug
    if restart:
        handler = logging.FileHandler("C:\\rThink\\Log\\"+str(RunId)+".log","w")
    else:
        handler = logging.FileHandler("C:\\rThink\\Log\\"+str(RunId)+".log","a")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(levelname)-8s] [%(asctime)s] [%(message)s]', "%d-%m %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    gL.log(gL.INFO, 'INIZIO DEL RUN')

    return True



def log(level, *message):
    runmsg = ''
    logger = logging.getLogger()
    if level == DEBUG:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        runmsg = "--> %s" % (inspect.stack()[1][3])   # nome della funzione
        logger.debug(runmsg)
        for msg in message:        
            runmsg = "--> %s" % (msg) 
            logger.debug(runmsg)    
        #logging.debug(stack_trace[:-1])

    if level == INFO:
        for msg in message:        
            runmsg = "%s" % (msg) 
            logger.info(runmsg)    

    if level == WARNING or level == WARN:
        for msg in message:        
            runmsg = "%s" % (msg) 
            logger.warn(message)
    
    if level == ERROR or level == CRITICAL or level == FATAL:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        runmsg = "--> %s" % (inspect.stack()[1][3])   # nome della funzione
        logger.error(runmsg)
        for msg in message:        
            runmsg = "--> %s" % (msg) 
            logger.error(runmsg)    
        exc_type, exc_value, exc_traceback = sys.exc_info()
        if exc_type is not None:
            filename = exc_traceback.tb_frame.f_code.co_filename
            lineno = exc_traceback.tb_lineno
            line = linecache.getline(filename, lineno)
            logger.error("--> Riga:%d - %s" % (lineno, line.strip()))
            #for line in pprint.pformat(stack_trace[:-1]).split('\n'):
            for line in stack_trace:
                logging.error(line)
        