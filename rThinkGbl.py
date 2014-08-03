# -*- coding: cp1252 -*-.
import logging
from rThinkDb import *
from rThinkFunctions import *
from rThinkNames import *
import difflib
import pypyodbc
import datetime
import re
import inspect
import traceback
import logging
import sys

# init var
CountryISO      = {}
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
wait            = 0
# database flag YES/NO
YES             = -1
NO              = 0
queue_fn        = "" 
nxpage_fn       = "" 
parse_fn        = "" 
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

#datefmt='%d-%m %H:%M'


# console
logger = logging.getLogger("rThink")
logger.setLevel(logging.DEBUG)

chformat = logging.Formatter("[%(levelname)-8s] [%(message)-50s]")
chandler = logging.StreamHandler(stream=sys.stdout)
chandler.setFormatter(chformat)
chandler.setLevel(logging.INFO)
logger.addHandler(chandler)

# file
#fh = logging.getLogger("mylogger")
fhformat = logging.Formatter('[%(levelname)-8s] [%(asctime)s] [%(message)s]', "%d-%m %H:%M:%S")
fhandler = logging.FileHandler("rThink.log", mode='w')
fhandler.setFormatter(fhformat)
fhandler.setLevel(logging.DEBUG)
logger.addHandler(fhandler)


#logger.debug("This is a debug message.")
#logger.info("Some info message.")
#logger.warning("A warning.")
#logger.warning(now)


def log(level, message=''):

    if level == DEBUG:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        runmsg = "--> %s" % (inspect.stack()[1][3])   # nome della funzione
        logger.debug(runmsg)
        if message != '':
            logger.debug(message)    
        #logging.debug(stack_trace[:-1])

    if level == INFO:
        logger.info(message)

    if level == WARNING or level == WARN:
        logger.warn(message)
    
    if level == ERROR:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        runmsg = "--> %s" % (inspect.stack()[1][3])   # nome della funzione
        logger.error(runmsg)
        if message != '':
            logger.error(message)    
        logging.error(stack_trace[:-1])
        
    if level == CRITICAL or level == FATAL:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        if message != '':
            logger.critical(message)
        logger.critical(stack_trace[:-1])