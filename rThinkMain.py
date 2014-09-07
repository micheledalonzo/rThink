# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python

# Gambero Rosso 1 Forchetta
# Gambero Rosso 2 Forchette
# Gambero Rosso 3 Forchette
# Espresso 1 Cappello
# Espresso 2 Cappelli
# Espresso 3 Cappelli
# Michelin 1 Stella
# Michelin 2 Stelle
# Michelin 3 Stelle
# Veronelli 1 Stella
# Veronelli 2 Stelle
# Veronelli 3 Stelle
# Gambero Rosso 1 Gambero
# Gambero Rosso 2 Gamberi
# Gambero Rosso 3 Gamberi
# Guida Michelin 1 Forchetta
# Guida Michelin 2 Forchette
# Guida Michelin 3 Forchette
# Guida Michelin 4 Forchette
# Guida Michelin 5 Forchette
# Guida Touring
# empty list = False
#

from lxml import html
import argparse
import collections
import pypyodbc
import datetime
import time
import re
import sys
import locale
# import jabba_webkit as jw
from urllib.parse import urlparse
import rThinkGbl as gL
#import rThinkParse
work_queue = collections.deque()


def RunInit():        
    try:
        rc = gL.CreateMemTableKeywords()
        rc = gL.sql_CopyKeywordsInMemory()
        rc = gL.LoadProxyList()
        if not rc:       
            gL.Useproxy = False        
    
        # fill lista delle funzioni per il trattamento delle pagine
        gL.cSql.execute("select source, assettype, country, NextPageFn, QueueFn, ParseFn from Drive")
        gL.Funzioni = gL.cSql.fetchall()

        return True

    except Exception as err:

        gL.log(gL.ERROR, err)
        return False


def RunPrepare():
    try:
        # Leggo la tabella guida per ogni sorgente, paese, tipo e metto tutti gli starturl nella tabella runlog
        gL.cSql.execute("SELECT * FROM QDrive ORDER BY rnd(starturlid)")
        gL.Drive = gL.cSql.fetchall()
        if len(gL.Drive) > 0:
            RunInit()
   
        for drive in gL.Drive:              # inserisco gli starturl nel run
            country = drive['country']  
            source = drive['source']
            assettype = drive['assettype']
            refresh = drive['refresh']
            language = drive['countrylanguage']     
            starturl = drive['starturl']     
            pageurl = starturl        
            SetLocaleString = drive['setlocalestring']
            # controllo la congruenza
            if not gL.OkParam():
                return False
            # gestione della lingua per l'interpretazione delle date
            if not SetLocaleString:          
                gL.log(gL.ERROR, "SetLocaleString non settata in QDrive")
                return False
            locale.setlocale(locale.LC_TIME, SetLocaleString)  
        
            # se richiesto cancello e ricreo la coda, ma solo per le righe dipendenti dallo starturl
            if not refresh:
                gL.cSql.execute("Delete * from queue where source = ? and AssetType = ? and Country = ? and StartUrl = ?", (source, assettype, country, starturl))           
                gL.cSql.execute("Delete * from pages where source = ? and AssetType = ? and Country = ? and StartUrl = ?", (source, assettype, country, starturl))
           
            # metto in tabella Pages tutti gli starturl che devo fare
            if not refresh:
                rc = gL.PagesCreate(source, assettype, country, starturl, pageurl)
    
        gL.cSql.commit()
        gL.log(gL.DEBUG, "Commit")
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

def BuildAssetList(country, assettype, source, starturl, pageurl, runlogid):    
    try:

        #   inizia da starturl e interpreta le pagine di lista costruendo la coda degli asset da esaminare
        #rc = gL.PagesCreate(source, assettype, country, starturl, pageurl)
        #gL.sql_Queue(country, assettype, source, starturl, pageurl)
        work_queue.append((pageurl, ""))

        while len(work_queue):
            pageurl, newpage = work_queue.popleft()            
            msg ="%s - %s" % ("PAGINATE", pageurl)
            gL.log(gL.INFO, msg)
            if newpage == '':
                page = gL.ReadPage(pageurl)
            else:
                page = newpage
            if page is not None:
                # inserisce la pagina da leggere nel runlog
                rc = gL.PagesStatus("START", country, assettype, source, starturl, pageurl)                                
                # legge la pagina lista, legge i link alle pagine degli asset e li inserisce nella queue
                rc = gL.BuildQueue(country, assettype, source, starturl, pageurl, page)
                # aggiorna il log del run con la data di fine esame della pagina
                gL.PagesStatus("END", country, assettype, source, starturl, pageurl)
                gL.cSql.commit()                
                # legge la prossima pagina lista                
                newpageurl, newpage = gL.ParseNextPage(source, assettype, country, pageurl, page)
                if newpageurl:
                    #gL.sql_Queue(country, assettype, source, starturl, newpageurl)    # inserisce nella coda
                    work_queue.append((newpageurl, newpage))
                    gL.PagesCreate(source, assettype, country, starturl, newpageurl)
            
    except Exception as err:

        gL.log(gL.ERROR, err)
        return False
    
    return True

def RunRestart():
    try:
        RunInit()
        
        #gL.cSql.execute("SELECT Source, AssetType, Country, Max(Start) AS MaxStart, StartUrl, Last(Pageurl) AS UltimoDiPageurl, RunId FROM pages \
        #            WHERE RunId=? GROUP BY Source, Country, AssetType, StartUrl, RunId", ([gL.RunId]))
        gL.cSql.execute("SELECT * from QRestart")
        check = gL.cSql.fetchall()   # l'ultima
        if not check:
            pass
        else:
            for log in check:
                gL.assetbaseurl = log['drivebaseurl']  # il baseurl per la tipologia di asset
                language        = log['countrylanguage']  # lingua                                                               
                gL.currency     = log['countrycurr']
                gL.sourcebaseurl= log['sourcebaseurl']    
                source          = log['source']
                sourcename      = log['sourcename']                
                assettypename   = log['assettypename']                
                assettype       = log['assettype']
                country         = log['country']
                starturl        = log['starturl']
                pageurl         = log['ultimodipageurl']        
                SetLocaleString = log['setlocalestring']        
                # gestione della lingua per l'interpretazione delle date
                if not SetLocaleString:
                    gL.log(gL.ERROR, "SetLocaleString non settata in QDrive")
                    return False
                locale.setlocale(locale.LC_TIME, SetLocaleString)  
              
                # stampo i parametri di esecuzione
                msg=('RESTART PAGINAZIONE: RUN: %s SOURCE: %s ASSET: %s COUNTRY: %s REFRESH: BOH RESTART: %s' % (gL.RunId, sourcename, assettypename, country, gL.restart))
                gL.log(gL.INFO, msg)

                page = gL.ReadPage(pageurl)
                if page is not None:
                    newpageurl, newpage = gL.ParseNextPage(source, assettype, country, pageurl, page)  # leggo se esiste la prossima pagina 
                    if newpageurl:
                        rc = RunPaginate(country, assettype, source, starturl, pageurl)        
                        if not rc:
                            gL.log(gL.WARNING, "PAGINATE KO")
                            return False
        
        #for log in check:
            rc = RunParse(log)
            if not rc:
                gL.log(gL.WARNING, "PARSE KO")
                return False

        return True

    except Exception as err:

        gL.log(gL.ERROR, err)
        return False



def Run():
    try:    
        rc = RunPrepare()    

        # Leggo la tabella guida per ogni sorgente, paese, tipo
        for drive in gL.Drive:
            gL.assetbaseurl = drive['drivebaseurl']  # il baseurl per la tipologia di asset
            language = drive['countrylanguage']  # lingua
            country = drive['country']  # paese
            source = drive['source']
            assettype = drive['assettype']
            refresh = drive['refresh']
            sourcename = drive['sourcename']
            gL.currency = drive['countrycurr']
            assettypename = drive['assettypename']
            rundate = drive['rundate']
            rundate_end = drive['rundate_end']
            starturl = drive['starturl']     
            pageurl = starturl            
            gL.sourcebaseurl = drive['sourcebaseurl']    
        
            if not refresh:
                # stampo i parametri di esecuzione
                msg=('RUN: %s SOURCE: %s ASSET: %s COUNTRY: %s REFRESH: %s STARTURL: %s' % (gL.RunId, sourcename, assettypename, country, refresh, starturl))
                gL.log(gL.INFO, msg)
                rc = RunPaginate(country, assettype, source, starturl, pageurl)        
                if not rc:
                    gL.log(gL.WARNING, "PAGINATE KO")
                    return False
    
        for drive in gL.Drive:
            rc = RunParse(drive)
            if not rc:
                gL.log(gL.WARNING, "PARSE KO")
                return False

        return True

    except Exception as err:

        gL.log(gL.ERROR, err)
        return False



def RunPaginate(country, assettype, source, starturl, pageurl):        

    try:    
        # FASE DI PAGINAZIONE
        # ---------------- se non richiesto refresh faccio la paginazione 
        msg=('RUN: %s: PAGINAZIONE' % (gL.RunId))
        gL.log(gL.INFO, msg)
        BuildAssetList(country, assettype, source, starturl, pageurl, gL.RunId)       
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

def RunParse(drive):

    try:
        # FASE DI PARSING
        # ---------------- leggo dalla coda i link creati con il run corrente, in ordine casuale         
        if gL.testrun and gL.testurl:      # se è un giro di test, esamino solo url indicato
            gL.cSql.execute("SELECT * FROM Queue where starturl = ? and AssetUrl = ? ", (drive['starturl'], gL.testurl))
        else:
            if not gL.restart:
                msg='RUN: %s: PARSING %s' % (gL.RunId, drive['starturl'])
                gL.log(gL.INFO, msg)            
                gL.cSql.execute("SELECT * FROM Queue where starturl = ? ORDER BY rnd(queueid)", ([drive['starturl']]))
            else:
                gL.cSql.execute("SELECT * FROM QQueue ORDER BY rnd(queueid)")
                msg=('RESTART PARSE: RUN: %s SOURCE: %s ASSET: %s COUNTRY: %s REFRESH: BOH RESTART: %s' % (gL.RunId, drive['source'], drive['assettype'], drive['country'], gL.restart))
                gL.log(gL.INFO, msg)
        rows = gL.cSql.fetchall()

        gL.T_Ass = str(len(rows))       
        msg=('RUN %s: PARSING %s Assets' % (gL.RunId, gL.T_Ass))
        gL.log(gL.INFO, msg)

        gL.N_Ass = 0
        for row in rows:            
            gL.N_Ass = gL.N_Ass + 1              
            pageurl  = row['pageurl']
            assettype  = row['assettype']
            asseturl = row['asseturl'] 
            starturl = row['starturl'] 
            name     = row['nome']
            country  = row['country']
            source = row['source']
            #if gL.testrun:      # se è un giro di test, esamino solo url indicato
            #    if asseturl != gL.testurl:
            #        continue
            msg ="%s - %s" % ("PARSE", asseturl)
            gL.log(gL.INFO, msg)

            # parse delle singole pagine degli asset
            gL.QueueStatus("START", country, assettype, source, starturl, pageurl, asseturl) # scrivo nella coda che ho finito
            Asset = gL.ParseContent(country, assettype, source, starturl, asseturl, name)                                                                      
            if Asset:  # se tutto ok
                gL.cSql.commit()
                AssetMatch, AssetRef = gL.StdAsset(Asset)   # controllo se esiste già un asset simile
                if AssetMatch is False: # is evita che 0 sia interpretato come false
                    gL.log(gL.ERROR, "StdAsset ha resituito False", asseturl)
                    continue
                AAsset = gL.AAsset(Asset, AssetMatch, AssetRef)   # creo il record in Asset a partire da SourceAsseId corrente con riferimento al suo simile oppure lo aggiorno
                gL.QueueStatus("END", country, assettype, source, starturl, pageurl, asseturl) # scrivo nella coda che ho finito
                # per ogni asset una call a Google Places
                gAsset = gL.ParseGooglePlacesMain(Asset, AAsset)
                #if gAsset:  # se > 0
                #    AssetMatch, AssetRef = gL.StdAsset(gAsset)   # controllo se esiste già un asset simile
                #    rc = gL.AAsset(Asset, AssetMatch, AssetRef)   # creo il record in Asset a partire da SourceAsseId corrente con riferimento al suo simile oppure lo aggiorno
                               
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def main(dns):
    try:
        #---------------------------------------------- M A I N ----------------------------------------------------------------------------------
        # apri connessione e cursori, carica keywords in memoria
        gL.SqLite, gL.C = gL.OpenConnectionSqlite()
        gL.MySql, gL.Cursor = gL.OpenConnectionMySql(dns)
    
        
        # determino se devo restartare - prendo l'ultimo record della tabella run
        sql = "SELECT RunId, Start, End FROM Run GROUP BY RunId, Start, End ORDER BY RunId DESC"
        gL.cSql.execute(sql)
        check = gL.cSql.fetchone()
        if check:   # se esiste un record in Run
            runid = check['runid']
            end   = check['end']
            start = check['start']
            if end is None or end < start:
                gL.restart = True
                gL.RunId = runid
                # setto il runid, la data di inizio e il logger
                rc = gL.SetLogger(gL.RunId, gL.restart)
                gL.log(gL.WARNING, "Proxy:"+str(gL.Useproxy))        
                rc = RunRestart()  
                if not rc:   
                    return False
                else:
                    #chiudo le tabelle dei run
                    rc = gL.RunIdStatus("END")
                    rc = gL.sql_UpdDriveRun("END")
                    gL.cSql.commit()                        
                    return True
    
        # diversamente.... run normale
        gL.restart = False   
        gL.RunId = gL.RunIdCreate()
        # setto il runid, la data di inizio e il logger
        rc = gL.RunIdStatus("START")
        if not rc:
            return False
        rc = gL.SetLogger(gL.RunId, gL.restart)
        if not rc:
            return False
        gL.log(gL.WARNING, "Proxy:"+str(gL.Useproxy))        
        rc = Run()
        if not rc:   
            return False
        else:
            #chiudo le tabelle dei run
            rc = gL.RunIdStatus("END")
            rc = gL.sql_UpdDriveRun("END")
            gL.cSql.commit()    
            
            return True

    except Exception as err:

        gL.log(gL.ERROR, err)
        return False



if __name__ == "__main__":
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-test', action='store_true', default=False,
                    dest='test',
                    help='Decide se il run è di test')
    parser.add_argument('-url', action='store', default='',
                    dest='testurl',
                    help="Esamina solo l'url")

    args = parser.parse_args()
    gL.testurl = args.testurl    
    if args.test:
        gL.testrun = True
        gL.Dsn = gL.DsnTest
        print("RUN DI TEST!!!!")
    else:
        gL.testrun = False
        gL.Dsn = gL.DsnProd
        print("RUN EFFETTIVO")
    
    # main
    
    rc = main(gL.Dsn)
    if not rc:
        gL.log(gL.ERROR, "Run terminato in modo errato" + str(rc))    
    else:
        gL.log(gL.INFO, 'FINE DEL RUN!') 
    
    # chiudi DB
    gL.CloseConnectionMySql()
    gL.CloseConnectionSqlite()
