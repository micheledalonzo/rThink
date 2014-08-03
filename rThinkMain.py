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


from lxml import html
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
import rThinkParse
work_queue = collections.deque()

def ProcessLogic(country, assettype, source, starturl, pageurl, refresh, rundate, runlogid):    
    gL.log(gL.DEBUG)

    #   build work queue
    rc = gL.sql_RunLogCreate(source, assettype, country, refresh, gL.wait, starturl, pageurl)
    rc = gL.sql_RunLogUpdateStart(country, assettype, source, starturl, pageurl) #lo faccio due volte? se da restart si...
    gL.sql_Queue(country, assettype, source, starturl, pageurl)
    work_queue.append(pageurl)

    while len(work_queue):
        pageurl = work_queue.popleft()            
        msg ="%s - %s" % ("PAGINATE", pageurl)
        gL.log(gL.DEBUG, msg)
        page = rThinkParse.ReadPage(pageurl)
        if page is not None:
            # inserisce la pagina da leggere nel runlog
            rc = gL.sql_RunLogUpdateStart(country, assettype, source, starturl, pageurl)
            # legge la pagina lista, legge i link alle pagine degli asset e li inserisce nella queue
            rc = rThinkParse.DriveParseQueue(country, assettype, source, starturl, pageurl, page)
            # aggiorna il log del run con la data di fine esame della pagina
            gL.sql_RunLogUpdateEnd(country, assettype, source, starturl, pageurl)
            # legge la prossima pagina lista                
            new_pag = rc = rThinkParse.DriveParseNextPage(pageurl, page)
            #new_pag = globals()[gL.nxpage_fn](pageurl, page)    
            if new_pag:
                gL.sql_Queue(country, assettype, source, starturl, new_pag)    # inserisce nella coda
                work_queue.append(new_pag)
                gL.sql_RunLogCreate(source, assettype, country, refresh, gL.wait, starturl, new_pag)
            gL.cSql.commit()
    return

def ProcessLogicRefresh(country, assettype, source, row, starturl):
       
    pageurl  = row['pageurl']
    asseturl = row['asseturl']
    name     = row['nome']
    msg ="%s - %s" % ("PARSE", asseturl)
    gL.log(gL.DEBUG, msg)

    # parse la pagina lista e leggi le singole pagine degli asset
    rc = rThinkParse.DrivePageParse(country, assettype, source, starturl, asseturl, name)  
    if rc:
        gL.sql_Queue(country, assettype, source, starturl, pageurl, asseturl, name) # scrivo nella coda che ho finito
    return

def RunRestart(runid):
    gL.log(gL.DEBUG)

    gL.restart = True

    # da runlog leggo le righe con chiave source/asset/country con data di start massima e data di end nulla
    sql = "SELECT SourceId, AssetTypeId, CountryId, Max(RunLog.RunStart) AS MaxDiRunStart, SourceId, CountryId, AssetTypeId, StartUrl, Wait, First(Pageurl) AS PrimoDiPageurl, RunId \
           FROM RunLog WHERE (RunId =" + str(runid) + " and (RunEnd) Is Null) GROUP BY SourceId, AssetTypeId, CountryId, SourceId, CountryId, AssetTypeId, StartUrl, Wait, RunId ORDER BY Max(RunStart) DESC"
    gL.cSql.execute(sql)
    logs = gL.cSql.fetchall()   # l'ultima
    if not logs:
        return "normal"

    for log in logs:
        source      = log['sourceid']
        assettype   = log['assettypeid']
        country     = log['countryid']
        starturl    = log['starturl']
        pageurl     = log['primodipageurl']
        gL.wait     = log['wait']
        gL.RunId    = log['runid']
        gL.RunLogId = log['runlogid']
        # per prendere i valori di esecuzione leggo la riga tabella drive corrispondente al run
        gL.cSql.execute("select * from qdriverun where CountryId= ? and AssetTypeId = ? and SourceId  = ?", (country, assettype, source))
        drive = gL.cSql.fetchone()   # l'ultima
        if not drive:
            msg ="%s - %s %s %s" % ("Non ho trovato la riga di Drive per ", source, assettype, country)
            gL.log(gL.ERROR, msg)
            return False
        
        gL.assetbaseurl = drive['drivebaseurl']  # il baseurl per la tipologia di asset
        language = drive['countrylanguage']  # lingua
        country = drive['countryid']  # paese
        source = drive['sourceid']
        assettype = drive['assettypeid']
        gL.sourcebaseurl = drive['sourcebaseurl']
        refresh = drive['refresh']
        sourcename = drive['sourcename']
        gL.currency = drive['countrycurr']
        assettypeename = drive['assettypedesc']
        rundate = drive['rundate']
        rundate_end = drive['rundate_end']
        suffissofunzioni = drive['suffissofunzioni']
        # nomi dinamici delle funzioni
        gL.queue_fn  = "Queue"    + suffissofunzioni
        gL.nxpage_fn = "Nextpage" + suffissofunzioni
        gL.parse_fn  = "Parse"    + suffissofunzioni

        gL.wait = drive['wait']
        if gL.wait is None:
            gL.wait = 0
        
        # se nel log non ho pageurl la imposto come starturl
        if pageurl == None:
            pageurl = starturl
        # stampo i parametri di esecuzione
        gL.log(gL.INFO)
        msg=('RUN: %s SOURCE: %s ASSET: %s COUNTRY: %s REFRESH: %s RESTART: %s' % (gL.RunId, sourcename, assettypeename, country, refresh, gL.restart))
        gL.log(gL.INFO, msg)

        # controllo la congruenza
        if not gL.OkParam():
            return False
        
        if language == 'ITA': locale.setlocale(locale.LC_ALL, '')
        
        rc = gL.sql_RunLogUpdateStart(country, assettype, source, starturl, pageurl)
        ProcessLogic(country, assettype, source, starturl, pageurl, refresh, rundate, runid)
    
        # ---------------- cerco di capire dove ero arrivato con il precedente run
        gL.cSql.execute("select * from runlog where RunId = ? and SourceId = ? and AssettypeId = ? and CountryId = ?", (gL.RunId, source, assettype, country))
        rest = gL.cSql.fetchall()
        for res in rest:
            res_starturl = res['starturl']
            res_pageurl = res['pageurl']
            if not res_pageurl:
                res_pageurl = res_starturl
            # leggo dalla coda tutti i link che non ho ancora esaminato        
            gL.cSql.execute("SELECT * FROM Queue where countryid = ? and assetTypeId = ? and StartUrl = ?  \
                                                    and SourceId = ? and PageUrl = ? and rundate = ? and asseturl <> '' ", \
                                                        (country, assettype, starturl, source, res_pageurl, rundate))
            rows = gL.cSql.fetchall()
            for row in rows:
                rc = ProcessLogicRefresh(country, assettype, source, row, starturl)

    rc = gL.sql_RunLogUpdateEnd(country, assettype, source, starturl, pageurl)
    gL.cSql.commit()

    return True

def RunNormale():
    gL.log(gL.DEBUG)
    
    # setto il runid
    gL.RunId = gL.sql_RunId("START")
    gL.sql_UpdDriveRun("START")

    #   Leggo la tabella guida per ogni sorgente, paese, tipo
    sql = "SELECT * FROM QDriveRun where Drive.Active = True"
    gL.cSql.execute(sql)
    drives = gL.cSql.fetchall()
    for drive in drives:
        gL.assetbaseurl = drive['drivebaseurl']  # il baseurl per la tipologia di asset
        language = drive['countrylanguage']  # lingua
        country = drive['countryid']  # paese
        source = drive['sourceid']
        assettype = drive['assettypeid']
        gL.sourcebaseurl = drive['sourcebaseurl']       
        refresh = drive['refresh']
        sourcename = drive['sourcename']
        gL.currency = drive['countrycurr']
        assettypeename = drive['assettypedesc']
        rundate = drive['rundate']
        rundate_end = drive['rundate_end']
        suffissofunzioni = drive['suffissofunzioni']
        starturl = drive['starturl']     
        pageurl = starturl
        gL.wait = drive['wait']
        if gL.wait is None:
            gL.wait = 0
        
        # nomi dinamici delle funzioni
        gL.queue_fn  = "Queue" + suffissofunzioni
        gL.nxpage_fn = "Nextpage" + suffissofunzioni
        gL.parse_fn  = "Parse" + suffissofunzioni
 
        # stampo i parametri di esecuzione
        msg=('RUN: %s SOURCE: %s ASSET: %s COUNTRY: %s REFRESH: %s RESTART: %s' % (gL.RunId, sourcename, assettypeename, country, refresh, gL.restart))
        gL.log(gL.INFO, msg)
        # controllo la congruenza
        if not gL.OkParam():
            return False
        if language == 'ITA': locale.setlocale(locale.LC_ALL, '')
        # se richiesto cancello e ricreo la coda, ma solo per le righe dipendenti dallo starturl
        if not refresh:
            sql = ("Delete * from queue where sourceid = " + str(source) + " and AssetTypeId = " + str(assettype) + " and CountryId = '" + country + "' and StartUrl = '" + starturl + "'")
            gL.cSql.execute(sql)
            gL.cSql.commit()

        # ---------------- se non richiesto refresh faccio prima la paginazione, poi rileggo le pagine degli asset
        if not refresh:   
            ProcessLogic(country, assettype, source, starturl, starturl, refresh, rundate, gL.RunId)
            rc = gL.sql_RunLogUpdateEnd(country, assettype, source, starturl, pageurl)
            gL.cSql.commit()
        # ---------------- leggo dalla coda i link che sono correlati allo starturl attivo e che sono attivi         
        gL.cSql.execute("SELECT * FROM Queue where countryid = ? and assetTypeId = ? and StartUrl = ? and SourceId = ? and asseturl <> ''", (country, assettype, starturl, source))
        # tutti i link presenti nella tabella starturl (tutte le pagine lista)
        rows = gL.cSql.fetchall()
        for row in rows:
            rc = ProcessLogicRefresh(country, assettype, source, row, starturl)

    return True

def main():
    #---------------------------------------------- M A I N ----------------------------------------------------------------------------------
    # apri connessione e cursori, carica keywords in memoria
    gL.log(gL.DEBUG, "MAIN")
    gL.log(gL.INFO, 'INIZIO DEL RUN')
    gL.SqLite, gL.C = gL.OpenConnectionSqlite()
    gL.MySql, gL.Cursor = gL.OpenConnectionMySql()
    
    rc = gL.sql_CreateMemTableKeywords()
    rc = gL.sql_CopyKeywordsInMemory()
    #
    # MAIN 
    #
    refresh = False
    #source = False
    #queuerebuild = False

    # determino se devo restartare - prendo l'ultimo record della tabella run
    sql = "SELECT RunId, StartDate, EndDate FROM Run GROUP BY RunId, StartDate, EndDate ORDER BY Run.RunId DESC"
    gL.cSql.execute(sql)
    check = gL.cSql.fetchone()
    if check:
        runid = check['runid']
        enddate = check['enddate']
        startate = check['startdate']
        if enddate is None or enddate == '':
            gL.restart = True
    else:
        gL.restart = False

    if gL.restart:
        rest = RunRestart(runid)
        if not rest:   # non ho scri
            sys.exit()
        if rest == "normal":
            gL.restart = False

    if not gL.restart or rest == "normal":   # REST=NORMAL se il run è iniziato ma non ha scritto nessuna riga di log
        rc = RunNormale()
        if not rc:   # controllo parametri non valido
            sys.exit()
        else:
            #chiudo le tabelle dei run
            rc = gL.sql_RunId("END")
            rc = gL.sql_UpdDriveRun("END")
            gL.cSql.commit()

    # decido il nome univoco dell'asset e i puntatori relativi
    #rc = gL.StdSourceAsset(country, source, assettype, True)


    # chiudi DB
    gL.CloseConnectionMySql()
    gL.CloseConnectionSqlite()

    gL.log(gL.INFO, 'FINE DEL RUN!')


if __name__ == "__main__":
    main()