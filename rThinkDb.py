# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python

# tutti gli accessi SQL 
import sqlite3
import pypyodbc
import rThinkGbl as gL
#from rThinkFunctions import *

def OpenConnectionMySql():
    if not gL.MySql:
        gL.MySql = pypyodbc.connect('DSN=rThink')
        gL.cSql = gL.MySql.cursor()
    return gL.MySql, gL.cSql

def OpenConnectionSqlite():
    if not gL.SqLite:
        gL.SqLite = sqlite3.connect(':memory:')
        gL.cLite = gL.SqLite.cursor()
    
    return gL.SqLite, gL.cLite

def CloseConnectionMySql():
    
    if gL.MySql:
        gL.MySql.close()
    return True

def LoadProxyList():
    gL.cSql.execute("Select * from RunProxies where Active = ?", ([gL.YES]) )
    proxies = gL.cSql.fetchall()
    if len(proxies) == 0:       
        return False
    for proxy in proxies:
        gL.Proxies.append(proxy[0])
    return True


def CloseConnectionSqlite():
 
    if gL.SqLite:
        gL.SqLite.close()
    return True

def sql_UpdDriveRun(startend):
    
    if startend == "START":
        gL.cSql.execute("Update Drive set RunDate = ? where active = True", ([gL.RunDate]))
    if startend == "END":        
        gL.cSql.execute("Update Drive set RunDate_end = ? where active = True", ([gL.SetNow()]))
    gL.cSql.commit()
    gL.log(gL.DEBUG, "Commit")

def sql_RunId(startend):
    runid = 0
    if startend == "START":
        gL.cSql.execute("Insert into Run (Start) Values (?)", ([gL.RunDate]))
        gL.cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
        run = gL.cSql.fetchone()
        runid = run[0]
    if startend == "END":
        gL.cSql.execute("Update Run set End = ? where RunId = ? ", (gL.SetNow(), gL.RunId)) 
        runid = gL.RunId
    gL.cSql.commit()
    gL.log(gL.DEBUG, "Commit")
    return runid

def sql_PagesCreate(source, assettype, country, starturl, pageurl):
    if pageurl == None or pageurl == '':
        pageurl = starturl
    
    try:
        a = gL.cSql.execute("Update Pages set Start = ?, End = 0 where source = ? and assettype = ? and country = ? and starturl = ? and pageurl = ?", \
                                    (gL.SetNow(), source, assettype, country, starturl, pageurl))
        if a.rowcount == 0:
            # inserisci il record
        
            gL.cSql.execute("Insert into Pages(Source, AssetType, Country, StartUrl, Pageurl, RunId) \
                            values (?,?,?,?,?,?)", \
                            (source, assettype, country, starturl, pageurl, gL.RunId))    
    except Exception as err:

        gL.log(gL.ERROR, str(source)+ str(assettype) + country + starturl + pageurl)
        gL.log(gL.ERROR, err)
    return True

def sql_PagesUpdStatus(startend, country, assettype, source, starturl, pageurl):
    try:
        if startend == "START":       
            gL.cSql.execute("Update Pages set Start = ?, End = 0 where source = ? and assettype = ? and country = ? and starturl = ? and pageurl = ?", \
                                                (gL.SetNow(), source, assettype, country, starturl, pageurl))
        if startend == "END":
            gL.cSql.execute("Update Pages set End   = ? where source = ? and assettype = ? and country = ? and starturl = ? and pageurl = ?", \
                                        (gL.SetNow(), source, assettype, country, starturl, pageurl))
    except Exception as err:

        gL.log(gL.ERROR, startend, str(source)+ str(assettype) + country + starturl + pageurl)
        gL.log(gL.ERROR, err)

    return True

def sql_RestartUrl(country, assettype, source, rundate, starturl="", pageurl=""):
    gL.log(gL.DEBUG)
    # se richiesto il restart prendo l'ultimo record di paginazione creato nel run precedente
    gL.cSql.execute( ("SELECT StartUrl, PageUrl, max(InsertDate) FROM Queue where \
                        country = ? and assetTypeId = ? and Source = ? and RunDate = ? and StartUrl is NOT NULL and PageUrl IS NOT NULL and AssetUrl='' \
                        group by starturl, pageurl order by InsertDate desc"),\
                        (country, assettype, source, rundate) )
    a = gL.cSql.fetchone()
    
    if a:
        starturl = a['starturl']
        pageurl = a['pageurl']
        return starturl, pageurl
    else:
        return False

def AssetTag(Asset, tag, tagname):
     
    try:
        # cancella e riscrive la classificazione dell'asset     
        if len(tag)>0:
            tag = list(set(tag))     # rimuovo duplicati dalla lista        
            gL.cSql.execute("Delete * from AssetTag where Asset = ? and TagName = ?", (Asset, tagname))
            for i in tag:
                i = gL.StdCar(i)
                if len(i) < 2:
                    continue
                gL.cSql.execute("Insert into AssetTag(Asset, TagName, Tag) Values (?, ?, ?)", (Asset, tagname, i))

        return True

    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False


def AssetReview(Asset, r):
    try:
        if len(r) == 0:
            return True
        gL.cSql.execute("Delete * from AssetReview where Asset = ?", ([Asset]))
        for a in r:
            nreview = int(a[0])
            punt    = int(a[1])
            gL.cSql.execute("Insert into AssetReview(Asset, EvalPoint, EvalNum) Values (?,?,?)", (Asset, punt, nreview))
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def AssetPrice(Asset, PriceList, currency):
     
    # cancella e riscrive la classificazione dell'asset 
 
    if len(PriceList)>0:                
        PriceCurr = ""
        PriceFrom = 0
        PriceTo = 0
        PriceAvg = 0
        for i in PriceList:
            if i[0] == 'PriceCurr':
                PriceCurr = i[1]
            if i[0] == 'PriceFrom':
                PriceFrom = i[1]
            if i[0] == 'PriceTo':
                PriceTo = i[1]
            if i[0] == 'PriceAvg':
                PriceAvg = i[1]
        if PriceCurr == '':
            PriceCurr = currency
        if PriceFrom == 0 and PriceTo == 0 and PriceAvg == 0:
            pass
        else:
            gL.cSql.execute("Delete * from AssetPrice where Asset = ? ", ([Asset]))
            gL.cSql.execute("Insert into AssetPrice(Asset, PriceCurrency, PriceFrom, PriceTo, PriceAvg) Values (?, ?, ?, ?, ?)", (Asset, PriceCurr, PriceFrom, PriceTo, PriceAvg))

    return True

def Enqueue(country, assettype, source, starturl, pageurl, asseturl, name):
    try:    
        # inserisce un url alla coda oppure lo aggiorna con la data del parsing e col numero del run     
        if pageurl is None or pageurl == '':
            pageurl = starturl
        a = gL.cSql.execute("Update queue set Start=0, End=0, InsertDate=?, RunId=? where Country=? and AssetType=? and Source=? and Starturl=? and Pageurl=? and AssetUrl=?", \
                                            (gL.SetNow(), gL.RunId, country, assettype, source, starturl, pageurl, asseturl))
        if a.rowcount == 0:
            gL.cSql.execute("Insert into queue(Country, AssetType, Source, StartUrl, PageUrl, AssetUrl, InsertDate, Nome, RunId) \
                                                Values (?, ?, ?, ?, ?, ?, ?, ?, ?)", \
                                                (country, assettype, source, starturl, pageurl, asseturl, gL.SetNow(), name, gL.RunId))

            return True
    except Exception as err:
        gL.log(gL.ERROR, str(source)+ str(assettype) + country + starturl + pageurl + asseturl + name)
        gL.log(gL.ERROR, err)
        return False
    

def QueueStatus(startend, country, assettype, source, starturl, pageurl, asseturl):
    try:
        if startend == "START":
            gL.cSql.execute("Update queue set Start=?, End=0, RunId=? where Country=? and AssetType=? and Source=? and Starturl=? and Pageurl=? and AssetUrl=?", \
                                              (gL.SetNow(), gL.RunId, country, assettype, source, starturl, pageurl, asseturl))
        if startend == "END":
            gL.cSql.execute("Update queue set End=?, RunId=? where Country=? and AssetType=? and Source=? and Starturl=? and Pageurl=? and AssetUrl=?", \
                                              (gL.SetNow(), gL.RunId, country, assettype, source, starturl, pageurl, asseturl))
    
    except Exception as err:        
        gL.log(gL.ERROR, (str(source)+ str(assettype) + country + starturl + pageurl + asseturl), err)

    return True


def AssetOpening(Asset, orario):
    gL.cSql.execute("Delete * from AssetOpening where Asset = ? ", ([Asset]))
    for j in orario:
        gL.cSql.execute("Insert into AssetOpening(Asset, WeekDay, OpenFrom, OpenTo) Values (?, ?, ?, ?)", \
                (Asset, j[0], j[1], j[2]))
    #orario.append([dayo, fro, to])
    return True

def AssettAddress(Asset, AddrList):

    try:
        AddrStreet = ""
        AddrCity = ""
        AddrCounty = ""
        AddrZIP = ""
        AddrPhone = ""
        AddrPhone1 = ""
        AddrWebsite = ""
        AddrLat = 0
        AddrLong = 0
        AddrRegion = ""
        Address = ""
        FormattedAddress = ""
        AddrValidated= ""

        if 'AddrValidated' in AddrList and AddrList['AddrValidated']:
            AddrValidated = AddrList['AddrValidated']
        else:
            AddrValidated = gL.NO           
        if 'Address' in AddrList and AddrList['Address']:
            Address = AddrList['Address']
        if 'AddrStreet' in AddrList and AddrList['AddrStreet']:
            AddrStreet = AddrList['AddrStreet']
        if 'AddrCity' in AddrList and AddrList['AddrCity']:
            AddrCity = AddrList['AddrCity']
        if 'AddrCounty' in AddrList and AddrList['AddrCounty']:
            AddrCounty = AddrList['AddrCounty']
        if 'AddrZIP' in AddrList and AddrList['AddrZIP']:
            AddrZIP = AddrList['AddrZIP']
        if 'AddrPhone' in AddrList and AddrList['AddrPhone']:
            AddrPhone = AddrList['AddrPhone']
        if 'AddrPhone1' in AddrList and AddrList['AddrPhone1']:
            AddrPhone1 = AddrList['AddrPhone1']
        if 'AddrWebsite' in AddrList and AddrList['AddrWebsite']:
            AddrWebsite = AddrList['AddrWebsite'].lower()
        if 'AddrLat' in AddrList and AddrList['AddrLat']:
            AddrLat = AddrList['AddrLat']
        if 'AddrLong' in AddrList and AddrList['AddrLong']:
            AddrLong = AddrList['AddrLong']
        if 'AddrRegion' in AddrList and AddrList['AddrRegion']:
            AddrRegion = AddrList['AddrRegion']
        if 'FormattedAddress' in AddrList and AddrList['FormattedAddress']:
            FormattedAddress = AddrList['FormattedAddress']            
        if 'Address' in AddrList and AddrList['Address']:
            Address = AddrList['Address']            

        gL.cSql.execute("Select * from AssetAddress where Asset = ?", ([Asset]))
        CurAsset = gL.cSql.fetchone()
        if CurAsset:          
            # controlla se ci sono dati cambiati
            if (       AddrStreet != CurAsset['AddrStreet'] or AddrCity        != CurAsset['AddrCity']          \
                    or AddrCounty != CurAsset['AddrCounty'] or AddrZIP         != CurAsset['AddrZIP']           \
                    or AddrPhone  != CurAsset['AddrPhone']  or AddrLat         != CurAsset['AddrLat']           \
                    or AddrLong   != CurAsset['AddrLong']   or AddrRegion      != CurAsset['AddrRegion']        \
                    or AddrPhone1 != CurAsset['AddrPhone1'] or FormattedAddress!= CurAsset['FormattedAddress']  \
                    or Address != CurAsset['Address']    ):
                    gL.cSql.execute("Update AssetAddress set  \
                                                            AddrStreet=?, AddrCity=?, AddrZip=?, AddrCounty=?,                  \
                                                            AddrPhone=?,  AddrPhone1=?, AddrLat=?,  AddrLong=?, AddrWebsite=?,  \
                                                            FormattedAddress=?, AddrRegion=?, AddrValidated=?, Address=?        \
                                                      where Asset=?",  \
                                                          ( AddrStreet,   AddrCity,   AddrZIP,   AddrCounty,   \
                                                            AddrPhone,    AddrPhone1,    AddrLat,    AddrLong,  \
                                                            AddrWebsite,  FormattedAddress, AddrRegion,  AddrValidated, Address, \
                                                            Asset))
            else:
                pass
        else:
            gL.cSql.execute("Insert into AssetAddress(AddrStreet, AddrCity,   AddrZip, AddrCounty,               \
                                                      AddrPhone,  AddrPhone1, AddrLat,  AddrLong, AddrWebsite, \
                                                      FormattedAddress, AddrRegion, AddrValidated, Address, Asset)    \
                                                      Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",         \
                                                     (AddrStreet, AddrCity,   AddrZIP,  AddrCounty,            \
                                                      AddrPhone,  AddrPhone1, AddrLat,  AddrLong,  AddrWebsite,\
                                                      FormattedAddress, AddrRegion, AddrValidated, Address, Asset))

    except Exception as err:
        #gL.log(gL.DEBUG, str(Asset))
        gL.log(gL.ERROR, "Asset:"+str(Asset), err)
        return False

    return True

def SqlSaveContent(url, content):
    CurContent = ''
    gL.log(gL.DEBUG)
    sql = "Select * from AssetContent where Url = '" + url + "'"
    gL.cSql.execute(sql)
    check = gL.cSql.fetchone()
    try:
        if check:
            CurContent = check['content']
            if CurContent != content:
                gL.cSql.execute("Update AssetContent set Content=?, RunId=? where url=?", (content, gL.RunId, url))
        else:
            gL.cSql.execute("Insert into AssetContent(Url, Content, RunId) Values (?, ?, ?)", \
                    (url, content, gL.RunId))

    except Exception as err:

        gL.log(gL.ERROR, err)
        return False
 
    return True


def Asset(country, assettype, source, name, url, GooglePid=''):
    
    try:    
        msg = "%s %s - %s" % ('Asset:', gL.N_Ass, name.encode('utf-8'))
        gL.log(gL.INFO, msg)

        NameSimple, NameSimplified, tag, cuc = gL.ManageName(name, country, assettype)

        if GooglePid == '':
            gL.cSql.execute("Select * from Asset where Url = ?", ([url]))
            CurAsset = gL.cSql.fetchone()
        else:
            gL.cSql.execute("Select * from Asset where GooglePid = ?", ([GooglePid]))
            CurAsset = gL.cSql.fetchone()

        # se è gia' presente
        if CurAsset:   
            Asset = int(CurAsset['asset'])       
            if name != CurAsset['name'] or NameSimple != CurAsset['namesimple']:
                gL.cSql.execute("Update Asset set Name=?, NameSimple=?, NameSimplified=?, Update=? where Asset=?", (name, NameSimple, NameSimplified, gL.SetNow(), Asset))
        else:
            gL.cSql.execute( "Insert into Asset(Source, AssetType, Country, Url, Name, NameSimple, NameSimplified, Created, Updated, Active, GooglePid) \
                              Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", \
                            ( source, assettype, country, url, name, NameSimple, NameSimplified, gL.RunDate, gL.SetNow(), gL.YES, GooglePid))
            gL.cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
            a = gL.cSql.fetchone()
            Asset = int(a[0])
    
        rc = AssetTag(Asset, tag, "Tipologia")
        rc = AssetTag(Asset, cuc, "Cucina")
        return Asset

    except Exception as err:

        gL.log(gL.ERROR, err)
        return 0


def UpdateLastReviewDate(Asset, LastReviewDate):
    try:
        # aggiorna la data di ultima recensione
        gL.cSql.execute("select LastReviewDate from Asset where Asset=?", ([Asset]))
        row = gL.cSql.fetchone()
        if len(row) == 0:
            return True
        CurLastReviewDate = row[0]
        if CurLastReviewDate is None or (CurLastReviewDate < LastReviewDate):
            gL.cSql.execute("Update Asset set LastReviewDate=? where Asset=?", (LastReviewDate, Asset))
        return True
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def CreateMemTableWasset():
    try:
        cmd_create_table = """CREATE TABLE if not exists
                    wasset (
                            country     STRING,
                            asset       INTEGER,
                            assettype   INTEGER,
                            assetid     INTEGER,
                            name        STRING,
                            source      INTEGER,
                            street      STRING,
                            city        STRING,
                            zip         STRING,
                            county      STRING,
                            namesimple  STRING,
                            namesimplified  INTEGER
                            );"""
        gL.SqLite.executescript(cmd_create_table)
        return True
    except Exception as err:

        gL.log(gL.ERROR, err)
        return False


def CreateMemTableAssetmatch():
    try:
        cmd_create_table = """CREATE TABLE if not exists
                assetmatch (
                            asset INTEGER,
                            name        STRING,
                            street      STRING,
                            city        STRING,
                            cfrasset    INTEGER,
                            cfrname     STRING,
                            cfrstreet   STRING,
                            cfrcity     STRING,
                            nameratio   FLOATING,
                            cityratio   FLOATING,
                            streetratio FLOATING,
                            gblratio    FLOATING,
                            country     STRING,
                            assettype   INTEGER,
                            source      INTEGER
        );"""
        gL.SqLite.executescript(cmd_create_table)
        return True
    except Exception as err:

        gL.log(gL.ERROR, err)
        return False


def CreateMemTableKeywords():
    try:
        cmd_create_table = """CREATE TABLE if not exists 
                  keywords (
                            assettype   STRING,
                            language    STRING,
                            keyword     STRING,
                            operatore   STRING,
                            tipologia1  STRING,
                            tipologia2  STRING,
                            tipologia3  STRING,
                            tipologia4  STRING,
                            tipologia5  STRING,
                            replacewith STRING,
                            numwords    INTEGER
        );"""
        gL.SqLite.executescript(cmd_create_table)
        return True
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def AAsset(Asset, AssetMatch, AssetRef):
    try:
        if AssetMatch == 0:   # devo inserire me stesso
            gL.cSql.execute("select * from asset where asset = ?", ([Asset]))
             # inserisce asset con info standardizzate     
            gL.cSql.execute("Insert into AAsset (Updated) values (?)" , ([gL.RunDate]))
            gL.cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
            asset = gL.cSql.fetchone()
            AAsset = int(asset[0])
            gL.cSql.execute("Update Asset set AAsset=? where Asset=?", (Asset, Asset))
        else:
            AAsset = AssetRef
            gL.cSql.execute("Update Asset set AAsset=? where Asset=?", (AssetRef, Asset))  # ci metto il record di rif 
        
        return AAsset

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def sql_dump_Assetmatch():
    try:

        now = gL.SetNow()
        # dump della tabella in memoria su db
        # dalla tabella assetmach mantengo solo i record che a parità di chiave hanno punteggio più alto
        sql = "SELECT * from assetmatch order BY asset, gblratio"
        gL.cLite.execute(sql)
        cur = gL.cLite.fetchall()
        for a in cur:
            asset = a[0]
            cfrasset = a[4]
            nameratio = a[8]
            cityratio = a[9]
            streetratio = a[10]
            gblratio = a[11]
            gL.cSql.execute("insert into assetmatch  (insertdate, asset, cfrasset, nameratio, streetratio, cityratio, gblratio) values(?, ?, ?, ?, ?, ?, ?)",\
                                                    (gL.SetNow(), asset, cfrasset, nameratio, streetratio, cityratio, gblratio))
        return True
    except Exception as err:

        gL.log(gL.ERROR, err)
        return False

def sql_CopyAssetInMemory(country=None, source=None, assettype=None):
    gL.log(gL.DEBUG)
    if source is not None and country is not None and assettype is not None:
        sql = "Select * from Asset where country = '" + country + "' and  source = " + str(source) + " and assettype = " + str(assettype) + " order by Name"
    else:
        sql = "Select * from Asset order by Name"
    
    gL.cSql.execute(sql)
    wassets = gL.cSql.fetchall()
    # ratio = 0

    gL.count = 0
    for wasset in wassets:
        country = wasset['country']
        asset = wasset['asset']
        assettype = wasset['assettype']
        assetid = wasset['assetid']
        name = wasset['name']
        source = wasset['source']
        street = wasset['addrstreet']
        city = wasset['addrcity']
        wzip = wasset['addrzip']
        county = wasset['county']
        namesimple = wasset['namesimple']
        namesimply = wasset['namesimplified']
        gL.cLite.execute("insert into wasset (country, asset, assettype, assetid, name, source, street, city, zip, county, namesimple, namesimplified) \
                                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                      (country, asset, assettype, assetid, name, source, street, city, wzip, county, namesimple, namesimply))
    return

def sql_CopyKeywordsInMemory():
    
    gL.cSql.execute("Select * from Assetkeywords order by keyword")
    ks = gL.cSql.fetchall()
    for k in ks:
        assettype   = k['assettype']
        language    = k['language']
        keyword     = k['keyword']
        operatore   = k['operatore']
        tipologia1  = k['tipologia1']
        tipologia2  = k['tipologia2']
        tipologia3  = k['tipologia3']
        tipologia4  = k['tipologia4']
        tipologia5  = k['tipologia5']
        replacewith = k['replacewith']
        kwdnumwords = k['kwdnumwords']
        numwords    = len(keyword.split())
        gL.cLite.execute("insert into keywords (assettype, language, keyword, operatore,tipologia1,tipologia2,replacewith,numwords) values (?, ?, ?, ?, ?, ?, ?, ?)",
                                        (assettype, language, keyword, operatore,tipologia1,tipologia2,replacewith,numwords))
    return