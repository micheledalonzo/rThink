# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python

# tutti gli accessi SQL 
import sqlite3
import pypyodbc
import rThinkGbl as gL
from rThinkFunctions import *

# database flag YES/NO
YES = -1
NO = 0


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

def CloseConnectionSqlite():
 
    if gL.SqLite:
        gL.SqLite.close()
    return True

def sql_RestartUrl(country, assettype, source, rundate, starturl="", pageurl=""):
    
    # se richiesto il restart prendo l'ultimo record creato nel run precedente
    gL.cSql.execute( ("SELECT StartUrl, PageUrl, InsertDate FROM Queue where \
                        countryid = ? and assetTypeId = ? and SourceId = ? and RunDate = ? and StartUrl is NOT NULL and PageUrl IS NOT NULL and AssetUrl='' \
                        order by insertdate desc"),\
                        (country, assettype, source, rundate) )
    a = gL.cSql.fetchone()
    
    if a:
        starturl = a['starturl']
        pageurl = a['pageurl']
        return starturl, pageurl
    else:
        return False

def sql_ManageTag(cur_asset_id, tag, classify):
    # ===========================================================================================================================================
    # cancella e riscrive la classificazione dell'asset
    # ===========================================================================================================================================
    
    if tag:
        sql = "Delete * from SourceAssetTag where SourceAssetId = " + str(cur_asset_id) + " and TagName = '" + str(classify) + "'"
        gL.cSql.execute(sql)
        for i in tag:
            i = StdCar(i)
            if len(i) < 2:
                continue
            gL.cSql.execute("Insert into SourceAssetTag(SourceAssetId, TagName, Tag) Values (?, ?, ?)", (cur_asset_id, classify, i))

    return True

def sql_ManagePrice(cur_asset_id, PriceList, currency):
    # ===========================================================================================================================================
    # cancella e riscrive la classificazione dell'asset
    # ===========================================================================================================================================
    
    if PriceList:
        sql = "Delete * from SourceAssetPrice where SourceAssetId = " + str(cur_asset_id) + " and PriceDate = #" + gL.runnow + "#"
        gL.cSql.execute(sql)
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
            gL.cSql.execute("Insert into SourceAssetPrice(SourceAssetId, PriceDate, PriceCurr, PriceFrom, PriceTo, PriceAvg) Values (?, ?, ?, ?, ?, ?)", (cur_asset_id, gL.runnow, PriceCurr, PriceFrom, PriceTo, PriceAvg))

    return True

def sql_Queue(country, assettype, source, starturl, pageurl, asseturl="", name=""):
    # 
    # aggiunge url alla coda
    # 
    if asseturl:
        gL.cSql.execute("SELECT * FROM Queue where Starturl = ? and Pageurl = ? and AssetUrl = ?", (starturl, pageurl, asseturl))
    else:
        gL.cSql.execute("SELECT * FROM Queue where Starturl = ? and Pageurl = ?", (starturl, pageurl))
    row = gL.cSql.fetchone()
    now = SetNow()
    if row:   
        gL.cSql.execute("Update queue set UpdateDate = ? where  Starturl = ? and Pageurl = ? and AssetUrl = ? and Nome = ?", (now, starturl, pageurl, asseturl, name))
    else:
        gL.cSql.execute("Insert into queue(CountryId, AssetTypeId, SourceId, StartUrl, PageUrl, AssetUrl, RunDate, InsertDate, UpdateDate, Nome) \
                                          Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", \
                                         (country, assettype, source, starturl, pageurl, asseturl, gL.runnow, now, now, name))
    return True

def sql_UpdSourceAddress(cur_asset_id, AddrList):
    
    AddrStreet = ""
    AddrCity = ""
    AddrCounty = ""
    AddrZIP = ""
    AddrPhone = ""
    AddrWebsite = ""
    AddrLat = 0
    AddrLong = 0
    AddrRegion = ""
    FormattedAddress = ""

    sql = "Select * from SourceAsset where SourceAssetId = " + str(cur_asset_id)
    gL.cSql.execute(sql)
    CurSourceAsset = gL.cSql.fetchone()
    if CurSourceAsset:
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
                    
        if CurSourceAsset['AddrValidated'] != YES:    
            rc, AddrStreet, AddrCity, AddrZIP, AddrLat, AddrLong, AddrRegion, AddrCounty, FormattedAddress = StdAddress(AddrStreet, AddrZIP, AddrCity, AddrCounty)
        if rc:
            AddrValidated = YES
        else:                
            AddrValidated = NO

        # controlla se ci
        # controlla se ci sono dati cambiati
        if (       AddrStreet != CurSourceAsset['AddrStreet'] or AddrCity != CurSourceAsset['AddrCity']     \
                or AddrCounty != CurSourceAsset['AddrCounty'] or AddrZIP != CurSourceAsset['AddrZIP']       \
                or AddrPhone != CurSourceAsset['AddrPhone']   or AddrLat != CurSourceAsset['AddrLat']       \
                or AddrLong != CurSourceAsset['AddrLong']     or AddrRegion != CurSourceAsset['AddrRegion'] \
                or FormattedAddress!= CurSourceAsset['FormattedAddress']):
            gL.cSql.execute("Update SourceAsset set  AddrStreet=?, AddrCity=?, AddrZip=?, AddrCounty=?, \
                                                    AddrPhone=?,  AddrLat=?,  AddrLong=?, AddrWebsite=?, \
                                                    FormattedAddress=?, AddrRegion=?, AddrValidated=?  \
                                              where SourceAssetId=?",  \
                                                  ( AddrStreet,   AddrCity,   AddrZIP,   AddrCounty,   \
                                                    AddrPhone,    AddrLat,    AddrLong,  AddrWebsite,  \
                                                    FormattedAddress, AddrRegion,  AddrValidated,       \
                                                    cur_asset_id))
    return True

def sql_UpdSourceAsset(source, assettype, country, name, link):

    global CurAssetLastReviewDate
    # 
    # se esiste aggiorna nome e data, se non esiste lo inserisce
    # 
    print('Asset: ', name.encode('utf-8'))
    link_sql = link.replace("'", "''")  # per evitare errori sql in caso di apostrofo nel nome
    sql = "Select * from SourceAsset where Url = '" + link_sql + "'"
    gL.cSql.execute(sql)
    CurSourceAsset = gL.cSql.fetchone()
    if CurSourceAsset:
        # a = gL.cSql.fetchone()
        cur_asset_id = int(CurSourceAsset[0])
        CurAssetLastReviewDate = CurSourceAsset['lastreviewdate']
        if name != CurSourceAsset['name']:
            gL.cSql.execute("Update SourceAsset set Name=?, UpdateDate=? where SourceAssetId=?", (name, now, cur_asset_id))
    else:
        gL.cSql.execute("Insert into SourceAsset(SourceId, AssetTypeId, Country, Url, Name, InsertDate, UpdateDate, Active) Values (?, ?, ?, ?, ?, ?, ?, ?)", (source, assettype, country, link, name, gL.runnow, gL.runnow, YES))
        gL.cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
        CurSourceAsset = gL.cSql.fetchone()
        cur_asset_id = int(CurSourceAsset[0])
        CurAssetLastReviewDate = gL.runnow

    return cur_asset_id, CurAssetLastReviewDate

def sql_ManageReview(cur_asset_id, nreview, punt):
    
    if int(nreview) == 0 and punt == 0:
        pass
    else:
        gL.cSql.execute("Insert into SourceAssetEval(SourceAssetId, EvalDate, EvalPoint, EvalNum) Values (?, ?, ?, ?)", (cur_asset_id, gL.runnow, punt, nreview))
    return

def sql_CreateMemTableWasset():
    cmd_create_table = """CREATE TABLE if not exists
                wasset (
                        country     STRING,
                        sourceasset INTEGER,
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
    return

def sql_CreateMemTableAssetmatch():
    cmd_create_table = """CREATE TABLE if not exists
            assetmatch (
                        sourceasset INTEGER,
                        name        STRING,
                        street      STRING,
                        city        STRING,
                        cfrsourceasset    INTEGER,
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
    return

def sql_CreateMemTableKeywords():
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
    return

def sql_InsertAsset(AssetCountry, AssetTypeId, AssetName, SourceId, InsertDate, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrLat, AddrLong, FormattedAddress, AddrRegion, AddrValidated):
    # 
    # inserisce asset con info standardizzate
    # 
    gL.cSql.execute("Insert into Asset(AssetCountry, AssetTypeId, AssetName, SourceId, InsertDate, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrLat, AddrLong, FormattedAddress, AddrRegion, AddrValidated) Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                                     (AssetCountry, AssetTypeId, AssetName, SourceId, InsertDate, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrLat, AddrLong, FormattedAddress, AddrRegion, AddrValidated))                                      
    gL.cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
    asset= gL.cSql.fetchone()
    assetid = int(asset[0])
    return True, assetid

def sql_dump_Assetmatch():
    now = SetNow()
    # dump della tabella in memoria su db
    # dalla tabella assetmach mantengo solo i record che a parità di chiave hanno punteggio più alto
    sql = "SELECT * from assetmatch order BY sourceasset, gblratio"
    gL.cLite.execute(sql)
    cur = gL.cLite.fetchall()
    for a in cur:
        sourceasset = a[0]
        cfrsourceasset = a[4]
        nameratio = a[8]
        cityratio = a[9]
        streetratio = a[10]
        gblratio = a[11]
        gL.cSql.execute("insert into assetmatch (insertdate, sourceasset, cfrsourceasset, nameratio, streetratio, cityratio, gblratio) values(?, ?, ?, ?, ?, ?, ?)",\
                                               (now, sourceasset, cfrsourceasset, nameratio, streetratio, cityratio, gblratio))
    return True

def sql_CopyAssetInMemory(countryid=None, sourceid=None, assettypeid=None):
    
    if sourceid is not None and countryid is not None and assettypeid is not None:
        sql = "Select * from SourceAsset where country = '" + countryid + "' and  sourceid = " + str(sourceid) + " and assettypeid = " + str(assettypeid) + " order by Name"
    else:
        sql = "Select * from SourceAsset order by Name"
    
    gL.cSql.execute(sql)
    wassets = gL.cSql.fetchall()
    # ratio = 0

    gL.count = 0
    for wasset in wassets:
        country = wasset['country']
        sourceasset = wasset['sourceassetid']
        assettype = wasset['assettypeid']
        assetid = wasset['assetid']
        name = wasset['name']
        source = wasset['sourceid']
        street = wasset['addrstreet']
        city = wasset['addrcity']
        wzip = wasset['addrzip']
        county = wasset['county']
        namesimple = wasset['namesimple']
        namesimply = wasset['namesimplified']
        gL.cLite.execute("insert into wasset (country, sourceasset, assettype, assetid, name, source, street, city, zip, county, namesimple, namesimplified) \
                                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                      (country, sourceasset, assettype, assetid, name, source, street, city, wzip, county, namesimple, namesimply))
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