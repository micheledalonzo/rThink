# -*- coding: cp1252 -*-.
import difflib
import pypyodbc
import datetime
import re


import rThinkGbl as gL
from rThinkDb import *
from rThinkFunctions import *

# fanne solo limita, di nomi
limita = 50

def LookForName(mode, country, source, assettype, sourceasset, name, city, street):
    #SqLite, c = OpenConnectionSqlite()

    # mode=1 Leggi solo i record con assetid, se ne trovi uno esci
    # mode=0 Leggi i record senza assetid, inserisci tutti quelli trovati
    found = False
    # legge la tabella in memoria degli asset selezionando quelli di source diversi e dello stesso tipo e paese
    if mode == 1:
        # leggo i record con assetid già valorizzato, quindi con un record nella tabella asset
        sql = "Select * from wasset where Source <> " + str(source) + " and AssetType = " + str(assettype) + " and AssetId <> 0  and Country = '" + country + "' order by name"
    else:
        # leggo i record con assetid non ancora valorizzato, quindi con un record nella tabella asset
        sql = "Select * from wasset where Source <> " + str(source) + " and AssetType = " + str(assettype) + " and AssetId= 0 and Country = '" + country + "' order by name"

    gL.cLite.execute(sql)
    cur = gL.cLite.fetchall()
    for wrk in cur:
        cfrsourceasset = wrk[1]
        cfrname = str(wrk[4])
        # cfrsource   = wrk[5]
        cfrcity = str(wrk[7])
        cfrstreet = str(wrk[6])
        # cfrzip      = str(wrk[8])
        # cfrcounty   = str(wrk[9])
        nameratio = streetratio = cityratio = 1
        gblratio = 0; numeri = 0
        if name is not None and cfrname is not None:
            name = name.title()
            cfrname = cfrname.title()
            numeri = numeri + 1
            nameratio = difflib.SequenceMatcher(None, a = name, b = cfrname).ratio()
        if city is not None and cfrcity is not None:
            city = city.title()
            cfrcity = cfrcity.title()
            cityratio = difflib.SequenceMatcher(None, a = city, b = cfrcity).ratio()
            numeri = numeri + 1
        if street is not None  and cfrstreet is not None:
            street = street.title()
            cfrstreet = cfrstreet.title()
            streetratio = difflib.SequenceMatcher(None, a = street, b = cfrstreet).ratio()
            numeri = numeri + 1
        # stampa ="Name:" + name + " with:" + cfrname
        # print("   --->", stampa[:50].encode('UTF-8'), end='\r')
        if nameratio > 0.8:
            # peso i match
            namepeso = 1.5
            streetpeso = 1
            citypeso = 0.5
            gblratio = ((nameratio * namepeso) + (streetratio * streetpeso) + (cityratio * citypeso)) / numeri
            if gblratio > 0.1:
                found = True
                print("   Trovato:", name.encode('utf-8'), cfrname.encode('utf-8'), gblratio)
                gL.cLite.execute ("insert into assetmatch (sourceasset,name,street,city,cfrsourceasset,cfrname,cfrstreet,cfrcity,nameratio,cityratio,streetratio,gblratio, country,assettype,source) \
                            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (sourceasset, name, street, city, cfrsourceasset, cfrname, cfrstreet, cfrcity, nameratio, cityratio, streetratio, gblratio,country,assettype,source))
                if mode == 1:
                    # ne ho trovata una buona già esistente, mi fermo
                    return found
    
    # inserisco la coppia che ho trovato
    if mode == 0 and not found:
        gL.cLite.execute ("insert into assetmatch (sourceasset,name,street,city,country,assettype) \
                   values (?, ?, ?, ?, ?, ?)",
                  (sourceasset, name, street, city, country, assettype))

    return found

def NameSimplify():

    # connect to db e crea il cursore
    gL.SqLite, gL.C = OpenConnectionSqlite()
    gL.MySql, gL.Cursor = OpenConnectionMySql()
    
    chgwrd = False
    chgfra = False
    namesimple = ""
    sql = "SELECT * from wasset where NameSimplified = " + str(NO) + " order BY name "
    gL.cLite.execute(sql)
    assets = gL.cLite.fetchall()
    for asset in assets:
        sourceassetid = asset[1]
        name = asset[4]        
        city = asset[7]
        assettype = asset[2]
        country = asset[0]
        # reperisco la lingua corrente
        gL.cSql.execute("select CountryLanguage from Country where countryid =?", ([country]))
        w = gL.cSql.fetchone()
        lang = w['countrylanguage']
        
        #
        # cerco le kwywords da trattare - frasi
        #
        idxlist=[]; newname=""
        sql = "SELECT * from keywords where language = '" + lang + "' and assettype = " + str(assettype) + " and numwords > 1 order by numwords desc"
        gL.cLite.execute(sql)
        frasi = gL.cLite.fetchall()
        for frase in frasi:
            keyword     = frase[2]
            operatoreF  = frase[3]
            typ1        = frase[4]
            typ2        = frase[5]
            typ3        = frase[6]
            typ4        = frase[7]
            typ5        = frase[8]
            replacewith = frase[9]
            numwords    = frase[7]
                                    
            trovato, chgfra, newname, idxlist = CercaFrase(keyword, name, operatoreF, replacewith)
            if trovato:
                print("Frase trattata:", name.encode('utf-8'), "trasformata in", newname.encode('utf-8'))
                # mi fermo alla prima trovata
                break
        
        #
        # cerco le kwywords da trattare - parole
        #
        sql = "SELECT * from keywords where language = '" + lang + "' and assettype = " + str(assettype) + " and numwords = 1 "
        gL.cLite.execute(sql)
        parole = gL.cLite.fetchall()        
       
        tmpname = newname; numdel = 0; y = []; yy = []; 
        yy = tmpname.split()

        for y, idx in enumerate(yy[:]):  # per ogni parola della stringa, : fa una copia della lista
                      
            for parola in parole:      # per ogni kwd 
                keyword    = parola[2]
                operatoreW = parola[3]
                typ1       = parola[4]
                typ3       = parola[5]
                replacew   = parola[6]
                numwords   = parola[7]

                # se ho una frase che deve essere preservata devo saltare le sue parole, i cui indici sono in idxlist
                if trovato and operatoreF == "Keep":
                    if idx in idxlist:
                        continue
                            
                if  y == "'":
                    yy.remove(y)
                    numdel = numdel + 1
                    chgwrd   = True
               
                if  keyword == y and operatoreW == "Replace":
                    yy.replace(y, replacew)
                    chgwrd   = True

                if  keyword == y and operatoreW == "Delete":
                    yy.remove(y)
                    numdel = numdel + 1
                    chgwrd   = True
                    break

                # toglie il nome della città!
                if keyword == city:
                    yy.remove(y)
                    numdel = numdel + 1
                    chgwrd   = True
                    break
        
        
        
            
                    
        # se ho eliminato tutte le parole del nome ripristino il nome stesso
        if (chgwrd or chgfra) and len(yy) == 0:
            newname = name
            
        if (chgwrd or chgfra):
            newname = " ".join(yy)
            gL.cSql.execute("Update SourceAsset set NameSimple = ?, NameSimplified = ? where SourceAssetId = ?", (newname, sourceassetid, YES))
        else:
            gL.cSql.execute("Update SourceAsset set NameSimple = ?, NameSimplified = ? where SourceAssetId = ?", (name, sourceassetid, NO))

    return True

def StdSourceAsset(countryid=None, sourceid=None, assettypeid=None, debug=True):
    gL.SqLite, gL.C = OpenConnectionSqlite()
    gL.MySql, gL.Cursor = OpenConnectionMySql()

    # leggo SourceAsset e esamino ogni record della tabella che non ha ancora un assetid
    # cerco un nome simile prima tra quelli già trovati, poi tra quelli simili ma non ancora battezzati
    # inserisco i risultati nella tabella AssetMatch come una coppia 
    # RecordSourceAsset da esaminare - RecordSourceAsset Trovato Simile 
    # o tutti e tre oppure niente
    if sourceid is not None and assettypeid is not None and countryid is not None:
        sql = "Select * from SourceAsset where AssetId = 0 and country = '" + countryid + "' and sourceid = " + str(sourceid) + " and assettypeid = " + str(assettypeid) + " order by name"
    else:
        sql = "Select * from SourceAsset where AssetId = 0 order by name"
    
    gL.cSql.execute(sql)
    cur = gL.cSql.fetchall()
    conta = 0
    for w in cur:
        conta = conta + 1
        if limita > 0:            
            if conta > limita:
                break
        sourceasset = w['sourceassetid']
        name = w['name']
        source = w['sourceid']
        city = w['addrcity']
        street = w['addrstreet']
        assettype = w['assettypeid']
        country = w['country']
        county  = w['addrcounty']
        phone = w['addrphone']
        print(" ")
        print(conta, "-Esamino: ", name.encode('utf-8'), sourceasset, source)
        # se il valore è alto lo inserisco in tabella
        rc = LookForName(1, country, source, assettype, sourceasset, name, city, street)
        if not rc:
            rc = LookForName(0, country, source, assettype, sourceasset, name, city, street)

    #  se richiesto, dumpo la tabella in memoria per capirci qualcosa
    if debug:
        sql_dump_Assetmatch()

    # dalla tabella assetmach mantengo solo i record che a parità di chiave hanno punteggio più alto
    sql = "SELECT sourceasset, cfrsourceasset, MAX(gblratio) FROM assetmatch GROUP BY sourceasset"
    gL.cLite.execute(sql)
    match = gL.cLite.fetchall()
    for a in match:
        sourceasset = a[0]
        cfrsourceasset = a[1]
        gblratio = a[2]
        
        # cerco nella tabella SourceAsset il record con l'assetid individuato
        sql = "select * from SourceAsset where sourceassetId = " + str(sourceasset)
        gL.cSql.execute(sql)          
        rows1 = gL.cSql.fetchone()
        AddrStreet = rows1['addrstreet']
        AddrCity = rows1['addrcity']
        AddrZIP = rows1['addrzip']
        AddrCounty = rows1['addrcounty']
        AddrPhone = rows1['addrphone']
        AddrWebsite = rows1['addrwebsite']
        AssetId = rows1['assetid']
        AddrLat  = rows1['AddrLat']
        AddrLong = rows1['AddrLong']
        AddrRegion = rows1['AddrRegion']
        FormattedAddress = rows1['FormattedAddress']
        AddrValidated = rows1['AddrValidated']

        # se nel record individuato esiste già l'asset battezzato
        if AssetId != 0:
            # devo aggiornare il record di SourceAsset che ho confrontato con l'assetid di quello corrente
            if gblratio > 0.5:
               gL.cSql.execute("Update SourceAsset set AssetId=? where SourceAssetId=?", (assetid, cfrsourceasset))
        else:
            rc, assetid = sql_InsertAsset(country, assettype, name, source, SetNow(), AddrStreet, AddrCity, AddrZIP, AddrCounty, phone, \
                                          AddrWebsite, AddrLat, AddrLong, FormattedAddress, AddrRegion, AddrValidated)
    return True


def NameInit(country=None, source=None, assettype=None):
    
    # connect to db e crea il cursore
    gL.SqLite, gL.C = OpenConnectionSqlite()
    gL.MySql, gL.Cursor = OpenConnectionMySql()
    
    # Create database table in memory
    sql_CreateMemTableWasset()
    sql_CreateMemTableAssetmatch()
    # popola con i dati
    rc = sql_CopyAssetInMemory(country, source, assettype)    
    rc = sql_CreateMemTableKeywords()
    rc = sql_CopyKeywordsInMemory()
    return True

if __name__ == "__main__":
    
    rc = NameInit()
    rc = StdSourceAsset(None, None, True)
    rc = gL.cSql.commit()
    
    rc = NameSimplify()
    rc = gL.cSql.commit()