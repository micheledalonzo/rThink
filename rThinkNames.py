# -*- coding: cp1252 -*-.
import rThinkGbl as gL

# fanne solo limita, di nomi
limita = 0

def LookForName(mode, country, source, assettype, sourceasset, name, city, street):
    #SqLite, c = gL.OpenConnectionSqlite()

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


def NameSimplify(lang, assettype, nome):

    # connect to db e crea il cursore
    gL.SqLite, gL.C = gL.OpenConnectionSqlite()
    gL.MySql, gL.Cursor = gL.OpenConnectionMySql()
    chg    = False
    chgwrd = False
    chgfra = False
    namesimple = ""
    tag = []
        
    if not nome or not lang:
        print("Errore nella chiamata di NameSimplify")
        return chg, '', ''

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
                                              
        trovato, chgfra, newname, idxlist = gL.CercaFrase(keyword, nome, operatoreF, replacewith)
        if trovato:
            if typ1 is not None:
                tag.append(typ1)
            if typ2 is not None:
                tag.append(typ2)
            if typ3 is not None:
                tag.append(typ3)
            if typ4 is not None:
                tag.append(typ4)
            if typ5 is not None:
                tag.append(typ5)
            print("Frase trattata:", nome.encode('utf-8'), "trasformata in", operatoreF, newname.encode('utf-8'))
            # mi fermo alla prima trovata
            break
        else:
            newname = nome
    #
    # cerco le kwywords da trattare - parole
    #
    sql = "SELECT * from keywords where language = '" + lang + "' and assettype = " + str(assettype) + " and numwords = 1 "
    gL.cLite.execute(sql)
    parole = gL.cLite.fetchall()        
       
    tmpname = newname; numdel = 0; y = []; yy = []; 
    yy = tmpname.split()

    for idx, y in enumerate(yy[:]):  # per ogni parola della stringa, : fa una copia della lista
                      
        for parola in parole:      # per ogni kwd 
            keyword     = parola[2]
            operatoreW  = parola[3]
            xtyp1       = parola[4]
            xtyp2       = parola[5]
            xtyp3       = parola[6]
            xtyp4       = parola[7]
            xtyp5       = parola[8]
            replacew    = parola[9]
            numwords    = parola[7]

            # se ho una frase che deve essere preservata devo saltare le sue parole, i cui indici sono in idxlist
            if trovato and operatoreF == "Keep":
                if idx in idxlist:
                    continue
                            
            if  y == "'":
                yy.remove(y)
                numdel = numdel + 1
                if xtyp1 is not None:
                    tag.append(xtyp1)
                if xtyp2 is not None:
                    tag.append(xtyp2)
                if xtyp3 is not None:
                    tag.append(xtyp3)
                if xtyp4 is not None:
                    tag.append(xtyp4)
                if xtyp5 is not None:
                    tag.append(xtyp5)
                chgwrd   = True
               
            if  keyword == y and operatoreW == "Replace":
                yy.replace(y, replacew)
                if xtyp1 is not None:
                    tag.append(xtyp1)
                if xtyp2 is not None:
                    tag.append(xtyp2)
                if xtyp3 is not None:
                    tag.append(xtyp3)
                if xtyp4 is not None:
                    tag.append(xtyp4)
                if xtyp5 is not None:
                    tag.append(xtyp5)
                chgwrd   = True

            if  keyword == y and operatoreW == "Delete":
                yy.remove(y)
                numdel = numdel + 1
                chgwrd   = True
                if xtyp1 is not None:
                    tag.append(xtyp1)
                if xtyp2 is not None:
                    tag.append(xtyp2)
                if xtyp3 is not None:
                    tag.append(xtyp3)
                if xtyp4 is not None:
                    tag.append(xtyp4)
                if xtyp5 is not None:
                    tag.append(xtyp5)
                break        
        
    if chgwrd:
        newname = " ".join(yy)
           
    # se ho eliminato tutte le parole del nome ripristino il nome stesso
    if (chgwrd or chgfra) and len(yy) > 0:
        chg = True
        newname = nome
            
    return chg, newname, tag



def AllNameSimplify():
 
    sql = "SELECT * from wasset where NameSimplified = " + str(gL.NO) + " order BY name "
    gL.cLite.execute(sql)
    assets = gL.cLite.fetchall() 
    if not assets:
        return False
    
    for asset in assets:
        tag = []
        sourceassetid = asset[1]
        name = str(asset[4])        
        city = asset[7]
        assettype = asset[2]
        country = asset[0]
        # reperisco la lingua corrente
        gL.cSql.execute("select CountryLanguage from Country where countryid =?", ([country]))
        w = gL.cSql.fetchone()
        lang = w['countrylanguage']
        chg, newname, tag = NameSimplify(lang, assettype, name)
        if chg:
            gL.cSql.execute("Update SourceAsset set NameSimple = ?, NameSimplified = ? where SourceAssetId = ?", (newname, gL.YES, sourceassetid))

    return True

def StdSourceAsset(countryid=None, sourceid=None, assettypeid=None, debug=True):
    gL.SqLite, gL.C = gL.OpenConnectionSqlite()
    gL.MySql, gL.Cursor = gL.OpenConnectionMySql()

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
        gL.sql_dump_Assetmatch()

    # dalla tabella assetmach mantengo solo i record che a parità di chiave hanno punteggio più alto
    #sql = "SELECT sourceasset, cfrsourceasset, MAX(gblratio) FROM assetmatch GROUP BY sourceasset"
    sql = "SELECT assetmatch.* FROM assetmatch INNER JOIN (SELECT assetmatch.sourceasset, Max(assetmatch.gblratio) AS MaxDigblratio FROM assetmatch \
           GROUP BY assetmatch.sourceasset) as QA ON (assetmatch.sourceasset = QA.sourceasset) AND (assetmatch.gblratio = QA.MaxDigblratio)"
    gL.cLite.execute(sql)
    match = gL.cLite.fetchall()
    for a in match:
        sourceasset = a[0]
        cfrsourceasset = a[1]
        gblratio = a[2]
        
        rate = 0; xrate = 0
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
        if FormattedAddress: rate = rate + 1
        if AddrStreet: rate = rate + 1 
        if AddrCity: rate = rate + 1 
        if AddrZIP: rate = rate + 1 
        if AddrCounty: rate = rate + 1 
        if AddrPhone: rate = rate + 1 
        if AddrWebsite: rate = rate + 1
        if AssetId: rate = rate + 1 
        if AddrLat: rate = rate + 3  
        if AddrLong: rate = rate + 3 
        if AddrRegion: rate = rate + 1 
        if AddrValidated: rate = rate * 2

        # cerco nella tabella SourceAsset il record con l'assetid di confronto
        sql = "select * from SourceAsset where sourceassetId = " + str(sourceasset)
        gL.cSql.execute(sql)          
        rows1 = gL.cSql.fetchone()
        xAddrStreet = rows1['addrstreet']
        xAddrCity = rows1['addrcity']
        xAddrZIP = rows1['addrzip']
        xAddrCounty = rows1['addrcounty']
        xAddrPhone = rows1['addrphone']
        xAddrWebsite = rows1['addrwebsite']
        xAssetId = rows1['assetid']
        xAddrLat  = rows1['AddrLat']
        xAddrLong = rows1['AddrLong']
        xAddrRegion = rows1['AddrRegion']
        xFormattedAddress = rows1['FormattedAddress']
        xAddrValidated = rows1['AddrValidated']
        if xFormattedAddress: xrate = xrate + 1
        if xAddrStreet: xrate = xrate + 1 
        if xAddrCity: xrate = xrate + 1 
        if xAddrZIP: xrate = xrate + 1 
        if xAddrCounty: xrate = xrate + 1 
        if xAddrPhone: xrate = xrate + 1 
        if xAddrWebsite: xrate = xrate + 1
        if xAssetId: xrate = xrate + 1 
        if xAddrLat: xrate = xrate + 3  
        if xAddrLong: xrate = xrate + 3 
        if xAddrRegion: xrate = xrate + 1 
        if xAddrValidated: xrate = xrate * 2
        bestasset = sourceasset    # devo decidere quale record ha le migliori informazioni
        if xrate > rate: bestasset = cfrsourceasset

        # se nel record individuato esiste già l'asset battezzato
        if AssetId != 0:
            # devo aggiornare il record di SourceAsset che ho confrontato con l'assetid di quello corrente
            if gblratio > 0.5:
               gL.cSql.execute("Update SourceAsset set AssetId=? where SourceAssetId=?", (assetid, bestasset))
        else:
            rc, newassetid = gL.sql_InsertAsset(country, assettype, name, source, gL.SetNow(), AddrStreet, AddrCity, AddrZIP, AddrCounty, phone, \
                                          AddrWebsite, AddrLat, AddrLong, FormattedAddress, AddrRegion, AddrValidated)
            gL.cSql.execute("Update SourceAsset set AssetId=? where SourceAssetId=?", (newassetid, cfrsourceasset))
            gL.cSql.execute("Update SourceAsset set AssetId=? where SourceAssetId=?", (newassetid, cfrsourceasset))
        return True


def NameInit(country=None, source=None, assettype=None):
    
    # connect to db e crea il cursore
    gL.SqLite, gL.C = gL.OpenConnectionSqlite()
    gL.MySql, gL.Cursor = gL.OpenConnectionMySql()
    
    # Create database table in memory
    gL.sql_CreateMemTableWasset()
    gL.sql_CreateMemTableAssetmatch()
    # popola con i dati
    rc = gL.sql_CopyAssetInMemory(country, source, assettype)    
    rc = gL.sql_CreateMemTableKeywords()
    rc = gL.sql_CopyKeywordsInMemory()
    return True

if __name__ == "__main__":
    
    rc = NameInit()
    #rc = gL.StdSourceAsset(None, None, True)
    #rc = gL.cSql.commit()
    
    rc = gL.AllNameSimplify()
    rc = gL.cSql.commit()