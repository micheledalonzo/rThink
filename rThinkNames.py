# -*- coding: cp1252 -*-.
import rThinkGbl as gL
import phonenumbers
import difflib

# fanne solo limita, di nomi
limita = 0

def LookForName(mode, country, source, assettype, asset, name, city, street):
    gL.log(gL.DEBUG)

    # mode=1 Leggi solo i record con assetid, se ne trovi uno esci
    # mode=0 Leggi i record senza assetid, inserisci tutti quelli trovati
    found = False
    # legge la tabella in memoria degli asset selezionando quelli di source diversi e dello stesso tipo e paese
    if mode == 1:
        # leggo i record con assetid già valorizzato, quindi con un record nella tabella asset
        sql = "Select * from wasset where Source <> " + str(source) + " and AssetType = " + str(assettype) + " and Asset <> 0  and Country = '" + country + "' order by name"
    else:
        # leggo i record con assetid non ancora valorizzato, quindi con un record nella tabella asset
        sql = "Select * from wasset where Source <> " + str(source) + " and AssetType = " + str(assettype) + " and Asset= 0 and Country = '" + country + "' order by name"

    gL.cLite.execute(sql)
    cur = gL.cLite.fetchall()
    for wrk in cur:
        cfrasset = wrk[1]
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
                gL.cLite.execute ("insert into assetmatch (asset,name,street,city,cfrasset,cfrname,cfrstreet,cfrcity,nameratio,cityratio,streetratio,gblratio, country,assettype,source) \
                            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (asset, name, street, city, cfrasset, cfrname, cfrstreet, cfrcity, nameratio, cityratio, streetratio, gblratio,country,assettype,source))
                if mode == 1:
                    # ne ho trovata una buona già esistente, mi fermo
                    return found
    
    # inserisco la coppia che ho trovato
    if mode == 0 and not found:
        gL.cLite.execute ("insert into assetmatch (asset,name,street,city,country,assettype) \
                   values (?, ?, ?, ?, ?, ?)",
                  (asset, name, street, city, country, assettype))

    return found


def NameSimplify(lang, assettype, nome):
    try:
        # connect to db e crea il cursore
        gL.SqLite, gL.C = gL.OpenConnectionSqlite()
        gL.MySql, gL.Cursor = gL.OpenConnectionMySql()
        chg    = False
        chgwrd = False
        chgfra = False
        namesimple = ""
        typ = []
        cuc = []
        
        if not nome or not lang:
            gL.log(gL.ERROR, "Errore nella chiamata di NameSimplify")
            return chg, '', ''

        # cerco le kwywords da trattare - frasi
        idxlist=[]; newname=""

        if len(gL.Frasi) == 0:
            sql = "SELECT * from keywords where language = '" + lang + "' and assettype = " + str(assettype) + " and numwords > 1 order by numwords desc"
            gL.cLite.execute(sql)
            gL.Frasi = gL.cLite.fetchall()
        for frase in gL.Frasi:
            keyword     = frase[2]
            operatoreF  = frase[3]
            typ1        = frase[4]
            typ2        = frase[5]
            cuc1        = frase[6]
            cuc2        = frase[7]
            cuc3        = frase[8]
            replacewith = frase[9]
            numwords    = frase[7]
                                              
            trovato, chgfra, newname, idxlist = gL.CercaFrase(keyword, nome, operatoreF, replacewith)
            if trovato:
                if typ1 is not None:
                    typ.append(typ1)
                if typ2 is not None:
                    typ.append(typ2)
                if cuc1 is not None:
                    cuc.append(cuc1)
                if cuc2 is not None:
                    cuc.append(cuc2)
                if cuc3 is not None:
                    cuc.append(cuc3)
                #print("Frase trattata:", nome.encode('utf-8'), "trasformata in", operatoreF, newname.encode('utf-8'))
                # mi fermo alla prima trovata
                break
            else:
                newname = nome
        
        # cerco le kwywords da trattare - parole
        if len(gL.Parole) == 0:
            sql = "SELECT * from keywords where language = '" + lang + "' and assettype = " + str(assettype) + " and numwords = 1 "
            gL.cLite.execute(sql)
            gL.Parole = gL.cLite.fetchall()        
       
        tmpname = newname; numdel = 0; y = []; yy = []; 
        yy = tmpname.split()

        for idx, y in enumerate(yy[:]):  # per ogni parola della stringa, : fa una copia della lista
                      
            for parola in gL.Parole:      # per ogni kwd 
                keyword     = parola[2]
                operatoreW  = parola[3]
                xtyp1       = parola[4]
                xtyp2       = parola[5]
                xcuc1       = parola[6]
                xcuc2       = parola[7]
                xcuc3       = parola[8]
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
                        typ.append(xtyp1)
                    if xtyp2 is not None:
                        typ.append(xtyp2)
                    if xcuc1 is not None:
                        cuc.append(xcuc1)
                    if xcuc2 is not None:
                        cuc.append(xcuc2)
                    if xcuc3 is not None:
                        cuc.append(xcuc3)
                    chgwrd   = True
               
                if  keyword == y and operatoreW == "Replace":
                    yy.replace(y, replacew)
                    if xtyp1 is not None:
                        typ.append(xtyp1)
                    if xtyp2 is not None:
                        typ.append(xtyp2)
                    if xcuc1 is not None:
                        cuc.append(xcuc1)
                    if xcuc2 is not None:
                        cuc.append(xcuc2)
                    if xcuc3 is not None:
                        cuc.append(xcuc3)
                    chgwrd   = True

                if  keyword == y and operatoreW == "Delete":
                    yy.remove(y)
                    numdel = numdel + 1
                    chgwrd   = True
                    if xtyp1 is not None:
                        typ.append(xtyp1)
                    if xtyp2 is not None:
                        typ.append(xtyp2)
                    if xcuc1 is not None:
                        cuc.append(xcuc1)
                    if xcuc2 is not None:
                        cuc.append(xcuc2)
                    if xcuc3 is not None:
                        cuc.append(xcuc3)
                    break        
        
        if chgwrd:
            newname = " ".join(yy)
        
        # se ho eliminato tutte le parole del nome ripristino il nome stesso
        if (chgwrd or chgfra) and len(yy) == 0:        
            newname = nome

        if nome != newname:
            msg = nome + "==>" + newname
            gL.log(gL.INFO, msg)
            chg = True            
    
    except Exception as err:

        gL.log(gL.ERROR, name)
        gL.log(gL.ERROR, err)

    return chg, newname, typ, cuc



def AllNameSimplify():
    gL.log(gL.DEBUG)
    sql = "SELECT * from wasset where NameSimplified = " + str(gL.NO) + " order BY name "
    gL.cLite.execute(sql)
    assets = gL.cLite.fetchall() 
    if not assets:
        return False
    
    for asset in assets:
        tag = []
        asset = asset[1]
        name = str(asset[4])        
        city = asset[7]
        assettype = asset[2]
        country = asset[0]
        # reperisco la lingua corrente
        gL.cSql.execute("select CountryLanguage from Country where country =?", ([country]))
        w = gL.cSql.fetchone()
        lang = w['countrylanguage']
        chg, newname, tag, cuc = NameSimplify(lang, assettype, name)
        if chg:
            gL.cSql.execute("Update Asset set NameSimple = ?, NameSimplified = ? where Asset = ?", (newname, gL.YES, asset))

    return True

def ManageName(name, country, assettype):
    # tabella delle lingue per paese
    CountryLang = {}
    language = CountryLang.get(country) 
    if language is None:
        gL.cSql.execute("select CountryLanguage from T_Country where Country = ?", ([country]))
        row = gL.cSql.fetchone()
        if row:
            language = row['countrylanguage']           
            CountryLang[country] = language
    if language is None:
        gL.log(gL.ERROR, "Lingua non trovata")
        return False
    
    # gestisco il nome e la tipologia del locale definita dal nome    
    NameSimplified = gL.NO; NameSimple = ''
    chg, newname, tag, cuc = gL.NameSimplify(language, assettype, name)
    if chg:
        #print("Frase trattata:", name.encode('utf-8'), "trasformata in", newname.encode('utf-8'))
        NameSimple = newname
        NameSimplified = gL.YES

    return NameSimple, NameSimplified, tag, cuc

def StdAsset(Asset):

    # il record corrente
    gL.cSql.execute("select Asset, Assettype, Source, AAsset, Country, Name, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrRegion, FormattedAddress from qaddress where asset =  ?", ([Asset]))
    asset = gL.cSql.fetchone() 
    if not asset:
        gL.log("ERROR", "Asset non trovato in tabella")
        return False
    # tutti i record dello stesso tipo e paese ma differenti source, e che hanno già un asset di riferimento (aasset)
    gL.cSql.execute("select AAsset, Asset, Country, Name, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrRegion, FormattedAddress from qaddress where \
                            Asset <> ? and source <> ? and country = ? and assettype = ? and AAsset <> 0", (Asset, asset['source'], asset['country'], asset['assettype']))
    rows  = gL.cSql.fetchall()     
    if len(rows) == 0:   # non ce ne sono
        return 0,0   #inserisco l'asset corrente

    for j in range (0, len(rows)):
        # se hanno stesso sito web o telefono o indirizzo sono uguali
        if asset['addrwebsite'] and rows[j]['addrwebsite'] and (asset['addrwebsite'] == rows[j]['addrwebsite']):
            return rows[j]['asset'], rows[j]['aasset']
        if asset['addrphone'] and rows[j]['addrphone'] and (asset['addrphone'] == rows[j]['addrphone']):
            return rows[j]['asset'], rows[j]['aasset']
        if asset['addrcity'] and asset['addrroute']:   # se c'è almeno la strada e la città
            if asset['formattedaddress'] and rows[j]['formattedaddress'] and (asset['formattedaddress'] == rows[j]['formattedaddress']):
                return rows[j]['asset'], rows[j]['aasset']
        # se non hanno lo stesso paese, regione, provincia, salto
        if asset['country'] and rows[j]['country'] and (asset['country'] != rows[j]['country']):
            continue
        if asset['addrregion'] and rows[j]['addrregion'] and (asset['addrregion'] != rows[j]['addrregion']):
            continue

        nameratio = streetratio = cityratio = 1
        gblratio = 0; quanti = 0
        if asset['name'] and rows[j]['name'] :
            name = asset['name'].title()
            cfrname = rows[j]['name'].title()
            quanti = quanti + 1
            nameratio = difflib.SequenceMatcher(None, a = name, b = cfrname).ratio()
        if asset['addrcity'] and rows[j]['addrcity'] :
            city = asset['addrcity'].title() 
            cfrcity = rows[j]['addrcity'].title()
            quanti = quanti + 1
            cityratio = difflib.SequenceMatcher(None, a = city, b = cfrcity).ratio()
        if asset['addrstreet'] and rows[j]['addrstreet'] :
            street = asset['addrstreet'].title()             
            cfrstreet = rows[j]['addrstreet'].title()
            quanti = quanti + 1
            streetratio = difflib.SequenceMatcher(None, a = street, b = cfrstreet).ratio()
        if asset['addrzip'] and rows[j]['addrzip'] :
            zip = rows[j]['addrzip'].title()
            cfrzip = rows[j]['addrzip'].title()
            quanti = quanti + 1
            zipratio = difflib.SequenceMatcher(None, a = street, b = cfrstreet).ratio()
        # stampa ="Name:" + name + " with:" + cfrname
        # print("   --->", stampa[:50].encode('UTF-8'), end='\r')
        if nameratio > 0.8:
            # peso i match 0,6 sufficiente, con tutti a 0,6 = 0,82
            namepeso = 2
            streetpeso = 1.5
            citypeso = 1
            zippeso = 1
            gblratio = (((nameratio * namepeso) + (streetratio * streetpeso) + (cityratio * citypeso)) / (quanti))
            if gblratio > 0.8:
                msg = (asset['name'],rows[j]['name'], gblratio)
                gL.log(gL.INFO, asset['name'] + "==" + rows[j]['name'] + "==" + str(gblratio))
                return rows[j]['asset'], rows[j]['aasset']

    return 0,0

def OldStdAsset(country=None, source=None, assettype=None, debug=True):
    gL.log(gL.DEBUG)
    gL.SqLite, gL.C = gL.OpenConnectionSqlite()
    gL.MySql, gL.Cursor = gL.OpenConnectionMySql()

    # leggo Asset e esamino ogni record della tabella che non ha ancora un assetid
    # cerco un nome simile prima tra quelli già trovati, poi tra quelli simili ma non ancora battezzati
    # inserisco i risultati nella tabella AssetMatch come una coppia 
    # RecordAsset da esaminare - RecordAsset Trovato Simile 
    # o tutti e tre oppure niente
    if source is not None and assettype is not None and country is not None:
        sql = "Select * from Asset where Asset = 0 and country = '" + country + "' and source = " + str(source) + " and assettype = " + str(assettype) + " order by name"
    else:
        sql = "Select * from Asset where Asset = 0 order by name"
    
    gL.cSql.execute(sql)
    cur = gL.cSql.fetchall()
    conta = 0
    for w in cur:
        conta = conta + 1
        if limita > 0:            
            if conta > limita:
                break
        asset = w['asset']
        name = w['name']
        source = w['source']
        city = w['addrcity']
        street = w['addrstreet']
        assettype = w['assettype']
        gL.country = w['country']
        county  = w['addrcounty']
        phone = w['addrphone']
        print(" ")
        print(conta, "-Esamino: ", name.encode('utf-8'), asset, source)
        # se il valore è alto lo inserisco in tabella
        rc = LookForName(1, gL.country, source, assettype, asset, name, city, street)
        if not rc:
            rc = LookForName(0, gL.country, source, assettype, asset, name, city, street)

    #  se richiesto, dumpo la tabella in memoria per capirci qualcosa
    if debug:
        gL.sql_dump_Assetmatch()

    # dalla tabella assetmach mantengo solo i record che a parità di chiave hanno punteggio più alto
    #sql = "SELECT asset, cfrasset, MAX(gblratio) FROM assetmatch GROUP BY asset"
    sql = "SELECT assetmatch.* FROM assetmatch INNER JOIN (SELECT assetmatch.asset, Max(assetmatch.gblratio) AS MaxDigblratio FROM assetmatch \
           GROUP BY assetmatch.asset) as QA ON (assetmatch.asset = QA.asset) AND (assetmatch.gblratio = QA.MaxDigblratio)"
    gL.cLite.execute(sql)
    match = gL.cLite.fetchall()
    for a in match:
        asset = a[0]
        cfrasset = a[1]
        gblratio = a[2]
        
        rate = 0; xrate = 0
        # cerco nella tabella Asset il record con l'assetid individuato
        sql = "select * from Asset where assetId = " + str(asset)
        gL.cSql.execute(sql)          
        rows1 = gL.cSql.fetchone()
        AddrStreet = rows1['addrstreet']
        AddrCity = rows1['addrcity']
        AddrZIP = rows1['addrzip']
        AddrCounty = rows1['addrcounty']
        AddrPhone = rows1['addrphone']
        AddrWebsite = rows1['addrwebsite']
        Asset = rows1['assetid']
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
        if Asset: rate = rate + 1 
        if AddrLat: rate = rate + 3  
        if AddrLong: rate = rate + 3 
        if AddrRegion: rate = rate + 1 
        if AddrValidated: rate = rate * 2

        # cerco nella tabella Asset il record con l'assetid di confronto
        sql = "select * from Asset where assetId = " + str(asset)
        gL.cSql.execute(sql)          
        rows1 = gL.cSql.fetchone()
        xAddrStreet = rows1['addrstreet']
        xAddrCity = rows1['addrcity']
        xAddrZIP = rows1['addrzip']
        xAddrCounty = rows1['addrcounty']
        xAddrPhone = rows1['addrphone']
        xAddrWebsite = rows1['addrwebsite']
        xAsset = rows1['assetid']
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
        if xAsset: xrate = xrate + 1 
        if xAddrLat: xrate = xrate + 3  
        if xAddrLong: xrate = xrate + 3 
        if xAddrRegion: xrate = xrate + 1 
        if xAddrValidated: xrate = xrate * 2
        bestasset = asset    # devo decidere quale record ha le migliori informazioni
        if xrate > rate: bestasset = cfrasset

        # se nel record individuato esiste già l'asset battezzato
        if Asset != 0:
            # devo aggiornare il record di Asset che ho confrontato con l'assetid di quello corrente
            if gblratio > 0.5:
               gL.cSql.execute("Update Asset set Asset=? where Asset=?", (assetid, bestasset))
        else:
            #rc, newassetid = gL.AAsset(country, assettype, name, source, gL.SetNow(), AddrStreet, AddrCity, AddrZIP, AddrCounty, phone, \
            #                              AddrWebsite, AddrLat, AddrLong, FormattedAddress, AddrRegion, AddrValidated)
            gL.cSql.execute("Update Asset set Asset=? where Asset=?", (newassetid, cfrasset))
            #gL.cSql.execute("Update Asset set Asset=? where Asset=?", (newassetid, cfrasset))
        return True


def NameInit(country=None, source=None, assettype=None):
    gL.log(gL.DEBUG)
    # connect to db e crea il cursore
    gL.SqLite, gL.C = gL.OpenConnectionSqlite()
    gL.MySql, gL.Cursor = gL.OpenConnectionMySql()
    
    # Create database table in memory
    gL.CreateMemTableWasset()
    gL.CreateMemTableAssetmatch()
    # popola con i dati
    rc = gL.sql_CopyAssetInMemory(country, source, assettype)    
    rc = gL.CreateMemTableKeywords()
    rc = gL.sql_CopyKeywordsInMemory()
    return True

if __name__ == "__main__":
    gL.log(gL.DEBUG)
    rc = NameInit()
    #rc = gL.StdAsset()
    rc = gL.cSql.commit()
    
    #rc = gL.AllNameSimplify()
    #rc = gL.cSql.commit()