# -*- coding: cp1252 -*-.
import rThinkGbl as gL
import phonenumbers
#import difflib
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

# fanne solo limita, di nomi
limita = 0


def NameSimplify(lang, assettype, nome):
    try:
        # connect to db e crea il cursore
        gL.SqLite, gL.C = gL.OpenConnectionSqlite()
        gL.MySql, gL.Cursor = gL.OpenConnectionMySql(gL.Dsn)
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

        gL.log(gL.ERROR, nome)
        gL.log(gL.ERROR, err)

    return chg, newname, typ, cuc



def AllNameSimplify():    
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

    try:
        tabratio = []
        # il record corrente
        gL.cSql.execute("select Asset, Assettype, Source, AAsset, Country, Name, NameSimple, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrRegion, FormattedAddress from qaddress where asset =  ?", ([Asset]))
        asset = gL.cSql.fetchone() 
        if not asset:
            gL.log("ERROR", "Asset non trovato in tabella")
            return False
        if asset['aasset'] != 0:   # se è già stato battezzato non lo esamino di nuovo
            return Asset, asset['aasset']
        # tutti i record dello stesso tipo e paese ma differenti source, e che hanno già un asset di riferimento (aasset)
        gL.cSql.execute("select AAsset, Asset, Country, Name, NameSimple, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrRegion, FormattedAddress from qaddress where \
                                Asset <> ? and source <> ? and country = ? and assettype = ? and AAsset <> 0", (Asset, asset['source'], asset['country'], asset['assettype']))
        rows  = gL.cSql.fetchall()     
        if len(rows) == 0:   # non ce ne sono
            return 0,0   #inserisco l'asset corrente

        for j in range (0, len(rows)):
            # se hanno esattamente stesso sito web o telefono o indirizzo sono uguali
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

            nameratio=nameratio_ratio=nameratio_set=nameratio_partial=0           
            streetratio=streetratio_set=streetratio_partial=streetratio_ratio=0
            cityratio_ratio=cityratio_set=cityratio_partial=cityratio=0             
            webratio=phoneratio=zipratio=0
            name = cfrname = city = cfrcity = street = cfrstreet = zip = cfrzip = ''
            gblratio = 0; quanti = 0; 
            if asset['name'] and rows[j]['name'] :
                name = asset['name'].title()
                cfrname = rows[j]['name'].title()                
                nameratio_ratio = fuzz.ratio(name, cfrname)
                nameratio_partial = fuzz.partial_ratio(name, cfrname)
                nameratio_set = fuzz.token_set_ratio(name, cfrname)
                nameratio = nameratio_set+ nameratio_partial + nameratio_ratio
                if nameratio_partial > 70:
                    quanti = quanti + 1
                else:
                    continue
                #print(name+","+cfrname+","+str(nameratio)+","+str(fuzz.ratio(name, cfrname))+","+str(fuzz.partial_ratio(name, cfrname))+","+str(fuzz.token_sort_ratio(name, cfrname))+","+str(fuzz.token_set_ratio(name, cfrname)))
            if asset['addrcity'] and rows[j]['addrcity'] :
                city = asset['addrcity'].title() 
                cfrcity = rows[j]['addrcity'].title()
                cityratio_ratio = fuzz.ratio(city, cfrcity)
                cityratio_partial = fuzz.partial_ratio(city, cfrcity)
                cityratio_set = fuzz.token_set_ratio(city, cfrcity)
                cityratio = cityratio_set + cityratio_partial + cityratio_ratio
                if cityratio > 75:
                    quanti = quanti + 1                
                else:
                    cityratio = 0
            if asset['addrstreet'] and rows[j]['addrstreet'] :
                street = asset['addrstreet'].title()             
                cfrstreet = rows[j]['addrstreet'].title()                               
                streetratio_ratio = fuzz.ratio(street, cfrstreet)
                streetratio_partial = fuzz.partial_ratio(street, cfrstreet)
                streetratio_set = fuzz.token_set_ratio(street, cfrstreet)
                streetratio = streetratio_set + streetratio_partial + streetratio_ratio
                if streetratio > 75:
                    quanti = quanti + 1 
                else:
                    streetratio = 0 
            if asset['website'] and rows[j]['website'] :
                web = asset['website'].title() 
                cfrweb = rows[j]['website'].title()                
                webratio = fuzz.ratio(web, cfrweb)
                if webratio > 90:
                    quanti = quanti + 1
                else:
                    webratio = 0
            if asset['addrphone'] and rows[j]['addrphone'] :
                pho = asset['addrphone'].title() 
                cfrpho = rows[j]['addrphone'].title()                
                phoneratio = fuzz.ratio(pho, cfrpho)
                if phoneratio > 90:
                    quanti = quanti + 1
                else:
                    phoneratio = 0
            if asset['addrzip'] and rows[j]['addrzip'] :
                zip = rows[j]['addrzip'].title()
                cfrzip = rows[j]['addrzip'].title()
                zipratio = fuzz.ratio(zip, cfrzip)
                if zipratio > 90:
                    quanti = quanti + 1
                else:
                    zipratio = 0
            if nameratio > 200:
                # peso i match 0,6 sufficiente, 
                namepeso = 2
                streetpeso = 1.5
                citypeso = 1
                zippeso = 1
                webpeso = 1
                phonepeso = 1
                gblratio =( ((nameratio     * namepeso) +             \
                             (streetratio   * streetpeso) +           \
                             (cityratio     * citypeso) +             \
                             (zipratio      * zippeso) +              \
                             (webratio      * webpeso) +              \
                             (phoneratio    * phonepeso))             \
                             /
                             (quanti)  )            
                tabratio.append((gblratio, asset['name'], rows[j]['name'], rows[j]['asset'], rows[j]['aasset'], nameratio, streetratio, cityratio, zipratio, webratio, phoneratio ))                  
            
        if len(tabratio) > 0:
            tabratio.sort(reverse=True, key=lambda tup: tup[0])  
            if tabratio[0][0] > 0.7:   # global
                msg = (asset['name'],rows[j]['name'], gblratio)
                gL.log(gL.INFO, tabratio[0][1] + "==" + tabratio[0][2] + "==" + str(tabratio[0][0]))
                return tabratio[0][3], tabratio[0][4]  # Asset, AAsset

        return 0,0

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

def NameInit(country=None, source=None, assettype=None):
    
    # connect to db e crea il cursore
    gL.SqLite, gL.C = gL.OpenConnectionSqlite()
    gL.MySql, gL.Cursor = gL.OpenConnectionMySql(gL.Dsn)
    
    # Create database table in memory
    gL.CreateMemTableWasset()
    gL.CreateMemTableAssetmatch()
    # popola con i dati
    rc = gL.sql_CopyAssetInMemory(country, source, assettype)    
    rc = gL.CreateMemTableKeywords()
    rc = gL.sql_CopyKeywordsInMemory()
    return True

if __name__ == "__main__":    
    rc = NameInit()
    #rc = gL.StdAsset()
    rc = gL.cSql.commit()
    
    #rc = gL.AllNameSimplify()
    #rc = gL.cSql.commit()