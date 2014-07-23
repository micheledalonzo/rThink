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

import requests
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
from rThinkDb import *
from rThinkFunctions import *
import rThinkGbl as gL
from rThinkNames import *


#---------------------------------------------------------------------------
#   COSTANTI E VAR GLOBALI
#---------------------------------------------------------------------------


def ReadPage(url):
    # legge una pagina e la restituisce
    # metodo con javascript risolto
    # pagejava = jw.get_page(link, 3)
    # content = html.fromstring(pagejava)
    try:
        time.sleep(wait)
        page = requests.get(url)  # timeout=0.001 è un parametro
        if page.status_code != requests.codes.ok:
            print("ERR:", page.status_code, url)
            return None
    except requests.exceptions.RequestException as e:
        print(e)
        return None

    content = html.fromstring(page.content)
    return content

def nextpage_tripadvisor(url, page):
    
    # get la prossima pagina lista e inseriscila nella coda di lavoro e nella
    # tabella starturl
    # per tutti i link rel next
    links = page.xpath('//link[@rel="next"]/@href')
    for link in links:
         # link = gL.assetbaseurl + link
         # controllo che esista e l'inserisco nella coda
         url = gL.assetbaseurl + link
         if ReadPage(url) is not None:
            return url
         else:
            return False

def nextpage_qristoranti(url, page):
    # get la prossima pagina lista e inseriscila nella coda di lavoro e nella
    # tabella starturl
    # per tutti i link rel next
    curpa = int(page.xpath('//span[@class="inactive"]/text()')[0])
    links = page.xpath('//a[@class="paginate"]/@href')
    numpa = page.xpath('//a[@class="paginate"]/text()')
    if numpa[0] is not None:
        if int(numpa[0]) > int(curpa):
            return(links[0])
    return False
         
def queue_tripadvisor(starturl, pageurl, page):
   # leggi la lista e inserisci asset
    lista = page.xpath('//*[@class="listing" or @class="listing first"]')
    for asset in lista:
        name = asset.xpath('.//*[@class="property_title"]//text()')[0]
        name = StdName(name)
        url  = asset.xpath('.//a[contains(@class,"property_title")]/@href')[0]
        url  = gL.sourcebaseurl + url
        # inserisci o aggiorna l'asset        
        rs = sql_Queue(country, assettype, source, starturl, pageurl, url, name)

def parse_tripadvisor(starturl, url, name):

    # leggi la pagina di contenuti
    content = ReadPage(url)
    if content is  None:
        return False
    cur_asset_id, CurAssetLastReviewDate = sql_UpdSourceAsset(source, assettype, country, name, url)
    LastReviewDate = content.xpath('//span[@class="ratingDate"]/text()')  # la prima che trovo e' la piÃ¹ recente
    if LastReviewDate:
        LastReviewDate = LastReviewDate[0]
        LastReviewDate = StdCar(LastReviewDate)
        LastReviewDate = LastReviewDate.replace('Recensito il ', '')
        LastReviewDate = datetime.datetime.strptime(LastReviewDate, '%d %B %Y')
        # locale.setlocale(locale.getdefaultlocale())
        # aggiorno la data di ultima recensione sulla tabella asset del
        # source
        if LastReviewDate != CurAssetLastReviewDate:
            gL.cSql.execute("Update SourceAsset set LastReviewDate=? where SourceAssetId=?", (LastReviewDate, cur_asset_id))

    AddrWebsite = ''
    AddrCounty  = ''
    AddrStreet  = ''
    AddrZIP     = ''
    AddrPhone   = ''      
    AddrCity    = ''               
    AddrStreet  = content.xpath('//span[@property="v:street-address"]/text()')
    AddrCity = content.xpath('//span[@property="v:locality"]/text()')
    if not AddrCity:
        AddrCity = content.xpath('//span[@property="v:municipality"]/text()')
    #AddrCounty = content.xpath('//span[@property="v:country-name"]/text()')
    AddrZIP = content.xpath('//span[@property="v:postal-code"]/text()')
    AddrPhone = content.xpath('//div[@class="fl phoneNumber"]/text()')
   
    if AddrStreet:
        AddrStreet = StdName(AddrStreet[0])
    if AddrCity:
        AddrCity = StdName(AddrCity[0])
    #if AddrCountry:  questo è il paese
    #    AddrCountry = AddrCounty[0]
    if AddrZIP:
        AddrZIP = StdZip(AddrZIP[0])
    if AddrPhone:
        AddrPhone = StdPhone(AddrPhone[0], CountryTelPrefx, CountryTelPrefx00)
    if AddrWebsite:
        AddrWebsite = AddrWebsite[0]

    AddrList = {'AddrStreet': AddrStreet,
                'AddrCity': AddrCity,
                'AddrCounty': AddrCounty,
                'AddrZIP': AddrZIP,
                'AddrPhone': AddrPhone,
                'AddrCountry': country}
    rc = sql_UpdSourceAddress(cur_asset_id, AddrList) 

    # 
    # gestione dei tag
    # 
    classify = content.xpath('//div[@class="detail"]//text()')
    tag0 = []
    for i in classify:
        if (i == '\n') or (i == '') or (i == ' '):
            continue                
        tag0.append(i)

    tag = []
    cucina = 0
    for i in tag0:
        x = StdCar(i)
        if x == 'Cucina:':
            cucina = 1
            continue
        if cucina == 1:
            cucina = 0
            i = i.split(',')
            for n in i:
                n = n.lstrip()
                tag.append(n)
        else:
            continue

    # rimuovo duplicati dalla lista
    tag = list(set(tag))
    rc = sql_ManageTag(cur_asset_id, tag, "Cucina")
    #
    # Gestione prezzo
    #
    price = 0
    cont = 0
    PriceFrom = 0
    PriceTo = 0
    PriceCurr = ''
    for i in tag0:
        if i == 'Fascia prezzo:':
            prezzo = tag0[cont + 1]
            PriceFrom = prezzo.split('-')[0].rstrip()
            PriceTo = prezzo.split('-')[1].lstrip()
            #PriceFrom  = StdCar(PriceFrom)
            #PriceTo    = StdCar(PriceTo)
            break
        cont = cont + 1
    if currency == "EUR":
        if PriceFrom != 0:
            PriceFrom = PriceFrom.replace(u'\xa0€', u'')
        if PriceTo != 0:
            PriceTo = PriceTo.replace(u'\xa0€', u'')

    PriceList = [['PriceCurr', currency],
                ['PriceFrom', PriceFrom],
                ['PriceTo', PriceTo]]
    rc = sql_ManagePrice(cur_asset_id, PriceList, currency)
    #
    # gestione recensioni
    # 
    for i in range(0, 5):
        punt = str(i + 1)
        mask1 = "\'" + punt + "\'"  # riassunto recensioni
        # mask =
        # '//div[@onclick[contains(.,"value=\'5\'")]]/following-sibling::*/text()'
        mask = '//div[@onclick[contains(.,"value=' + mask1 + '")]]/following-sibling::*/text()'
        nreview = content.xpath(mask)  # num review
        if nreview:
            nreview = locale.atoi(nreview[0])
            rc = sql_ManageReview(cur_asset_id, nreview, int(punt))

    return True

def queue_qristoranti(starturl, pageurl, page):

     # leggi la lista e inserisci asset
     lista = page.xpath('//div[@class="contentTitle"]')
     conta = 0
     for asset in lista:
        name = page.xpath('//div[@class="contentTitle"]/a//text()')[conta]
        url   = page.xpath('//div[@class="contentTitle"]/a//@href')[conta]
        name = StdName(name)
        conta = conta + 1
        o = urlparse(starturl)
        link = "http://" + o.hostname + url
        
        sql_Queue(country, assettype, source, starturl, pageurl, link, name)

def parse_qristoranti(starturl, url, name):

    # leggi la pagina di contenuti
    content = ReadPage(url)
    if content is None:
        return False
    # inserisci o aggiorna l'asset
    cur_asset_id, CurAssetLastReviewDate = sql_UpdSourceAsset(source, assettype, country, name, url)
    cerca = content.xpath('//div[@class="reviewInfo"]/text()')  # la prima che trovo e' la piu' recente

    for a in cerca:
        # cerca: Text='Ultimo aggiornamento: 21 Novembre, 2012'
        # LastReviewDate = a[0]
        tx = "Ultimo aggiornamento: "
        x = a.find(tx)
        if x <= 0:
            continue
        x = x + len(tx)
        a = a.replace(',', '')
        b = a.strip()
        c = b.replace(tx, '')
        try:
            #LastReviewDate = datetime.datetime.strptime([x:], '%d %B %Y')
            LastReviewDate = datetime.datetime.strptime(c, '%d %B %Y')
        except :
            try:
                LastReviewDate = datetime.datetime.strptime(c, '%d %b %Y')   # provo con il mese abbreviato
            except:
                LastReviewDate  = 0
            
        # locale.setlocale(locale.getdefaultlocale())
        # aggiorno la data di ultima recensione sulla tabella asset del
        # source
        if LastReviewDate != CurAssetLastReviewDate:
            gL.cSql.execute("Update SourceAsset set LastReviewDate=? where SourceAssetId=?", (LastReviewDate, cur_asset_id))
        break
        
    AddrWebsite = content.xpath('//td[contains(.,"sito")]//@href') # link al sito
    if AddrWebsite:
        AddrWebsite = AddrWebsite[0]
    ind = content.xpath('//td[contains(., "Indirizzo")]/following-sibling::td/text()')
    if len(ind) > 0:
        a = ind[0].split(",")
        AddrStreet = StdCar(a[0])
        AddrZIP = AddrCounty = AddrCity = ""
                   
    AddrPhone = content.xpath('//td[contains(., "Telefono")]/following-sibling::td/text()')[0]
    AddrPhone = StdPhone(AddrPhone, CountryTelPrefx, CountryTelPrefx00)
    AddrList = {'AddrStreet': AddrStreet,
                'AddrCity': AddrCity,
                'AddrCounty': AddrCounty,
                'AddrZIP': AddrZIP,
                'AddrPhone': AddrPhone,
                'AddrWebsite': AddrWebsite,
                'AddrCountry': country}

    rc = sql_UpdSourceAddress(cur_asset_id, AddrList)  
    
    # gestione dei tag
        
    x = content.xpath("//td[contains(., 'Tipo di cucina')]/following-sibling::td/a/text()")   # classificazione
    if x is not None:
        tag = []
        #tag.append("Cucina")
        cucina = " ".join(x[0].split())
        tag.append(cucina)
        rc = sql_ManageTag(cur_asset_id, tag, "Cucina")
    # ==================================================================================================================
    # Gestione prezzo
    # ====================================================================================================================================
    y = content.xpath('//td[contains(., "Fascia di prezzo")]/following-sibling::td/text()')
    if y:
        x = y[0]
    x = StdCar(x)
    PriceFrom = PriceTo = PriceAvg = 0
    if x is not None:
        if x == "bassa":
            PriceFrom = 5
            PriceTo = 12
        if x == "medio-bassa":
            PriceFrom = 12
            PriceTo = 25
        if x == "media":
            PriceFrom = 25
            PriceTo = 40
        if x == "medio-alta":
            PriceFrom = 40
            PriceTo = 60
        if x == "alta":
            PriceFrom = 60
            PriceTo = 100

    PriceList = [['PriceCurr', currency],
                ['PriceFrom', PriceFrom],
                ['PriceTo', PriceTo]]
    rc = sql_ManagePrice(cur_asset_id, PriceList, currency)

    # gestione recensioni
    # =====================================================================================================================
    x = content.xpath('//td[@class="rating_value average"]/text()')[0]   # valutazione
    y = content.xpath('//span[@class="count"]/text()')[0]                   # n. recensioni
    if x:
        nreview = locale.atoi(x)
    if y:
        punt = locale.atoi(y)
    rc = sql_ManageReview(cur_asset_id, nreview, punt)

    return True

def nextpage_duespaghi(url, page):
    # 
    #   DUESPAGHI - PAGINAZIONE - RICEVE UNA PAGINA, E RESTIRUISCE URL DELLA
    #   PROSSIMA PAGINA
    #
    o = urlparse(url)
    try:
        found = re.search('pag=(.+?)&', o.query).group(1)
        if found is not None:
            # l'url della paginazione
            nx = int(found) + 1
            url_a = "http://" + o.hostname + o.path + "?pag=" + str(nx) + "&ord=relevance&dir=desc"
        # controlla che esista
        rc = ReadPage(url_a)
        test = rc.xpath('//*[@class="row-identity-container"]/a/@href')  # le pagine esistono ma non hanno contenuto
        if rc is not None and test:
            return url_a

    except Exception as err:
        url_a = "http://" + o.hostname + o.path + "?pag=2&ord=relevance&dir=desc"
        # controlla che esista
        rc = ReadPage(url_a)
        test = rc.xpath('//*[@class="row-identity-container"]/a/@href')  # le pagine esistono ma non hanno contenuto
        if rc is not None and test:
            return url_a

    return False

def queue_duespaghi(starturl, pageurl, page):

    lista = page.xpath('//a[@class="clearfix"]')  # funziona
    href = page.xpath('//a[@class="clearfix"]/@href')
    nomi = page.xpath('//a[@class="clearfix"]/@title')
    n = 0
    for asset in lista:
        if not nomi or not lista or not href:
            print("Errore in lettura di ", url)
            return False
        if not href[n]:
            continue 
        name = StdName(nomi[n])
        
        url  = gL.sourcebaseurl + href[n]
               
        sql_Queue(country, assettype, source, starturl, pageurl, url, name)
        n = n + 1  # next asset


def parse_duespaghi(starturl, url, name):

    # leggi la pagina di contenuti
    content = ReadPage(url)
    
    if content is None:
        return False
    
    # inserisci/aggiorna l'asset
    cur_asset_id, CurAssetLastReviewDate = sql_UpdSourceAsset(source, assettype, country, name, url)

    LastReviewDate = content.xpath('//div[@class="metadata-text pull-left"]/text()')  # la prima che trovo e' la piu' recente
    if LastReviewDate:
        LastReviewDate = StdCar(LastReviewDate[0])
        LastReviewDate = LastReviewDate.replace('alle', ' ')
        LastReviewDate = LastReviewDate.replace(',', '')
        LastReviewDate = LastReviewDate.replace('  ', ' ')
        LastReviewDate = LastReviewDate.replace('Recensito il ', '')
        LastReviewDate = datetime.datetime.strptime(LastReviewDate, '%A %d %B %Y %H:%M').date()
        # locale.setlocale(locale.getdefaultlocale())
                    # aggiorno la data di ultima recensione sulla
                    # tabella asset del source
        if LastReviewDate != CurAssetLastReviewDate:
            gL.cSql.execute("Update SourceAsset set LastReviewDate=? where SourceAssetId=?", (LastReviewDate, cur_asset_id))

    AddrStreet = content.xpath('//span[@itemprop="streetAddress"]/text()')
    AddrCity = content.xpath('//span[@itemprop="addressLocality"]/text()')
    AddrCounty = content.xpath('//span[@itemprop="ADDRESSREGION"]/text()')
    AddrZIP = content.xpath('//span[@itemprop="postalCode"]/text()')
    AddrPhone = content.xpath('//*[@itemprop="telephone"]/text()')            
    AddrWebsite = content.xpath('//a[@itemprop="url"]/@href')
    if AddrStreet:
        AddrStreet = StdName(AddrStreet[0])
    if AddrCity:
        AddrCity = StdName(AddrCity[0])
    if AddrCounty:
        AddrCounty = AddrCounty[0]
    if AddrZIP:
        AddrZIP = StdZip(AddrZIP[0])
    if AddrPhone:
        AddrPhone = StdPhone(AddrPhone[0], CountryTelPrefx, CountryTelPrefx00)
    if AddrWebsite:
        AddrWebsite = AddrWebsite[0]
    AddrList = {'AddrStreet': AddrStreet,
                'AddrCity': AddrCity,
                'AddrCounty': AddrCounty,
                'AddrZIP': AddrZIP,
                'AddrPhone': AddrPhone,
                'AddrWebsite': AddrWebsite,
                'AddrCountry': country}
    rc = sql_UpdSourceAddress(cur_asset_id, AddrList)

    # gestione dei tag
    # 
    tag = []
    x = content.xpath('//span[@itemprop="servesCuisine"]//text()')
    y = content.xpath('//p[@class="detail-category"]//text()')
    if x:
        for i in x: 
            #tag.append("Cucina")
            cucina = StdName(i)
            tag.append(cucina)
            sql_ManageTag(cur_asset_id, tag, "Cucina")
    if y:
        for i in y: 
            #tag.append("Cucina")
            cucina = StdName(i)
            tag.append(cucina)
            sql_ManageTag(cur_asset_id, tag, "Cucina")
    #
    # gestione recensioni
    # 
    nvoti = 0
    a = content.xpath('//span[@class="review-counter clearfix"]/text()')
    if a:
        a = a[0]
        b = a.split()
        if b:
            nvoti = b[0]
        else:
            nvoti = 0
    one = content.xpath('//span[@class="fa icon-farfalla star1 on"]')
    two = content.xpath('//span[@class="fa icon-farfalla star2 on"]')
    thre = content.xpath('//span[@class="fa icon-farfalla star3 on"]')
    four = content.xpath('//span[@class="fa icon-farfalla star4 on"]')
    five = content.xpath('//span[@class="fa icon-farfalla star5 on"]')
    punt = 0
    if five:
        punt = 5
    elif four:
        punt = 4
    elif thre:
        punt = 3
    elif two:
        punt = 2
    elif one:
        punt = 1
    rc = sql_ManageReview(cur_asset_id, nvoti, punt)

    price = content.xpath('//*[@itemprop="priceRange"]/text()')
    if price:
        a = price[0]
        PriceAvg = a.replace('€', '')
        PriceList = [['PriceCur', currency],
                        ['PriceAvg', PriceAvg]]
        rc = sql_ManagePrice(cur_asset_id, PriceList, currency)

    return True


#---------------------------------------------- M A I N ----------------------------------------------------------------------------------
#
#   OPEN ODBC CONNECTION
#
# open db cursor
OpenConnectionMySql()
# coda di lavoro
work_queue = collections.deque()

#
# MAIN CYCLE: PER OGNI RECORD NELLA TABELLA DRIVE RIEMPIO LE CODE
#   StartUrl - da dove partire, incluse le pagine di paginazione trovate
#   durante
#
#
#   Parametri di esecuzione
#
paginate = False
source = False
queuerebuild = False
run_console = False

if run_console:
    sys.stdout = open('log.txt', 'w')
    sys.stderr = open('err.txt', 'w')

#   Leggo la tabella guida per ogni sorgente, paese, tipo
sql = "SELECT * FROM QDriveRun where Active = True"
gL.cSql.execute(sql)
drives = gL.cSql.fetchall()
for drive in drives:
    gL.assetbaseurl = drive['drivebaseurl']  # il baseurl per la tipologia di asset
    language = drive['countrylanguage']  # lingua
    country = drive['countryid']  # paese
    source = drive['sourceid']
    assettype = drive['assettypeid']
    gL.sourcebaseurl = drive['sourcebaseurl']
    queuerebuild = drive['queuerebuild']
    paginate = drive['paginate']
    parse = drive['parse']
    restart = drive['restart']
    sourcename = drive['sourcename']
    currency = drive['countrycurr']
    assettypeename = drive['assettypedesc']
    rundate = drive['rundate']
    rundate_end = drive['rundate_end']
    suffissofunzioni = drive['suffissofunzioni']
    CountryTelPrefx = drive['countrytelprefx']
    CountryTelPrefx00 = drive['countrytelprefx00']
    # nomi dinamici delle funzioni
    queue_fn  = "queue_" + suffissofunzioni
    nxpage_fn = "nextpage_" + suffissofunzioni
    parse_fn  = "parse_" + suffissofunzioni

    wait = drive['wait']
    if wait is None:
        wait = 0
   
    # stampo i parametri di esecuzione
    print("RUN:", gL.runnow, "SOURCE:", sourcename, "ASSET:", assettypeename, "COUNTRY:", country, "QUEUEREBUILD:", queuerebuild, "PAGINATE:", paginate, "PARSE:", parse, "RESTART:", restart)
    # controllo la congruenza
    if queuerebuild and not paginate:
        print("Errore nel run: con QUEUEREBUILD = True PAGINATE deve essere True")
        break
    if restart and queuerebuild:
        print("Errore nel run: con RESTART = True QUEUEREBUILD deve essere False")
        break
    if restart and (rundate is None):
        print("Errore nel run: con RESTART = True non trovo Rundate su Drive")
        break

    # scrivo la dataora del run sulla tabella drive
    if not restart:
        gL.cSql.execute( ("Update Drive set RunDate = ? where SourceId = ? and AssetTypeId = ? and CountryId = ?"), (gL.runnow, source, assettype, country) )

    # se richiesto cancello e ricreo la coda
    if queuerebuild:
        sql = ("Delete * from queue where sourceid = " + str(source) + " and AssetTypeId = " + str(assettype) + " and CountryId = '" + country + "'")
        gL.cSql.execute(sql)
        gL.cSql.commit()

    if language == 'ITA':
        locale.setlocale(locale.LC_ALL, '')

    #
    #if restart:   # da rifare se serve
    #    starturl, pageurl = sql_RestartUrl(country, assettype, source, rundate)
    #    if not starturl or not pageurl:
    #        print("Ma che cavolo vai dicendo? Io non ce la faccio più con te")
    #        break
    #    main_cycle(source, starturl, pageurl, paginate, parse)    

    if not restart:  
        gL.cSql.execute(("SELECT * FROM Starturl where countryid = ? AND assetTypeId = ? AND SourceId = ? and Active = ?"), (country, assettype, source, YES))
        rows = gL.cSql.fetchall()    
        if rows is None:
            print("NESSUN URL INIZIALE TROVATO")

        for row in rows:
            starturl = row['starturl']
            
            if paginate:
                #   build work queue
                sql_Queue(country, assettype, source, starturl, starturl)
                work_queue.append(starturl)
                print("Esamino: ", starturl)

                while len(work_queue):
                    pageurl = work_queue.popleft()
                    print(pageurl)
                    page = ReadPage(pageurl)
                    if page is not None:
                        # parse la pagina lista e leggi i link alle pagine degli asset
                        rc = globals()[queue_fn](starturl, pageurl, page)
                        new_pag = globals()[nxpage_fn](pageurl, page)
                        if new_pag:
                            sql_Queue(country, assettype, source, starturl, new_pag)
                            work_queue.append(new_pag)
                        gL.cSql.commit()
 
            if parse:
                # leggo dalla coda tutti i link che sono correlati allo starturl attivo e che sono attivi         
                gL.cSql.execute("SELECT * FROM Queue where countryid = ? and assetTypeId = ? and StartUrl = ? and SourceId = ? and asseturl <> ''", (country, assettype, starturl, source))
                # tutti i link presenti nella tabella starturl (tutte le pagine lista)
                rows = gL.cSql.fetchall()
                for row in rows:
                    starturl = row['starturl']
                    pageurl  = row['pageurl']
                    asseturl = row['asseturl']
                    name     = row['nome']
                    print("Esamino: ", asseturl)
                    # parse la pagina lista e leggi le singole pagine degli asset
                    rc = globals()[parse_fn](starturl, asseturl, name)
                    gL.cSql.commit()    


# decido il nome univoco dell'asset e i puntatori relativi
# connect to db e crea il cursore, creo le tabelle in memoria e le popolo
rc = NameInit(country, source, assettype)

rc = NameSimplify()
gL.cSql.commit()

# decido il nome univoco dell'asset e i puntatori relativi
rc = StdSourceAsset(country, source, assettype, True)
gL.cSql.commit()

# scrivo la dataora del run sulla tabella drive
now = SetNow()
gL.cSql.execute("Update Drive set RunDate_end = ? where SourceId = ? and AssetTypeId = ? and CountryId = ?", (now, source, assettype, country))
gL.cSql.commit()

# chiudi DB
CloseConnectionMySql()
CloseConnectionSqlite()

if run_console:
    sys.stdout.close()
    sys.stderr.close()

print("FINE")

