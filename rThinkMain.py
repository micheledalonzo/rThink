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
import rThinkGbl as gL


#---------------------------------------------------------------------------
#   COSTANTI E VAR GLOBALI
#---------------------------------------------------------------------------


def ReadPage(url):
    # legge una pagina e la restituisce
    # metodo con javascript risolto
    # pagejava = jw.get_page(link, 3)
    # content = html.fromstring(pagejava)
    try:
        time.sleep(gL.wait)
        page = requests.get(url)  # timeout=0.001 � un parametro
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
         
def queue_tripadvisor(country, assettype, source, starturl, pageurl, page):
   # leggi la lista e inserisci asset
    lista = page.xpath('//*[@class="listing" or @class="listing first"]')
    for asset in lista:
        name = asset.xpath('.//*[@class="property_title"]//text()')[0]
        name = gL.StdName(name)
        url  = asset.xpath('.//a[contains(@class,"property_title")]/@href')[0]
        url  = gL.sourcebaseurl + url
        # inserisci o aggiorna l'asset        
        rs = gL.sql_Queue(country, assettype, source, starturl, pageurl, url, name)

def parse_tripadvisor(country, source, assettype, starturl, url, name, language):

    # leggi la pagina di contenuti
    content = ReadPage(url)
    if content is  None:
        return False

    cur_asset_id, CurAssetLastReviewDate = gL.sql_InsUpdSourceAsset(source, assettype, country, name, url, language)
    LastReviewDate = content.xpath('//span[@class="ratingDate"]/text()')  # la prima che trovo e' la più recente
    if LastReviewDate:
        LastReviewDate = LastReviewDate[0]
        LastReviewDate = gL.StdCar(LastReviewDate)
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
        AddrStreet = gL.StdName(AddrStreet[0])
    if AddrCity:
        AddrCity = gL.StdName(AddrCity[0])
    #if AddrCountry:  questo � il paese
    #    AddrCountry = AddrCounty[0]
    if AddrZIP:
        AddrZIP = gL.StdZip(AddrZIP[0])
    if AddrPhone:
        AddrPhone = gL.StdPhone(AddrPhone[0], country)
    if AddrWebsite:
        AddrWebsite = AddrWebsite[0]

    AddrList = {'AddrStreet': AddrStreet,
                'AddrCity': AddrCity,
                'AddrCounty': AddrCounty,
                'AddrZIP': AddrZIP,
                'AddrPhone': AddrPhone,
                'AddrCountry': country}
    rc = gL.sql_UpdSourceAddress(cur_asset_id, AddrList) 

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
        x = gL.StdCar(i)
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
    rc = gL.sql_ManageTag(cur_asset_id, tag, "Cucina")
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
            #PriceFrom  = gL.StdCar(PriceFrom)
            #PriceTo    = gL.StdCar(PriceTo)
            break
        cont = cont + 1
    if gL.currency == "EUR":
        if PriceFrom != 0:
            PriceFrom = PriceFrom.replace(u'\xa0�', u'')
        if PriceTo != 0:
            PriceTo = PriceTo.replace(u'\xa0�', u'')

    PriceList = [['PriceCurr', gL.currency],
                ['PriceFrom', PriceFrom],
                ['PriceTo', PriceTo]]
    rc = gL.sql_ManagePrice(cur_asset_id, PriceList, gL.currency)
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
            rc = gL.sql_ManageReview(cur_asset_id, nreview, int(punt))

    return True

def queue_qristoranti(country, assettype, source, starturl, pageurl, page):

     # leggi la lista e inserisci asset
     lista = page.xpath('//div[@class="contentTitle"]')
     conta = 0
     for asset in lista:
        name = page.xpath('//div[@class="contentTitle"]/a//text()')[conta]
        url   = page.xpath('//div[@class="contentTitle"]/a//@href')[conta]
        name = gL.StdName(name)
        conta = conta + 1
        o = urlparse(starturl)
        link = "http://" + o.hostname + url
        
        gL.sql_Queue(country, assettype, source, starturl, pageurl, link, name)

def parse_qristoranti(country, source, assettype, starturl, url, name, language):

    # leggi la pagina di contenuti
    content = ReadPage(url)
    if content is None:
        return False
    # inserisci o aggiorna l'asset
    cur_asset_id, CurAssetLastReviewDate = gL.sql_InsUpdSourceAsset(source, assettype, country, name, url, language)
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
        AddrStreet = gL.StdCar(a[0])
        AddrZIP = AddrCounty = AddrCity = ""
                   
    AddrPhone = content.xpath('//td[contains(., "Telefono")]/following-sibling::td/text()')[0]
    AddrPhone = gL.StdPhone(AddrPhone)
    AddrList = {'AddrStreet': AddrStreet,
                'AddrCity': AddrCity,
                'AddrCounty': AddrCounty,
                'AddrZIP': AddrZIP,
                'AddrPhone': AddrPhone,
                'AddrWebsite': AddrWebsite,
                'AddrCountry': country}

    rc = gL.sql_UpdSourceAddress(cur_asset_id, AddrList)  
    
    # gestione dei tag
        
    x = content.xpath("//td[contains(., 'Tipo di cucina')]/following-sibling::td/a/text()")   # classificazione
    if x is not None:
        tag = []
        #tag.append("Cucina")
        cucina = " ".join(x[0].split())
        tag.append(cucina)
        rc = gL.sql_ManageTag(cur_asset_id, tag, "Cucina")
    # ==================================================================================================================
    # Gestione prezzo
    # ====================================================================================================================================
    y = content.xpath('//td[contains(., "Fascia di prezzo")]/following-sibling::td/text()')
    if y:
        x = y[0]
    x = gL.StdCar(x)
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

    PriceList = [['PriceCurr', gL.currency],
                ['PriceFrom', PriceFrom],
                ['PriceTo', PriceTo]]
    rc = gL.sql_ManagePrice(cur_asset_id, PriceList, gL.currency)

    # gestione recensioni
    # =====================================================================================================================
    x = content.xpath('//td[@class="rating_value average"]/text()')[0]   # valutazione
    y = content.xpath('//span[@class="count"]/text()')[0]                   # n. recensioni
    if x:
        nreview = locale.atoi(x)
    if y:
        punt = locale.atoi(y)
    rc = gL.sql_ManageReview(cur_asset_id, nreview, punt)

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

def queue_duespaghi(country, assettype, source, starturl, pageurl, page):

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
        name = gL.StdName(nomi[n])
        
        url  = gL.sourcebaseurl + href[n]
               
        gL.sql_Queue(country, assettype, source, starturl, pageurl, url, name)
        n = n + 1  # next asset


def parse_duespaghi(country, source, assettype, starturl, url, name, language):

    # leggi la pagina di contenuti
    content = ReadPage(url)
    
    if content is None:
        return False
    
    # inserisci/aggiorna l'asset
    cur_asset_id, CurAssetLastReviewDate = gL.sql_InsUpdSourceAsset(source, assettype, country, name, url, language)

    LastReviewDate = content.xpath('//div[@class="metadata-text pull-left"]/text()')  # la prima che trovo e' la piu' recente
    if LastReviewDate:
        LastReviewDate = gL.StdCar(LastReviewDate[0])
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
        AddrStreet = gL.StdName(AddrStreet[0])
    if AddrCity:
        AddrCity = gL.StdName(AddrCity[0])
    if AddrCounty:
        AddrCounty = AddrCounty[0]
    if AddrZIP:
        AddrZIP = gL.StdZip(AddrZIP[0])
    if AddrPhone:
        AddrPhone = gL.StdPhone(AddrPhone[0], country)
    if AddrWebsite:
        AddrWebsite = AddrWebsite[0]
    AddrList = {'AddrStreet': AddrStreet,
                'AddrCity': AddrCity,
                'AddrCounty': AddrCounty,
                'AddrZIP': AddrZIP,
                'AddrPhone': AddrPhone,
                'AddrWebsite': AddrWebsite,
                'AddrCountry': country}
    rc = gL.sql_UpdSourceAddress(cur_asset_id, AddrList)

    # gestione dei tag
    # 
    tag = []
    x = content.xpath('//span[@itemprop="servesCuisine"]//text()')
    y = content.xpath('//p[@class="detail-category"]//text()')
    if x:
        for i in x: 
            #tag.append("Cucina")
            cucina = gL.StdName(i)
            tag.append(cucina)
            gL.sql_ManageTag(cur_asset_id, tag, "Cucina")
    if y:
        for i in y: 
            #tag.append("Cucina")
            cucina = gL.StdName(i)
            tag.append(cucina)
            gL.sql_ManageTag(cur_asset_id, tag, "Cucina")
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
    rc = gL.sql_ManageReview(cur_asset_id, nvoti, punt)

    price = content.xpath('//*[@itemprop="priceRange"]/text()')
    if price:
        a = price[0]
        PriceAvg = a.replace('�', '')
        PriceList = [['PriceCur', gL.currency],
                        ['PriceAvg', PriceAvg]]
        rc = gL.sql_ManagePrice(cur_asset_id, PriceList, gL.currency)

    return True

def main_cycle(country, source, starturl, pageurl, paginate, parse, assettype, rundate, language, runlogid=0):    

    if paginate:
        #   build work queue
        gL.sql_Queue(country, assettype, source, starturl, pageurl)
        work_queue.append(pageurl)
        print("Esamino: ", pageurl)

        while len(work_queue):
            pageurl = work_queue.popleft()
            print(pageurl)
            page = ReadPage(pageurl)
            if page is not None:
                # parse la pagina lista e leggi i link alle pagine degli asset
                rc = globals()[gL.queue_fn](country, assettype, source, starturl, pageurl, page)
                new_pag = globals()[gL.nxpage_fn](pageurl, page)
                if new_pag:
                    gL.sql_Queue(country, assettype, source, starturl, new_pag)
                    work_queue.append(new_pag)
                    gL.sql_RunLogCreate(source, assettype, country, paginate, parse, queuerebuild, gL.wait, starturl, new_pag)
                gL.cSql.commit()
 
    if parse:
        if gL.restart:
            gL.cSql.execute("select * from runlog where RunId = ? and SourceId = ? and AssettypeId = ? and CountryId = ? and Restart = ?", (gL.RunId, source, assettype, country, gL.restart))
            rest = gL.cSql.fetchall()
            for res in rest:
                res_pageurl = res['pageurl']
                # leggo dalla coda tutti i link che non ho ancora esaminato        
                gL.cSql.execute("SELECT * FROM Queue where countryid = ? and assetTypeId = ? and StartUrl = ?  \
                                                       and SourceId = ? and PageUrl = ? and asseturl <> '' ", \
                                                          (country, assettype, starturl, source, res_pageurl, rundate))
        else:
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
            rc = gL.sql_RunLogUpdateStart(source, assettype, country, starturl, pageurl)
            # parse la pagina lista e leggi le singole pagine degli asset
            rc = globals()[gL.parse_fn](country, source, assettype, starturl, asseturl, name, language)
            # aggiorno il log del run con la pagina finita 
            rc = gL.sql_RunLogUpdateEnd(source, assettype, country, starturl, pageurl)
            gL.cSql.commit()    


def RunRestart(runid):
    gL.restart = True
    queuerebuild = False

    # da runlog leggo le righe con chiave source/asset/country con data di start massima e data di end nulla
    sql = "SELECT SourceId, AssetTypeId, CountryId, Max(RunLog.RunStart) AS MaxDiRunStart, SourceId, CountryId, AssetTypeId, StartUrl, Wait, First(Pageurl) AS PrimoDiPageurl, RunId \
           FROM RunLog WHERE (((RunEnd) Is Null)) GROUP BY SourceId, AssetTypeId, CountryId, SourceId, CountryId, AssetTypeId, StartUrl, Wait, RunId ORDER BY Max(RunStart) DESC"
    gL.cSql.execute(sql)
    logs = gL.cSql.fetchall()   # l'ultima
    for log in logs:
        source      = log['sourceid']
        assettype   = log['assettypeid']
        country     = log['countryid']
        starturl    = log['starturl']
        pageurl     = log['primodipageurl']
        gL.wait     = log['wait']
        gL.RunId    = log['runid']
        gL.RunLogId = log['runlogid']
        gL.cSql.execute("select * from qdriverun where SourceId = ? and AssetTypeId = ? and CountryId = ?", (source, assettype, country))
        drive = gL.cSql.fetchone()   # l'ultima
        if not drive:
            print("Non ho trovato la riga di Drive per ", source, assettype, country)
            return False
        
        gL.assetbaseurl = drive['drivebaseurl']  # il baseurl per la tipologia di asset
        language = drive['countrylanguage']  # lingua
        country = drive['countryid']  # paese
        source = drive['sourceid']
        assettype = drive['assettypeid']
        gL.sourcebaseurl = drive['sourcebaseurl']
        
        paginate = drive['paginate']
        parse = drive['parse']
        sourcename = drive['sourcename']
        gL.currency = drive['countrycurr']
        assettypeename = drive['assettypedesc']
        rundate = drive['rundate']
        rundate_end = drive['rundate_end']
        suffissofunzioni = drive['suffissofunzioni']
        # nomi dinamici delle funzioni
        gL.queue_fn  = "queue_" + suffissofunzioni
        gL.nxpage_fn = "nextpage_" + suffissofunzioni
        gL.parse_fn  = "parse_" + suffissofunzioni

        gL.wait = drive['wait']
        if gL.wait is None:
            gL.wait = 0

        # stampo i parametri di esecuzione
        print("RUN:", gL.RunDate, "SOURCE:", sourcename, "ASSET:", assettypeename, "COUNTRY:", country, "QUEUEREBUILD:", queuerebuild, "PAGINATE:", paginate, "PARSE:", parse, "RESTART:", gL.restart)
        # controllo la congruenza
        if not gL.OkParam(queuerebuild, paginate, gL.restart, rundate):
            return False
        
        if language == 'ITA': locale.setlocale(locale.LC_ALL, '')
        
        #gL.cSql.execute(("SELECT * FROM Starturl where countryid = ? AND assetTypeId = ? AND SourceId = ? and Active = ?"), (country, assettype, source, gL.YES))
        #rows = gL.cSql.fetchall()    
        #if rows is None:
        #    print("NESSUN URL INIZIALE TROVATO")
        #    return False

        #for row in rows:
        rc = gL.sql_RunLogUpdateStart(source, assettype, country, starturl, pageurl)
        main_cycle(country, source, starturl, pageurl, paginate, parse, assettype, rundate, language)
        rc = gL.sql_RunLogUpdateEnd(source, assettype, country, starturl, pageurl)
        gL.cSql.commit()

    return True



def RunNormale():
    
    # setto il runid
    gL.RunId = gL.sql_RunId("START")

    # scrivo il log per gli starturl
    sql = "SELECT * FROM QDriveRun where Drive.Active = True"
    gL.cSql.execute(sql)
    drives = gL.cSql.fetchall()
    for drive in drives:
        country = drive['countryid']  # paese
        assettype = drive['assettypeid']
        source = drive['sourceid']
        starturl = drive['starturl']
        queuerebuild = drive['queuerebuild']
        paginate = drive['paginate']
        parse = drive['parse']
        gL.wait = drive['wait']
        pageurl = ''
        # insert tutti gli startrun
        Rc = gL.sql_RunLogCreate(source, assettype, country, paginate, parse, queuerebuild, gL.wait, starturl, pageurl)
        gL.cSql.commit()         

    #   Leggo la tabella guida per ogni sorgente, paese, tipo
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
        sourcename = drive['sourcename']
        gL.currency = drive['countrycurr']
        assettypeename = drive['assettypedesc']
        rundate = drive['rundate']
        rundate_end = drive['rundate_end']
        suffissofunzioni = drive['suffissofunzioni']
        # nomi dinamici delle funzioni
        gL.queue_fn  = "queue_" + suffissofunzioni
        gL.nxpage_fn = "nextpage_" + suffissofunzioni
        gL.parse_fn  = "parse_" + suffissofunzioni

        wait = drive['wait']
        if wait is None:
            wait = 0
 
        # stampo i parametri di esecuzione
        print("RUN:", gL.RunDate, "SOURCE:", sourcename, "ASSET:", assettypeename, "COUNTRY:", country, "QUEUEREBUILD:", queuerebuild, "PAGINATE:", paginate, "PARSE:", parse, "RESTART:", gL.restart)
        # controllo la congruenza
        if not gL.OkParam(queuerebuild, paginate, gL.restart, rundate):
            return False

        # se richiesto cancello e ricreo la coda
        if queuerebuild:
            sql = ("Delete * from queue where sourceid = " + str(source) + " and AssetTypeId = " + str(assettype) + " and CountryId = '" + country + "'")
            gL.cSql.execute(sql)
            gL.cSql.commit()

        if language == 'ITA': locale.setlocale(locale.LC_ALL, '')

        #
        gL.cSql.execute(("SELECT * FROM Starturl where countryid = ? AND assetTypeId = ? AND SourceId = ? and Active = ?"), (country, assettype, source, gL.YES))
        rows = gL.cSql.fetchall()    
        if rows is None:
            print("NESSUN URL INIZIALE TROVATO")
            return False

        for row in rows:
            starturl = row['starturl']
            rc = gL.sql_RunLogUpdateStart(source, assettype, country, starturl, '')
            main_cycle(country, source, starturl, starturl, paginate, parse, assettype, rundate, language)
            rc = gL.sql_RunLogUpdateEnd(source, assettype, country, starturl, '')
            gL.cSql.commit()
    
    return True


#---------------------------------------------- M A I N ----------------------------------------------------------------------------------
# apri connessione e cursori, carica keywords in memoria

gL.SqLite, gL.C = gL.OpenConnectionSqlite()
gL.MySql, gL.Cursor = gL.OpenConnectionMySql()
work_queue = collections.deque()
rc = gL.sql_CreateMemTableKeywords()
rc = gL.sql_CopyKeywordsInMemory()
#
# MAIN 
#
paginate = False
source = False
queuerebuild = False
run_console = False

if run_console:
    sys.stdout = open('log.txt', 'w')
    sys.stderr = open('err.txt', 'w')

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
    rc = RunRestart(runid)
    if not rc:
        sys.exit()
else:
    rc = RunNormale()
    if not rc:   # controllo parametri non valido
        sys.exit()

#era qui
rc = gL.sql_RunId("END")
gL.cSql.commit()

# decido il nome univoco dell'asset e i puntatori relativi
#rc = gL.StdSourceAsset(country, source, assettype, True)
#gL.cSql.commit()

# scrivo la dataora del run sulla tabella drive
now = gL.SetNow()
gL.cSql.execute("Update Drive set RunDate_end = ? where active = True", (now))
gL.cSql.commit()

# chiudi DB
gL.CloseConnectionMySql()
gL.CloseConnectionSqlite()

if run_console:
    sys.stdout.close()
    sys.stderr.close()

print("FINE")