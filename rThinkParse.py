# -*- coding: cp1252 -*-.

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
import requests
import logging
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)


def ParseQueue(country,assettype, source, starturl, pageurl, page):   
        
    return globals()[gL.queue_fn](country, assettype, source, starturl, pageurl, page)
    
def ParsePage(country, assettype, source, starturl, asseturl, name):   
     
    return  globals()[gL.parse_fn](country, assettype, source, starturl, asseturl, name) 
    
    
def ParseNextPage(pageurl, page):

    return globals()[gL.nxpage_fn](pageurl, page)

def ReadPage(url):
    gL.log(gL.DEBUG)

    # legge una pagina e la restituisce
    # metodo con javascript risolto
    # pagejava = jw.get_page(link, 3)
    # content = html.fromstring(pagejava)
    try:
        time.sleep(gL.wait)
        page = requests.get(url)  # timeout=0.001 è un parametro
        if page.status_code != requests.codes.ok:
            msg ="%s - %s" % (page.status_code, url)
            gL.log(gL.ERROR, msg)
            return None
    except requests.exceptions.RequestException as e:
        gL.log(gL.ERROR, e)
        return None

    content = html.fromstring(page.content)

    import tempfile
    tempfile.tempdir = "C:/Users/michele.dalonzo/Documents/Projects/rThink/Temp/"
    f = tempfile.NamedTemporaryFile(mode='w',delete=False)
    f.write(str(page.content))
    f.close()

    return content

def nextpage_tripadvisor(url, page):
    gL.log(gL.DEBUG, "nextpage_tripadvisor")
    
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
    gL.log(gL.DEBUG)
    
    # get la prossima pagina lista e inseriscila nella coda di lavoro e nella
    # tabella starturl
    # per tutti i link rel next
    pagact = page.xpath('//span[@class="inactive"]/text()')   # pagina attuale, se zero non c'è paginazione,
    if len(pagact) == 0:
        return False
    curpa = int(pagact[0])
    links = page.xpath('//a[@class="paginate"]/@href')
    numpa = page.xpath('//a[@class="paginate"]/text()')
    if numpa[0] is not None:
        if int(numpa[0]) > int(curpa):
            return(links[0])
    return False
         
def queue_tripadvisor(country, assettype, source, starturl, pageurl, page):
    gL.log(gL.DEBUG)
    # leggi la lista e inserisci asset
    lista = page.xpath('//*[@class="listing" or @class="listing first"]')
    for asset in lista:
        name = asset.xpath('.//*[@class="property_title"]//text()')[0]
        name = gL.StdName(name)
        url  = asset.xpath('.//a[contains(@class,"property_title")]/@href')[0]
        url  = gL.sourcebaseurl + url
        # inserisci o aggiorna l'asset        
        rs = gL.sql_Queue(country, assettype, source, starturl, pageurl, url, name)
    
    return True

def parse_tripadvisor(country, assettype, source, starturl, url, name):
    gL.log(gL.DEBUG)
    # leggi la pagina di contenuti
    content = ReadPage(url)
    if content is  None:
        return False

    cur_asset_id, CurAssetLastReviewDate = gL.sql_InsUpdSourceAsset(source, assettype, country, name, url)
    LastReviewDate = content.xpath('//span[@class="ratingDate"]/text()')  # la prima che trovo e' la piÃ¹ recente
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
            PriceFrom = PriceFrom.replace(u'\xa0€', u'')
        if PriceTo != 0:
            PriceTo = PriceTo.replace(u'\xa0€', u'')

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
     gL.log(gL.DEBUG)
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

     return True

def parse_qristoranti(country, assettype, source, starturl, url, name):
    gL.log(gL.DEBUG)
    # leggi la pagina di contenuti
    content = ReadPage(url)
    if content is None:
        return False
    # inserisci o aggiorna l'asset
    cur_asset_id, CurAssetLastReviewDate = gL.sql_InsUpdSourceAsset(source, assettype, country, name, url)
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
                   
    test = content.xpath('//td[contains(., "Telefono")]/following-sibling::td/text()')
    if len(test) > 0:
        AddrPhone = test[0]
    else:
        AddrPhone = ''
    AddrPhone = gL.StdPhone(AddrPhone, country)
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
    gL.log(gL.DEBUG)
     
    #   DUESPAGHI - PAGINAZIONE - RICEVE UNA PAGINA, E RESTIRUISCE URL DELLA NEXT
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
    gL.log(gL.DEBUG)
    lista = page.xpath('//a[@class="clearfix"]')  # funziona
    href = page.xpath('//a[@class="clearfix"]/@href')
    nomi = page.xpath('//a[@class="clearfix"]/@title')
    n = 0
    for asset in lista:
        if not nomi or not lista or not href:
            msg ="%s - %s" % ("Errore get ", url)
            gL.log(gL.ERROR, msg)
            #print("Errore in lettura di ", url)
            return False
        if not href[n]:
            continue 
        name = gL.StdName(nomi[n])
        
        url  = gL.sourcebaseurl + href[n]
               
        gL.sql_Queue(country, assettype, source, starturl, pageurl, url, name)
        n = n + 1  # next asset
    
    return True

def parse_duespaghi(country, assettype, source, starturl, url, name):
    gL.log(gL.DEBUG)
    # leggi la pagina di contenuti
    content = ReadPage(url)
    
    if content is None:
        return False
    
    # inserisci/aggiorna l'asset
    cur_asset_id, CurAssetLastReviewDate = gL.sql_InsUpdSourceAsset(source, assettype, country, name, url)

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
        PriceAvg = a.replace('€', '')
        PriceList = [['PriceCur', gL.currency],
                     ['PriceAvg', PriceAvg]]
        rc = gL.sql_ManagePrice(cur_asset_id, PriceList, gL.currency)

    return True