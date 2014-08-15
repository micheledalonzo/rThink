# -*- coding: cp1252 -*-.

import requests
from lxml import html
from lxml import etree
import collections
import pypyodbc
import datetime
import time
import re
import sys
import locale
import random
# import jabba_webkit as jw
from urllib.parse import urlparse
import rThinkGbl as gL
import requests
import logging
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)


def DriveParseQueue(country,assettype, source, starturl, pageurl, page):           
    return globals()[gL.queue_fn](country, assettype, source, starturl, pageurl, page)
    
def DrivePageParse(country, assettype, source, starturl, asseturl, name):   
    AssetId = gL.sql_InsUpdSourceAsset(source, assettype, country, name, asseturl)  # inserisco l'asset
    return  globals()[gL.parse_fn](country, assettype, source, starturl, asseturl, name, AssetId) 
        
def DriveParseNextPage(pageurl, page):
    return globals()[gL.nxpage_fn](pageurl, page)

def ReadPageOld(url):
    #gL.log(gL.DEBUG, url)

    # legge una pagina e la restituisce
    # metodo con javascript risolto
    # pagejava = jw.get_page(link, 3) attesa secondi
    # content = html.fromstring(pagejava)
    while True:
        count = 0
        try:
            attesa = random.randint(1, 7) # da 1 a 4 sec 
            count = count + 1
            time.sleep(attesa)
            if gL.Useproxy:
                rand_proxy = random.choice(gL.Proxies)
                proxy = {}
                proxy = {"http" : rand_proxy} 
                gL.log(gL.DEBUG, "proxy=" + rand_proxy)
                page = requests.get(url,proxies=proxy)  
            else:
                page = requests.get(url)              
            if page.status_code != requests.codes.ok:
                msg ="%s - %s" % (page.status_code, url)
                gL.log(gL.ERROR, msg)
                return None
            else:
                break # ho letto, ok
        except requests.exceptions.RequestException as e:
            if gL.Useproxy: 
                msg = "Errore dal server o dal proxy, riprovo n. " + str(count) + proxy + e                
            else:
                msg = "Tentativo n. " + str(count) + proxy + e                
            gL.log(gL.ERROR, e)
            if count > 10:   # riprovo dieci volte e poi esco dal loop
                return None
            else:
                count = count + 1
            

    content = html.fromstring(page.content)

    import tempfile
    tempfile.tempdir = "C:/Users/michele.dalonzo/Documents/Projects/rThink/Temp/"
    f = tempfile.NamedTemporaryFile(mode='w',delete=False)
    f.write(str(page.content))
    f.close()

    return content

def ReadPage(url):
    gL.log(gL.INFO, "ReadPage=" + url)
    max = 10; n = 0
    while True:
        try:
            n = n + 1
            attesa = random.randint(1, 7) # da 1 a 4 sec 
            time.sleep(attesa)
            if gL.Useproxy:
                rand_proxy = random.choice(gL.Proxies)
                proxy = {}
                proxy = {"http" : rand_proxy} 
                gL.log(gL.DEBUG, "proxy=" + rand_proxy)
                page = requests.get(url,proxies=proxy)  
            else:
                page = requests.get(url)  
            page.raise_for_status()       
            content = html.fromstring(page.content)
            return content
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            msg ="HTTP %s - %s" % (status_code, url)
            gL.log(gL.WARNING, msg)
            if status_code == 500 or status_code == 502:
                if n > max: 
                    gL.log(gL.WARNING, "FINE MAX TENTATIVI DI LETTURA PER URL=" + url + "STATUS=" + str(status_code))
                    break
                else:
                    #gL.log(gL.WARNING, "HTTP=" + str(status_code) + "url=" + url)
                    continue   # ritenta sarai pià fortunato
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:            
            n = n + 1
            if n > max:
                break
            attesa = random.randint(1, 10) # da 1 a 4 sec 
            time.sleep(attesa)
            continue
        except requests.exceptions.RequestException as e:  # altro errore qualsiasi
            gL.log(gL.ERROR, e)
            break

    return None

def NextpageTripadvisor(url, page):
    try:
        gL.log(gL.DEBUG)
    
        # get la prossima pagina lista e inseriscila nella coda di lavoro e nella
        # tabella starturl
        # per tutti i link rel next
        links = page.xpath('//link[@rel="next"]/@href')
        for link in links:
             # link = gL.assetbaseurl + link
             # controllo che esista e l'inserisco nella coda
             url = gL.assetbaseurl + link
             newpage = ReadPage(url) 
             if newpage is not None:
                return url, newpage
             else:
                return False, qui

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False, qui
         
def NextpageDuespaghi(url, page):

    try:
        gL.log(gL.DEBUG)
     
        #   DUESPAGHI - PAGINAZIONE - RICEVE UNA PAGINA, E RESTIRUISCE URL DELLA NEXT
        o = urlparse(url)
        found = re.search('pag=(.+?)&', o.query).group(1)
        if found is not None:
            # l'url della paginazione
            nx = int(found) + 1
            url_a = "http://" + o.hostname + o.path + "?pag=" + str(nx) + "&ord=relevance&dir=desc"
        # controlla che esista
        page = ReadPage(url_a)
        if page is None:
            return False, qui
        test = rc.xpath('//*[@class="row-identity-container"]/a/@href')  # le pagine esistono ma non hanno contenuto
        if rc is not None and test:
            return url_a, page

    except Exception as err:
        url_a = "http://" + o.hostname + o.path + "?pag=2&ord=relevance&dir=desc"  # se non trovo il numero pagina vuol dire che è la prima pagina, 
        # controlla che esista
        newpage = ReadPage(url_a)
        if newpage is not None:
            test = rc.xpath('//*[@class="row-identity-container"]/a/@href')  # controllo che la seconda esista con del contenuto
            if test:
                return url_a, newpage
            else:
                return False, qui

    return False, qui

def NextpageViamichelin(url, page):
    try:
        # format: http://www.viamichelin.it/web/Ristoranti/Ristoranti-Italia?page=2
        gL.log(gL.DEBUG)
     
        #   uso il numero della pagina nell'url, controllando che esista
        o = urlparse(url)
        try:
            #found = re.search('page=(.+?)&', o.query).group(1)
            found = re.search('page=(.+)', o.query).group(1)
            if found is not None:
                # l'url della paginazione
                nx = int(found) + 1
                url_a = "http://" + o.hostname + o.path + "?page=" + str(nx) 
            # controlla che esista
            newpage = ReadPage(url_a)
            if newpage is not None:                
                test = newpage.xpath('//div[@id="noResult"]')  # le pagine esistono ma non hanno contenuto
                if len(test) == 0:
                    return url_a, newpage
                else:
                    return False, qui

        except Exception as err:
            url_a = "http://" + o.hostname + o.path + "?page=2"  # se non trovo il numero pagina vuol dire che è la prima pagina, 
            # controlla che esista
            newpage = ReadPage(url_a)
            if newpage is not None:
                test = newpage.xpath('//div[@id="noResult"]')  # controllo che la seconda esista con del contenuto (questo è il messaggio "No result"
                if len(test) == 0:
                    return url_a, newpage
            else:
                return False, qui

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False, qui

    return False

def NextpageQristoranti(url, page):
    try:
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
                newpage = ReadPage(links[0])
                if newpage is not None:
                    return(links[0], newpage)
                else:
                    return False, qui
            else:
                return False, qui               

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False, qui

    return False

def ParseTripadvisor(country, assettype, source, starturl, url, name, AssetId):

    try:
        gL.log(gL.DEBUG)
        # leggi la pagina di contenuti
        content = ReadPage(url)
        if content is  None:
            return False

        LastReviewDate = content.xpath('//span[@class="ratingDate"]/text()')  # la prima che trovo e' la piÃ¹ recente
        if LastReviewDate:
            LastReviewDate = LastReviewDate[0]
            LastReviewDate = gL.StdCar(LastReviewDate)
            LastReviewDate = LastReviewDate.replace('Recensito il ', '')
            LastReviewDate = datetime.datetime.strptime(LastReviewDate, '%d %B %Y')
            LastReviewDate = datetime.datetime.combine(LastReviewDate, datetime.time(0, 0))  # mettila in formato datetime.datetime

            # aggiorno la data di ultima recensione sulla tabella asset del source
            rc = gL.sql_UpdLastReviewDate(AssetId, LastReviewDate)

        AddrWebsite = ''
        AddrCounty  = ''
        AddrStreet  = ''
        AddrZIP     = ''
        AddrPhone   = ''      
        AddrPhone1  = ''      
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
            #AddrPhone = gL.StdPhone(AddrPhone[0], country)
            AddrPhone, AddrPhone1 = gL.StdPhone(AddrPhone[0], country)
        if AddrWebsite:
            AddrWebsite = AddrWebsite[0]

        AddrList = {'AddrStreet': AddrStreet,
                    'AddrCity': AddrCity,
                    'AddrCounty': AddrCounty,
                    'AddrZIP': AddrZIP,
                    'AddrPhone': AddrPhone,
                    'AddrPhone1': AddrPhone1,
                    'AddrCountry': country}
        rc = gL.sql_UpdSourceAddress(AssetId, AddrList) 

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
        rc = gL.sql_ManageTag(AssetId, tag, "Cucina")
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
        rc = gL.sql_ManagePrice(AssetId, PriceList, gL.currency)
    
        # gestione recensioni    
        for i in range(0, 5):
            punt = str(i + 1)
            mask1 = "\'" + punt + "\'"  # riassunto recensioni
            # mask =
            # '//div[@onclick[contains(.,"value=\'5\'")]]/following-sibling::*/text()'
            mask = '//div[@onclick[contains(.,"value=' + mask1 + '")]]/following-sibling::*/text()'
            nreview = content.xpath(mask)  # num review
            if nreview:
                nreview = locale.atoi(nreview[0])
                rc = gL.sql_ManageReview(AssetId, nreview, int(punt))
    
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

    return True

def ParseDuespaghi(country, assettype, source, starturl, url, name, AssetId):

    try:    
        gL.log(gL.DEBUG)
        # leggi la pagina di contenuti
        content = ReadPage(url)
    
        if content is None:
            return False

        LastReviewDate = content.xpath('//div[@class="metadata-text pull-left"]/text()')  # la prima che trovo e' la piu' recente
        if LastReviewDate:
            LastReviewDate = gL.StdCar(LastReviewDate[0])
            LastReviewDate = LastReviewDate.replace('alle', ' ')
            LastReviewDate = LastReviewDate.replace(',', '')
            LastReviewDate = LastReviewDate.replace('  ', ' ')
            LastReviewDate = LastReviewDate.replace('Recensito il ', '')
            LastReviewDate = datetime.datetime.strptime(LastReviewDate, '%A %d %B %Y %H:%M').date()
            LastReviewDate = datetime.datetime.combine(LastReviewDate, datetime.time(0, 0))  # mettila in formato datetime.datetime
            # aggiorno la data di ultima recensione sulla tabella asset del source
            rc = gL.sql_UpdLastReviewDate(AssetId, LastReviewDate)


        AddrWebsite = ''
        AddrCounty  = ''
        AddrStreet  = ''
        AddrZIP     = ''
        AddrPhone   = ''      
        AddrPhone1  = ''      
        AddrCity    = ''               
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
            AddrPhone, AddrPhone1 = gL.StdPhone(AddrPhone[0], country)
        if AddrWebsite:
            AddrWebsite = AddrWebsite[0]
        AddrList = {'AddrStreet': AddrStreet,
                    'AddrCity': AddrCity,
                    'AddrCounty': AddrCounty,
                    'AddrZIP': AddrZIP,
                    'AddrPhone': AddrPhone,
                    'AddrPhone1': AddrPhone1,
                    'AddrWebsite': AddrWebsite,
                    'AddrCountry': country}
        rc = gL.sql_UpdSourceAddress(AssetId, AddrList)

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
                gL.sql_ManageTag(AssetId, tag, "Cucina")
        if y:
            for i in y: 
                #tag.append("Cucina")
                cucina = gL.StdName(i)
                tag.append(cucina)
                gL.sql_ManageTag(AssetId, tag, "Cucina")
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
        rc = gL.sql_ManageReview(AssetId, nvoti, punt)

        price = content.xpath('//*[@itemprop="priceRange"]/text()')
        if price:
            a = price[0]
            PriceAvg = a.replace('€', '')
            PriceList = [['PriceCur', gL.currency],
                            ['PriceAvg', PriceAvg]]
            rc = gL.sql_ManagePrice(AssetId, PriceList, gL.currency)
    
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

    return True

def ParseViamichelin(country, assettype, source, starturl, url, name, AssetId):
        
    try:    
        gL.log(gL.DEBUG)
        # leggi la pagina di contenuti
        content = ReadPage(url)
        if content is  None:
            return False
    
        #LastReviewDate = content.xpath('//span[@class="ratingDate"]/text()')  # la prima che trovo e' la piu' recente
        #if LastReviewDate:
        #    LastReviewDate = LastReviewDate[0]
        #    LastReviewDate = gL.StdCar(LastReviewDate)
        #    LastReviewDate = LastReviewDate.replace('Recensito il ', '')
        #    LastReviewDate = datetime.datetime.strptime(LastReviewDate, '%d %B %Y')
        #    # aggiorno la data di ultima recensione sulla tabella asset del source
        #    if LastReviewDate != CurAssetLastReviewDate:
        #        gL.cSql.execute("Update SourceAsset set LastReviewDate=? where SourceAssetId=?", (LastReviewDate, AssetId))
        indirizzo = ''
        addr = content.xpath('//li[@class="vm-clear"]//text()')
        for add in addr:
            if add == ''           or \
               add == ' '          or \
               add == "Vedi mappa" or \
               add == "Indirizzo"  or \
               add == " : \xa0":
                continue
            indirizzo = add
        indirizzo = indirizzo + " " + country       

        telefono = (content.xpath('//a[contains(@href, "tel:")]//text()'))
        if telefono:
            AddrPhone, AddrPhone1 = gL.StdPhone(telefono[0], country)

        sito = (content.xpath('//li[contains(., "Sito internet")]//@href'))
        if sito:
            AddrWebsite = sito[0]
        AddrList = {'AddrStreet': '',
                    'AddrCity': '',
                    'AddrCounty': '',
                    'AddrZIP': '',
                    'AddrPhone': AddrPhone,
                    'AddrPhone1': '',
                    'AddrWebsite': AddrWebsite,
                    'AddrCountry': country}
        rc = gL.sql_UpdSourceAddress(AssetId, AddrList, indirizzo) 
       
        # gestione dei tag             
        classify = (content.xpath('//div[@class="fleft"]//text()'))
        #Text=''
        #Text='Cucina :'
        #Text='regionale'
        tag = []
        ok = 0
        for i in classify:
            if (i == '\n') or (i == '') or (i == ' '):
                continue                
            if i == "Cucina :":
                ok = 1
                continue
            if ok == 1:    
                tag.append(i)

        # rimuovo duplicati dalla lista
        rc = gL.sql_ManageTag(AssetId, tag, "Cucina")
    
        # Gestione prezzo    
        test = content.xpath('//span[@class="priceFrom parseClass itemPoisOn"]//text()')
        #Text='Da'
        #Text='30 EUR'
        #Text='a'
        #Text='56 EUR'
        #Text=''
        PriceFrom = 0; PriceTo = 0
        for idx, a in enumerate(test):
            if a == "  Da " and (len(test) - idx) > 1:
                PriceFrom = test[idx+1].replace(u'EUR', u'')
            if a == " a "  and (len(test) - idx) > 1:
                PriceTo   = test[idx+1].replace(u'EUR', u'')
        PriceList = [['PriceCurr', gL.currency],
                    ['PriceFrom', PriceFrom],
                    ['PriceTo', PriceTo]]
        rc = gL.sql_ManagePrice(AssetId, PriceList, gL.currency)
    
        # gestione recensioni    
        punt = 0; nreview = 1 # ispettore viamichelin
        lev1 = content.xpath('//span[@class="pStars1 pRating pStarsImg parseClass"]')
        lev2 = content.xpath('//span[@class="pStars2 pRating pStarsImg parseClass"]')
        lev3 = content.xpath('//span[@class="pStars3 pRating pStarsImg parseClass"]')
        lev4 = content.xpath('//span[@class="pStars4 pRating pStarsImg parseClass"]')   
        if len(lev4) > 0:
            punt = 4
        if len(lev3) > 0:
            punt = 3
        if len(lev2) > 0:
            punt = 2
        if len(lev1) > 0:    # bib gourmand locali ottimo rapporto qualità/prezzo
            punt = 1
        if punt > 0:
            rc = gL.sql_ManageReview(AssetId, nreview, punt)

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False
    
    return True

def ParseQristoranti(country, assettype, source, starturl, url, name, AssetId):
    try:
        gL.log(gL.DEBUG)
        # leggi la pagina di contenuti
        content = ReadPage(url)
        if content is None:
            return False

        cerca = content.xpath('//div[@class="reviewInfo"]/text()')  # la prima che trovo e' la piu' recente
        LastReviewDate = ''
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
                LastReviewDate = datetime.datetime.strptime(c, '%d %B %Y')
                LastReviewDate = datetime.datetime.combine(LastReviewDate, datetime.time(0, 0))  # mettila in formato datetime.datetime
            except :
                try:
                    LastReviewDate = datetime.datetime.strptime(c, '%d %b %Y')   # provo con il mese abbreviato
                    LastReviewDate = datetime.datetime.combine(LastReviewDate, datetime.time(0, 0))  # mettila in formato datetime.datetime
                except:
                    pass
        if LastReviewDate != '':            
            # aggiorno la data di ultima recensione sulla tabella asset del source
            rc = gL.sql_UpdLastReviewDate(AssetId, LastReviewDate)
        
        AddrWebsite = ''
        AddrCounty  = ''
        AddrStreet  = ''
        AddrZIP     = ''
        AddrPhone   = ''      
        AddrPhone1  = ''      
        AddrCity    = ''               
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
        #AddrPhone = gL.StdPhone(AddrPhone, country)
        AddrPhone, AddrPhone1 = gL.StdPhone(AddrPhone, country)

        AddrList = {'AddrStreet': AddrStreet,
                    'AddrCity': AddrCity,
                    'AddrCounty': AddrCounty,
                    'AddrZIP': AddrZIP,
                    'AddrPhone': AddrPhone,
                    'AddrPhone1': AddrPhone1,
                    'AddrWebsite': AddrWebsite,
                    'AddrCountry': country}

        rc = gL.sql_UpdSourceAddress(AssetId, AddrList)  
    
        # gestione dei tag
        
        x = content.xpath("//td[contains(., 'Tipo di cucina')]/following-sibling::td/a/text()")   # classificazione
        if x:
            tag = []
            #tag.append("Cucina")
            cucina = " ".join(x[0].split())
            tag.append(cucina)
            rc = gL.sql_ManageTag(AssetId, tag, "Cucina")
        # 
        # Gestione prezzo
        # 
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
        rc = gL.sql_ManagePrice(AssetId, PriceList, gL.currency)

        # gestione recensioni
        # 
        x = content.xpath('//td[@class="rating_value average"]/text()')[0]   # valutazione
        y = content.xpath('//span[@class="count"]/text()')[0]                   # n. recensioni
        if x:
            nreview = locale.atoi(x)
        if y:
            punt = locale.atoi(y)
        rc = gL.sql_ManageReview(AssetId, nreview, punt)

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

    return True

def QueueTripadvisor(country, assettype, source, starturl, pageurl, page):
    try:
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

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False
    
    return True

def QueueDuespaghi(country, assettype, source, starturl, pageurl, page):
    try:
        gL.log(gL.DEBUG)
        lista = page.xpath('//a[@class="clearfix"]')  # funziona
        href = page.xpath('//a[@class="clearfix"]/@href')
        nomi = page.xpath('//a[@class="clearfix"]/@title')
        n = 0
        if nomi is None or href is None:
            msg ="%s - %s" % ("Parsing nomi / href senza risultati", url)
            gL.log(gL.ERROR, msg)
            return False
        if len(nomi) != len(href):
            msg ="%s - %s" % ("Errore nel parsing dei nomi o di href", url)
            gL.log(gL.ERROR, msg)
            return False
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

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False
    
    return True

def QueueViamichelin(country, assettype, source, starturl, pageurl, page):
    try:
        gL.log(gL.DEBUG)
        #lista = page.xpath('//a[@class="clearfix"]')  # funziona
        href = page.xpath('//a[@class="parseHref jsNodePoiLink"]//@href')
        test = page.xpath('//h2[@class="parseInnerText jsNodePoiTitle"]//text()')
        # togli i nomi vuoti
        nomi = []
        for item in test:
            if item.replace(" ","") != '':
                nomi.append(item)
        if len(nomi) > len(href):
            msg ="%s - %s" % ("Errore nel parsing dei nomi o di href", url)
            gL.log(gL.ERROR, msg)
            return False
        if nomi is None or href is None:
            msg ="%s - %s" % ("Parsing dei nomi / href senza risultati", url)
            gL.log(gL.ERROR, msg)
            return False
        n = 0
        for asset in nomi:        
            if not href[n]:
                continue 
            name = gL.StdName(nomi[n])        
            url  = gL.sourcebaseurl + href[n]               
            gL.sql_Queue(country, assettype, source, starturl, pageurl, url, name)
            n = n + 1  # next asset

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False
    
    return True

def QueueQristoranti(country, assettype, source, starturl, pageurl, page):
    try:
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

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

    return True