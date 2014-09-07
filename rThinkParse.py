# -*- coding: cp1252 -*-.

import requests
from lxml import html
from lxml import etree
import collections
import pypyodbc
import datetime
import time
import requests
import sys
import locale
import random
# import jabba_webkit as jw
from urllib.parse import urlparse
import rThinkGbl as gL
import requests
import re
import json
import logging
from collections import namedtuple
from rThinkDb import AssetReview
import difflib

# override del loggin di requests
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)


def BuildQueue(country, assettype, source, starturl, pageurl, page):     
    fn = gL.GetFunzione("QUEUE", source, assettype, country)
    if not fn:
        gL.log(gL.ERROR, "Funzione PARSE non trovata")
        return False
    return globals()[fn](country, assettype, source, starturl, pageurl, page)
     
    #return globals()[gL.queue_fn](country, assettype, source, starturl, pageurl, page)
    
def ParseContent(country, assettype, source, starturl, asseturl, name):   

    Asset = gL.Asset(country, assettype, source, name, asseturl)  # inserisco l'asset
    fn = gL.GetFunzione("PARSE", source, assettype, country)
    if not fn:
        gL.log(gL.ERROR, "Funzione PARSE non trovata")
        return False
    #rc = globals()[gL.parse_fn](country, asseturl, name, Asset) 
    rc = globals()[fn](country, asseturl, name, Asset)
    if rc:
        return Asset
    else:
        gL.log(gL.ERROR, "Errore " + str(rc) + " nel parse")
        return False

        
def ParseNextPage(source, assettype, country, pageurl, page):
    #return globals()[gL.nxpage_fn](pageurl, page)
    fn = gL.GetFunzione("NEXT", source, assettype, country)
    if not fn:
        gL.log(gL.ERROR, "Funzione NEXT non trovata")
        return False
    return globals()[fn](pageurl, page)


def ReadPage(url):
    #gL.log(gL.INFO, url)
    max = 10; n = 0
    while True:
        try:
            n = n + 1
            attesa = random.randint(1, 15) # da 1 a 15 sec 
            time.sleep(attesa)
            if gL.Useproxy:
                rand_proxy = random.choice(gL.Proxies)
                proxy = {}
                proxy = {"http" : rand_proxy} 
                gL.log(gL.INFO, "proxy=" + rand_proxy)
                page = requests.get(url,proxies=proxy)  
            else:
                page = requests.get(url)  
            page.raise_for_status()       
            #rc = gL.SqlSaveContent(url, page.text)            
            return html.fromstring(page.content)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            msg ="HTTP %s - %s" % (status_code, url)
            gL.log(gL.WARNING, msg)
            if status_code == 500 or status_code == 502:
                if n > max: 
                    gL.log(gL.WARNING, "FINE MAX TENTATIVI DI LETTURA PER URL=" + url + "STATUS=" + str(status_code))
                    break
                else:
                    continue
            else:
                gL.log(gL.WARNING, "HTTP=" + str(status_code) + "url=" + url)
                break   
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:            
            n = n + 1
            if n > max:
                break
            attesa = random.randint(1, 10) # da 1 a 4 sec 
            time.sleep(attesa)
            continue
        except requests.exceptions.RequestException as e:  # altro errore qualsiasi
            gL.log(gL.ERROR, url)
            gL.log(gL.ERROR, e)
            break
        except:
            gL.log(gL.ERROR, url)
            gL.log(gL.ERROR, sys.exc_info()[0])
            break

    return None

def NextpageTripadvisor(url, page):
    try:

    
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
                            
    except Exception as err:
        gL.log(gL.ERROR, url, err)
        return False, ''

    return False, ''
         
def NextpageDuespaghi(url, page):

    try:

     
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
            return False, ''
        test = page.xpath('//*[@class="row-identity-container"]/a/@href')  # le pagine esistono ma non hanno contenuto
        if page is not None and test:
            return url_a, page

    except Exception as err:
        url_a = "http://" + o.hostname + o.path + "?pag=2&ord=relevance&dir=desc"  # se non trovo il numero pagina vuol dire che è la prima pagina, 
        # controlla che esista
        newpage = ReadPage(url_a)
        if newpage is not None:
            test = newpage.xpath('//*[@class="row-identity-container"]/a/@href')  # controllo che la seconda esista con del contenuto
            if test:
                return url_a, newpage
            else:
                return False, ''

    return False, ''

def NextpageViamichelin(url, page):
    try:
        # format: http://www.viamichelin.it/web/Ristoranti/Ristoranti-Italia?page=2

     
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
                    return False, ''

        except Exception as err:
            url_a = "http://" + o.hostname + o.path + "?page=2"  # se non trovo il numero pagina vuol dire che è la prima pagina, 
            # controlla che esista
            newpage = ReadPage(url_a)
            if newpage is not None:
                test = newpage.xpath('//div[@id="noResult"]')  # controllo che la seconda esista con del contenuto (questo è il messaggio "No result"
                if len(test) == 0:
                    return url_a, newpage
            else:
                return False, ''

    except Exception as err:
        gL.log(gL.ERROR, url)
        gL.log(gL.ERROR, err)
        return False, ''

    return False, ''

def NextpageQristoranti(url, page):
    try:

    
        # get la prossima pagina lista e inseriscila nella coda di lavoro e nella
        # tabella starturl
        # per tutti i link rel next
        pagact = page.xpath('//span[@class="inactive"]/text()')   # pagina attuale, se zero non c'è paginazione,
        if len(pagact) == 0:
            return False, ''
        curpa = int(pagact[0])
        links = page.xpath('//a[@class="paginate"]/@href')
        numpa = page.xpath('//a[@class="paginate"]/text()')
        if numpa[0] is not None:
            if int(numpa[0]) > int(curpa):
                newpage = ReadPage(links[0])
                if newpage is not None:
                    return(links[0], newpage)
                else:
                    return False, ''
            else:
                return False, ''               

    except Exception as err:
        gL.log(gL.ERROR, url)
        gL.log(gL.ERROR, err)
        return False, ''

    return False, ''

def ParseTripadvisor(country, url, name, Asset):

    try:

        # leggi la pagina di contenuti
        content = ReadPage(url)
        if content is  None:
            return False

        LastReviewDate = content.xpath('//span[@class="ratingDate"]/text()')  # la prima che trovo e' la piu' recente
        if LastReviewDate:
            LastReviewDate = LastReviewDate[0]
            LastReviewDate = gL.StdCar(LastReviewDate)
            LastReviewDate = LastReviewDate.replace('Recensito il ', '')
            LastReviewDate = datetime.datetime.strptime(LastReviewDate, '%d %B %Y')
            LastReviewDate = datetime.datetime.combine(LastReviewDate, datetime.time(0, 0))  # mettila in formato datetime.datetime

            # aggiorno la data di ultima recensione sulla tabella asset del source
            rc = gL.UpdateLastReviewDate(Asset, LastReviewDate)

        AddrWebsite = ''
        AddrCounty  = ''
        AddrStreet  = ''
        AddrZIP     = ''
        AddrPhone   = ''      
        AddrPhone1  = ''      
        AddrCity    = ''               
        AddrStreet  = content.xpath('//span[@property="v:street-address"]/text()')
        AddrCity = content.xpath('//span[@property="v:locality"]/text()')
        if len(AddrCity) == 0:
            AddrCity = content.xpath('//span[@property="v:municipality"]/text()')
        #AddrCounty = content.xpath('//span[@property="v:country-name"]/text()')
        AddrZIP = content.xpath('//span[@property="v:postal-code"]/text()')
        AddrPhone = content.xpath('//div[@class="fl phoneNumber"]/text()')
   
        if len(AddrStreet)>0:
            AddrStreet = gL.StdName(AddrStreet[0])
        if len(AddrCity)>0:
            AddrCity = gL.StdName(AddrCity[0])
        if len(AddrZIP)>0:
            AddrZIP = gL.StdZip(AddrZIP[0])
        if len(AddrPhone)>0:
            #AddrPhone = gL.StdPhone(AddrPhone[0], country)
            AddrPhone, AddrPhone1 = gL.StdPhone(AddrPhone[0], country)
            if not AddrPhone:
                AddrPhone   = ''; AddrPhone1  = ''
        if len(AddrWebsite)>0:
            AddrWebsite = AddrWebsite[0]

        AddrList = {'AddrStreet': AddrStreet,
                    'AddrCity': AddrCity,
                    'AddrCounty': AddrCounty,
                    'AddrZIP': AddrZIP,
                    'AddrPhone': AddrPhone,
                    'AddrPhone1': AddrPhone1,
                    'AddrCountry': country}
        rc = gL.AssettAddress(Asset, AddrList) 
         
        # gestione dei tag
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
        rc = gL.AssetTag(Asset, tag, "Cucina")

        # Gestione prezzo        
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
        rc = gL.AssetPrice(Asset, PriceList, gL.currency)
    
        # gestione recensioni    
        r = []
        for i in range(0, 5):
            punt = str(i + 1)
            mask1 = "\'" + punt + "\'"  # riassunto recensioni
            # mask =
            # '//div[@onclick[contains(.,"value=\'5\'")]]/following-sibling::*/text()'
            mask = '//div[@onclick[contains(.,"value=' + mask1 + '")]]/following-sibling::*/text()'
            nreview = content.xpath(mask)  # num review
            if nreview:
                nreview = locale.atoi(nreview[0])
                r.append((nreview, int(punt)))
            
        #rc = gL.AssettReview(Asset, nreview, int(punt))
        if len(r) > 0:
            gL.AssetReview(Asset, r)
    
    except Exception as err:        
        gL.log(gL.ERROR, url, err)
        return False

    return True

def ParseGooglePlacesMain(Asset, AAsset):
    try:        
        gL.cSql.execute("Select * from QAddress where Asset = ?", ([Asset]))
        row = gL.cSql.fetchone()
        if not row:
            gL.log(gL.ERROR, "asset:" + str(Asset))
            return False           
       
        country     = row['country']
        assettype   = row['assettype']
        source      = row['source']
        starturl    = row['starturl']
        asseturl    = row['asseturl']
        name        = row['name']
        address     = row['address']
        addrstreet  = row['addrstreet']
        addrcity    = row['addrcity']
        addrzip     = row['addrzip']
        addrcounty  = row['addrcounty']
            
        
        gAsset = gL.ParseGooglePlaces(assettype, name, gL.xstr(addrstreet), gL.xstr(addrzip), gL.xstr(addrcity), gL.xstr(country), gL.xstr(address), AAsset)
            
        return gAsset

    except Exception as err:
        gL.log(gL.ERROR, "asset:" + str(Asset))
        gL.log(gL.ERROR, err)
        return False

def ParseGooglePlaces(assettype, name, street, zip, city, country, address, AAsset):
    if address != '':
        indirizzo = address
    else:
        indirizzo =  street + " " + zip + " " + city 

    try:
        qry = name + " " + city
        qry = qry.encode()
        API_KEY  = "AIzaSyDbLzwj-f_IJOEWYdgx12n0CizPN3xPUfM"
        searchurl = 'https://maps.googleapis.com/maps/api/place/textsearch/json'
    
        params = dict(
            query = qry,
            key = API_KEY,    
            language='it',    
            #types='cafe|reastaurant|bakery|bar|food|meal_takeaway'
            )

        response = requests.get(url=searchurl, params=params)
        data = json.loads(response.text)
        if data['status'] == 'ZERO_RESULTS':
            gL.log(gL.WARNING, "GooglePlaces ZERO RESULTS:" + str(qry))
            return False
        if data['status'] != 'OK':
            gL.log(gL.WARNING, "GooglePlaces Status " + data['status'])
            return False

        # se ci sono più elementi ritornati scelgo quello che meglio matcha ---------------------
        chk = []   
        namepeso = 1.5
        streetpeso = 1     
        if len(data['results']) > 0:
            for idx, test in enumerate(data['results']):
                if 'formatted_address' in test:
                    adr = test['formatted_address']
                nam = test['name']    
                nameratio = streetratio = 0
                nameratio = difflib.SequenceMatcher(None, a = name, b = nam).ratio()
                streetratio = difflib.SequenceMatcher(None, a = indirizzo, b = adr).ratio()    
                gblratio = ((nameratio * namepeso) + (streetratio * streetpeso)) / len(data['results'])          
                chk.append((gblratio, idx, nam, adr, nameratio, streetratio))      # nome, indirizzo, ratio del nome, ratio dell'indirizzo
            #chk.sort(reverse=True) 
            chk.sort(reverse=True, key=lambda tup: tup[0])  
            idx = chk[0][1]
        else:
            idx = 0

        # --------------------------------------------------------------------- OK LEGGO 
        a = data['results'][idx]   # l'elemento selezionato tra quelli ritornati
        lat = 0; lng = 0; tag=[]; prz = None
        if 'geometry' in a:
            lat = a['geometry']['location']['lat']
            lng = a['geometry']['location']['lng']
        nam = a['name']                  
        if 'place_id' in a:
            pid = a['place_id']
        if 'reference' in a:
            ref = a['reference']
        if 'price_level' in a:
            prz = a['price_level'] # 0 — Free 1 — Inexpensive 2 — Moderate 3 — Expensive 4 — Very Expensive
        if 'rating' in a:
            rat = a['rating']        
        if 'formatted_address' in a:
            adr = a['formatted_address']
        tag = []
        for type in a['types']:
            if type == 'cafe' or type == 'bar':
                tag.append("Caffetteria")
            if type == 'restaurant' :
                tag.append("Ristorante")
            if type == 'lodging' :
                tag.append("Hotel")
            if type == 'bakery' :
                tag.append("Panetteria")
        PriceList = []
        if prz is not None:
            if prz == 0:
                PriceFrom = 5
                PriceTo = 12
            if prz == 1:
                PriceFrom = 5
                PriceTo = 12
            if prz == 2:
                PriceFrom = 13
                PriceTo = 25
            if prz == 3:
                PriceFrom = 26
                PriceTo = 60
            if prz == 4:
                PriceFrom = 60
                PriceTo = 100
            PriceList = [['PriceCurr', gL.currency],
                        ['PriceFrom', PriceFrom],
                        ['PriceTo', PriceTo]]

        # chiedo il dettaglio
        detailurl = 'https://maps.googleapis.com/maps/api/place/details/json'
        params = dict(
            placeid = pid,
            key = API_KEY,    
            language='it',         
            #types='cafe|reastaurant|bakery|bar|food|meal_takeaway'
            )

        response = requests.get(url=detailurl, params=params)
        data2 = json.loads(response.text)
        if data2['status'] != 'OK':
            gL.log(gL.WARNING, "GooglePlaces Status " + data['status'])
            return False    
        d = data2['result']
        if d['url']:
            url = d['url']
        else:
            url = ''
        
        # ---------------------------- INSERISCO L'ASSET
        Asset = gL.Asset(country, assettype, gL.GoogleSource, nam, url, AAsset, pid)  # inserisco l'asset
        if Asset == 0:
            return Asset
        rc = gL.AssetTag(Asset, tag, "Tipologia")
        rc = gL.AssetPrice(Asset, PriceList, gL.currency)
        
        AddrCounty = AddrStreet = AddrNumber = AddrRegion = AddrCity = AddrZIP = ""; 
        for component in d['address_components']:
            a = component['types']
            if a:
                if a[0] == "locality":             
                            AddrCity = component['long_name']     
                if a[0] == "route":             
                            AddrStreet = component['long_name']     
                if a[0] == "administrative_area_level_1":             
                            AddrRegion = component['long_name']     
                if a[0] == "administrative_area_level_2":             
                            AddrCounty = component['short_name']     
                if a[0] == "street_number":             
                            AddrNumber = component['long_name']     
                if a[0] == "postal_code":             
                            AddrZIP = component['long_name']     
    
        AddrStreet = AddrStreet + " " + AddrNumber                    
        if 'international_phone_number' in d:
            if d['international_phone_number']:
                AddrPhone = d['international_phone_number']
            elif d['formatted_phone_number']:
                AddPhone = d['formatted_phone_number']
        
        punt = 0; nreview = 0
        if 'rating' in d:
            punt = d['rating']        
            nreview = d['user_ratings_total']
        if 'website' in d:
            AddrWebsite = d['website']
        if 'geometry' in d:        
            AddrLat  = d['geometry']['location']['lat']
            AddrLong = d['geometry']['location']['lng']        

        AddrCity=AddrCounty=AddrZIP=AddrPhone=AddrPhone1=AddrWebsite=AddrLat=AddrLong=AddrRegion=FormattedAddress=AddrCountry=Address=''
        AddrValidated = gL.NO
        FormattedAddress = d['formatted_address']
        AddrList = {'AddrStreet': AddrStreet,
            'AddrCity': AddrCity,
            'AddrCounty': AddrCounty,
            'AddrZIP': AddrZIP,
            'AddrPhone': AddrPhone,
            'AddrPhone1': '',
            'AddrWebsite': AddrWebsite,
            'AddrLat': AddrLat,
            'AddrLong': AddrLong,
            'AddrRegion': AddrRegion,
            'FormattedAddress': FormattedAddress,
            'AddrValidated': gL.YES,
            'AddrCountry': country,
            'Address': indirizzo}
        rc = gL.AssettAddress(Asset, AddrList) 
                   
        # gestione recensioni    
        if punt and nreview:            
            r = []
            r.append((int(nreview), int(punt)))
            rc = gL.AssetReview(Asset, r)
        
        # gestione orario
        if 'opening_hours' in d:        
            ope = d['opening_hours']['periods']
            #orario = namedtuple('orario', 'ggft')
            orario = []
            for item in ope:
                dayo = item['open']['day']
                fro  = item['open']['time']
                dayc = item['close']['day']
                to   = item['close']['time']
                orario.append((dayo, fro, to))
            rc = gL.AssetOpening(Asset, orario)
    
    except Exception as err:        
        gL.log(gL.ERROR, (name + " " + indirizzo), err)
        return False

    return Asset


def ParseDuespaghi(country, url, name, Asset):

    try:    

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
            rc = gL.UpdateLastReviewDate(Asset, LastReviewDate)


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
        rc = gL.AssettAddress(Asset, AddrList)

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
                gL.AssetTag(Asset, tag, "Tipologia")
        if y:
            for i in y: 
                #tag.append("Cucina")
                cucina = gL.StdName(i)
                tag.append(cucina)
                gL.AssetTag(Asset, tag, "Tipologia")
        #
        # gestione recensioni
        # 
        nreview = 0
        a = content.xpath('//span[@class="review-counter clearfix"]/text()')
        if a:
            a = a[0]
            b = a.split()
            if b:
                nreview = b[0]
            else:
                nreview = 0
        one = content.xpath('//span[@class="fa icon-farfalla star1 on"]')
        two = content.xpath('//span[@class="fa icon-farfalla star2 on"]')
        thre = content.xpath('//span[@class="fa icon-farfalla star3 on"]')
        four = content.xpath('//span[@class="fa icon-farfalla star4 on"]')
        five = content.xpath('//span[@class="fa icon-farfalla star5 on"]')
        punt = 0; r = []
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
        if punt>0:
            r.append((nreview, punt))
            gL.AssetReview(Asset, r)        

        price = content.xpath('//*[@itemprop="priceRange"]/text()')
        if price:
            a = price[0]
            PriceAvg = a.replace('€', '')
            PriceList = [['PriceCur', gL.currency],
                            ['PriceAvg', PriceAvg]]
            rc = gL.AssetPrice(Asset, PriceList, gL.currency)
    
    except Exception as err:
        gL.log(gL.ERROR, url)
        gL.log(gL.ERROR, err)
        return False

    return True

def ParseViamichelin(country, url, name, Asset):
        
    try:    

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
        #        gL.cSql.execute("Update Asset set LastReviewDate=? where Asset=?", (LastReviewDate, Asset))
        indirizzo = ''
        addr = content.xpath('//li[@class="vm-clear"]//li//text()')
        for add in addr:
            if add == ''           or \
               add == ' '          or \
               add == "Vedi mappa" or \
               add == "Indirizzo"  or \
               add == " : \xa0":
                continue
            indirizzo = add
            break
        if indirizzo:
            indirizzo = indirizzo + " " + country       
        AddrPhone= ''; AddrPhone1 = ''
        telefono = (content.xpath('//a[contains(@href, "tel:")]//text()'))
        if len(telefono)>0:
            AddrPhone, AddrPhone1 = gL.StdPhone(telefono[0], country)

        AddrWebsite = ''
        sito = (content.xpath('//li[contains(., "Sito internet")]//@href'))
        if len(sito)>0:
            AddrWebsite = sito[0]
        AddrList = {'AddrStreet': '',
                    'AddrCity': '',
                    'AddrCounty': '',
                    'AddrZIP': '',
                    'AddrPhone': AddrPhone,
                    'AddrPhone1': '',
                    'AddrWebsite': AddrWebsite,
                    'Address': indirizzo,
                    'AddrCountry': country}
        rc = gL.AssettAddress(Asset, AddrList) 
       
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
        rc = gL.AssetTag(Asset, tag, "Cucina")
    
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
        rc = gL.AssetPrice(Asset, PriceList, gL.currency)
    
        # gestione recensioni    
        r = []; punt = 0; nreview = 1 # ispettore viamichelin
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
            r.append((nreview, punt))
            gL.AssetReview(Asset, r)        

    except Exception as err:
        gL.log(gL.ERROR, url)
        gL.log(gL.ERROR, err)
        return False
    
    return True

def ParseQristoranti(country, url, name, Asset):
    try:
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
        if LastReviewDate is not None and LastReviewDate != '':            
            # aggiorno la data di ultima recensione sulla tabella asset del source
            rc = gL.UpdateLastReviewDate(Asset, LastReviewDate)
        
        AddrWebsite = ''
        AddrCounty  = ''
        AddrStreet  = ''
        AddrZIP     = ''
        AddrPhone   = ''      
        AddrPhone1  = ''      
        AddrCity    = ''               
        AddrWebsite = content.xpath('//td[contains(.,"sito")]//@href') # link al sito
        if len(AddrWebsite)>0:
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

        rc = gL.AssettAddress(Asset, AddrList)  
    
        # gestione dei tag
        
        x = content.xpath("//td[contains(., 'Tipo di cucina')]/following-sibling::td/a/text()")   # classificazione
        if len(x)>0:
            tag = []
            #tag.append("Cucina")
            cucina = " ".join(x[0].split())
            tag.append(cucina)
            rc = gL.AssetTag(Asset, tag, "Cucina")
        # 
        # Gestione prezzo
        # 
        y = content.xpath('//td[contains(., "Fascia di prezzo")]/following-sibling::td/text()')
        if len(y)>0:
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
        rc = gL.AssetPrice(Asset, PriceList, gL.currency)

        # gestione recensioni
        # 
        r = []
        x = content.xpath('//td[@class="rating_value average"]/text()')[0]   # valutazione
        y = content.xpath('//span[@class="count"]/text()')[0]                   # n. recensioni
        if len(x)>0:
            nreview = locale.atoi(x)
        if len(y)>0:
            punt = locale.atoi(y)
        if len(x)>0:
            r.append((nreview, punt))
            
        #rc = gL.AssettReview(Asset, nreview, int(punt))
        if len(r) > 0:
            gL.AssetReview(Asset, r)        

    except Exception as err:
        gL.log(gL.ERROR, url)
        gL.log(gL.ERROR, err)
        return False

    return True

def QueueTripadvisor(country, assettype, source, starturl, pageurl, page):
    try:

        # leggi la lista e inserisci asset
        lista = page.xpath('//*[@class="listing" or @class="listing first"]')
        for asset in lista:
            name = asset.xpath('.//*[@class="property_title"]//text()')[0]
            name = gL.StdName(name)
            url  = asset.xpath('.//a[contains(@class,"property_title")]/@href')[0]
            url  = gL.sourcebaseurl + url
            # inserisci o aggiorna l'asset        
            rc = gL.Enqueue(country, assettype, source, starturl, pageurl, url, name)

    except Exception as err:
        gL.log(gL.ERROR, pageurl)
        gL.log(gL.ERROR, err)
        return False
    
    return True

def QueueDuespaghi(country, assettype, source, starturl, pageurl, page):
    try:

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
               
            rc = gL.Enqueue(country, assettype, source, starturl, pageurl, url, name)
            n = n + 1  # next asset

    except Exception as err:
        gL.log(gL.ERROR, pageurl)
        gL.log(gL.ERROR, err)
        return False
    
    return True

def QueueViamichelin(country, assettype, source, starturl, pageurl, page):
    try:

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
            rc = gL.Enqueue(country, assettype, source, starturl, pageurl, url, name)
            n = n + 1  # next asset

    except Exception as err:
        gL.log(gL.ERROR, pageurl)
        gL.log(gL.ERROR, err)
        return False
    
    return True

def QueueQristoranti(country, assettype, source, starturl, pageurl, page):
    try:

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
        
            rc = gL.Enqueue(country, assettype, source, starturl, pageurl, link, name)

    except Exception as err:
        gL.log(gL.ERROR, pageurl)
        gL.log(gL.ERROR, err)
        return False

    return True