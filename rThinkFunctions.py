# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python
import datetime
from pygeocoder import Geocoder
from pygeolib import GeocoderError
import re
import rThinkGbl as gL
import phonenumbers
import inspect

def SetNow():
    # data corrente del run
    wrk = datetime.datetime.now()
    return str(wrk.replace(microsecond = 0))

def StdCar(stringa):
    #gL.log(gL.DEBUG)
    if isinstance(stringa, list):
        clean = stringa[0]
    else:
        clean = stringa
    CaratteriVietati = ['#', '(', ')', '/', '.', '-', ';',  '"']
    for ch in CaratteriVietati:
        if ch in clean:
            clean = clean.replace(ch, "")
    clean = " ".join(clean.split())
    stringa = clean.strip()
    return stringa

def StdName(stringa):
    #gL.log(gL.DEBUG)
    stringa = gL.StdCar(stringa)    
    return stringa.title()

def StdPhone(stringa, country):
    gL.log(gL.DEBUG)
    test = stringa.split(' - ')   # due numeri di tel separati da trattino
    if len(test) > 1:
        stringa = test[0]
    
    ISO = gL.CountryISO.get(country) 
    if ISO is None:
        gL.cSql.execute("select CountryISO2 from Country where CountryId = ?", ([country]))
        row = gL.cSql.fetchone()
        if row:
            ISO = row['countryiso2']           
            gL.CountryISO[country] = ISO

    if ISO is None:
        gL.log(gL.ERROR, "Lingua non trovata")
        return False
    
    # formatta telefono
    try:
        y = phonenumbers.parse(stringa, ISO)
        newphone = ''    
        newphone = phonenumbers.format_number(y, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
    except:
        msg ="%s - %s" % ("Phone stdz error", stringa)
        gL.log(gL.ERROR, msg)
        newphone = stringa
        return False
    return newphone

def StdZip(stringa):
    gL.log(gL.DEBUG)
    stringa = gL.StdCar(stringa) 
    # formatta ZIP
    return stringa

def CercaFrase(frase, stringa, operatore, replacew):
    #gL.log(gL.DEBUG)
    mod = False
    stringa = str(stringa)
    newstringa = stringa
    a = []
    b = []
    idx = []
    out = []
    wrk = []
    a = stringa.split()
    b = frase.split()
    if replacew:
        c = replacew.split()
    maxa = len(a)
    maxb = len(b)
    conta = 0; 
    trovato = False

    if a and b and maxa >= maxb: 
        for i, x in enumerate(a):
            conta = 0
            #idx = a.index(x)
            if x == b[0]:
                idx.append(i)
                out.append(x)
                trovato = True
                for z in b[1:]:
                    conta = conta + 1
                    if conta+i < maxa:
                        if z != a[conta+i]:
                            trovato = False
                            idx = []
                            out = []
                        else:
                            idx.append(conta+i)
                            out.append(z)
                if trovato:
                    break

    if operatore == "Keep":
        return trovato, mod, newstringa, idx
    
    if trovato:
        if operatore == "Delete" or operatore == "Replace":
            conta = 0
            for zz in a:
                if not conta in idx:           
                    wrk.append(zz)
                    mod = True
                conta = conta + 1
            newstringa = " ".join(wrk)        

        if operatore == "Replace":
            conta = 0
            if idx[0] == 0:
                ind = 0
                indrpc = 0
            else:
                ind = idx[0] + 1
                indrpc = idx[0]
            if ind > len(wrk):   # se sono alla fine della stringa                
                for rplc in c:
                    wrk.append(rplc)
                    mod = True
            else:
                for rplc in reversed(c):
                    wrk.insert(indrpc, rplc)
                    mod = True

            newstringa = " ".join(wrk)

    return trovato, mod, newstringa, idx

def xstr(s):
    #gL.log(gL.DEBUG)
    if s is None:
        return ''
    return str(s)

def OkParam():
    return True


def StdAddress(AddrStreet, AddrZIP, AddrCity, AddrCountry):
    gL.log(gL.DEBUG)
    gL.GmapNumcalls = gL.GmapNumcalls + 1
    
    AddrRegion = ''
    AddrLat    = 0
    AddrCounty = ''
    AddrLong   = 0
    FormattedAddress = ''

    indirizzo = xstr(AddrStreet) + " " + xstr(AddrZIP) + " " + xstr(AddrCity) + " " + xstr(AddrCountry) 

    try:
        results = Geocoder.geocode(indirizzo)
        if results is None:
           return (False, AddrStreet, AddrCity, AddrZIP, 0, 0, '', '', '')

        if results.count > 0:
            result = results[0]   # solo il primo valore ritornato
            #print(result.postal_code)  # zip
            #print(result.route)   #strada
            #print(result.locality) # citta
            #print(result.administrative_area_level_2)  # provincia estesa
            #print(result.administrative_area_level_1)  # regione
            #print(result.country)  # stato
            #print(result.street_number)  # n.civico
            #print(result.coordinates[0])  #  lat
            #print(result.coordinates[1])  #  long
            #print(result.formatted_address)  #  indirizzo formattato da Google
            AddrCounty = ""
            for component in result.current_data['address_components']:
                a = component['types']
                if a:
                    if a[0] == "administrative_area_level_2":                    
                        AddrCounty = component['short_name']     
                        break            
            if result.route and result.street_number:
                AddrStreet = result.route + " " + result.street_number
            AddrCity = result.locality
            AddrZIP =  result.postal_code            
            if result.coordinates[0]:
                AddrLat  = result.coordinates[0]
            if result.coordinates[1]:
                AddrLong = result.coordinates[1]
            if result.administrative_area_level_1:
                AddrRegion = result.administrative_area_level_1
            if result.formatted_address:
                FormattedAddress = result.formatted_address
            return True, AddrStreet, AddrCity, AddrZIP, AddrLat, AddrLong, AddrRegion, AddrCounty, FormattedAddress                   
        else:
            return (False, AddrStreet, AddrCity, AddrZIP, 0, 0, '', '', '')
    except GeocoderError as err:                      
        return (False, AddrStreet, AddrCity, AddrZIP, 0, 0, '', '', '')


