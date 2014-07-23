# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python
import datetime
from pygeocoder import Geocoder
from pygeolib import GeocoderError
import re
import rThinkGbl as gL


def SetNow():
    # data corrente del run
    wrk = datetime.datetime.now()
    return str(wrk.replace(microsecond = 0))

def StdCar(stringa):
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
    stringa = StdCar(stringa)    
    return stringa.title()

def StdPhone(stringa, CountryTelPrefx, CountryTelPrefx00):
    # formatta telefono
    newphone = []
    phone = StdCar(stringa) 
    separa =  re.split(r'[ +/\-()]+', phone)
    for token in separa:
        if token == "":
            continue
        if token == CountryTelPrefx or token == CountryTelPrefx00:
            continue
        newphone.append(token)
    new = " ".join(newphone)
    return new

def StdZip(stringa):
    stringa = StdCar(stringa) 
    # formatta ZIP
    return stringa

def CercaFrase(frase, stringa, operatore, replacew):
    mod = False
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
    if s is None:
        return ''
    return str(s)


def StdAddress(AddrStreet, AddrZIP, AddrCity, AddrCountry):

    gL.GmapNumcalls = gL.GmapNumcalls + 1

    indirizzo = xstr(AddrStreet) + " " + xstr(AddrZIP) + " " + xstr(AddrCity) + " " + xstr(AddrCountry) 

    AddrStreet = ""
    AddrCity = ""
    AddrZIP  = "" 
    AddrLat  = 0
    AddrLong = 0
    AddrRegion = ""
    AddrCounty = ""
    FormattedAddress = indirizzo

    try:
        results = Geocoder.geocode(indirizzo)
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
            return (False, "", "", "", 0, 0, "", "", FormattedAddress)
    except GeocoderError as err:          
        print("Indirizzo non validato", indirizzo, err.status, gL.GmapNumcalls )
        return (False, "", "", "", 0, 0, "", "", FormattedAddress)
