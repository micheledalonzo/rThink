# -*- coding: cp1252 -*-.

import rThinkGbl as gL
import rThinkDb
import pypyodbc
import sqlite3
import rThinkMain

u = []
url1 = 'http://www.tripadvisor.it/Restaurant_Review-g2232879-d2222903-Reviews-Pizzeria_da_Libero-Busso_Province_of_Campobasso_Molise.html'
url2 = 'http://www.tripadvisor.it/Restaurant_Review-g194930-d4881219-Reviews-Termoli_Marina-Termoli_Province_of_Campobasso_Molise.html'
url3 = 'http://www.tripadvisor.it/Restaurant_Review-g2034252-d2032875-Reviews-Ristorante_Why_Not-San_Polo_Matese_Province_of_Campobasso_Molise.html'
url4 = 'http://www.tripadvisor.it/Restaurant_Review-g2034252-d2032875-Reviews-Ristorante_Why_Not-San_Polo_Matese_Province_of_Campobasso_Molise.html'
url5 = ''


if url1 != '':
    u.append(url1)
if url2 != '':
    u.append(url2)
if url3 != '':
    u.append(url3)
if url4 != '':
    u.append(url4)
if url5 != '':
    u.append(url5)

name = "Pizzeria da Libero"
gL.Dsn ="rThinkTest"
if not gL.MySql:
    gL.MySql = pypyodbc.connect('DSN=rThinkTest')
    gL.cSql = gL.MySql.cursor()
    
if not gL.SqLite:
    gL.SqLite = sqlite3.connect(':memory:')
    gL.cLite = gL.SqLite.cursor()        

gL.SqLite, gL.C = gL.OpenConnectionSqlite()
gL.MySql, gL.Cursor = gL.OpenConnectionMySql(gL.Dsn)
    
# diversamente.... run normale
gL.restart = False   
# setto il runid, la data di inizio e il logger
#gL.RunId = gL.sql_RunId("START")
#rc = gL.SetLogger(gL.RunId, gL.restart)

for i in u:
    gL.assetbaseurl = 'http://www.tripadvisor.it/'
    gL.sourcebaseurl = 'http://www.tripadvisor.it'   
    language = 'ITA'
    country = 'ITA'
    source = 1
    assettype = 1
    refresh = True
    gL.currency = 'EUR'
    starturl = 'test'     
    pageurl = starturl            
            
    gL.N_Ass = gL.N_Ass + 1              
    asseturl = i 
    rc = rThinkMain.RunInit()
    # parse delle singole pagine degli asset
    Asset = gL.ParseContent(country, assettype, source, starturl, asseturl, name)                                                  
    if Asset:  # se tutto ok
        AssetMatch, AssetRef = gL.StdAsset(Asset)   # controllo se esiste già un asset simile
        AAsset = gL.AAsset(Asset, AssetMatch, AssetRef)   # creo il record in Asset a partire da SourceAsseId corrente con riferimento al suo simile oppure lo aggiorno
        gL.QueueStatus("END", country, assettype, source, starturl, pageurl, asseturl) # scrivo nella coda che ho finito
        # per ogni asset una call a Google Places
        gAsset = gL.ParseGooglePlacesMain(Asset, AAsset)
        if gAsset:  # se > 0
            AssetMatch, AssetRef = gL.StdAsset(gAsset)   # controllo se esiste già un asset simile
            rc = gL.AAsset(Asset, AssetMatch, AssetRef)   # creo il record in Asset a partire da SourceAsseId corrente con riferimento al suo simile oppure lo aggiorno
                               

#chiudo le tabelle dei run
gL.cSql.commit()    
gL.CloseConnectionMySql()
gL.CloseConnectionSqlite()
