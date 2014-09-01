# stampa i nomi delle colonne del cursore





lista1 = []
lista2 = []
cursor.execute(sql)
for a in cursor.description:
    lista1.append(a[0])  # creo la lista dei nomi di colonna      
cursor.execute(sql)
for a in cursor.description:
    lista2.append(a[0])  # creo la lista dei nomi di colonna      


# stampo la situazione, ma non aggiorno il record nella tabella Asset
sql = ("select AssetId from Asset where AssetId = " + AssetId)
count = 0
cursor.execute(sql)
for a in cursor.description:
    lista2.append(a[0])  # creo la lista dei nomi di colonna                
rows2 = cursor.fetchone()
for row in rows2:
    print("   :  ", lista1[count], row[count])              
    count = count + 1 
for row in rows1:
    print("   :  ", lista1[count], row[count])
    count = count + 1 