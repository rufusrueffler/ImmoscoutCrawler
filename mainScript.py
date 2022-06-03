# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 10:33:40 2020

@author: rufus
"""
# https://statisquo.de/2018/06/11/immobilienscout24-mining-2-0-der-web-scraper-fuer-haeuser/
# https://statisquo.de/2017/11/29/immobilienscout24-mining-tutorial-der-web-scraper/2/

import bs4 as bs
import urllib.request
import time
from datetime import datetime
import pandas as pd
import json
import os


dataPath = "C:\\Thomas\\40_SoftwareDaten\\Python\\10_RawData\\99_otherSampleData\\ImmoscoutCrawler\\Rohdaten\\"

def CrawlImmoscoutHouses(dataPath):
    for seite in range(1,3): #ursprünglich 3000
        print("Loop " + str(seite) + " startet.")
    
        df = pd.DataFrame()
        l=[]
    
        try:
            soup = bs.BeautifulSoup(urllib.request.urlopen("https://www.immobilienscout24.de/Suche/S-2/P-"+str(seite)+"/Haus-Kauf").read(),'lxml')
            print("Aktuelle Seite: "+"https://www.immobilienscout24.de/Suche/S-2/P-"+str(seite)+"/Haus-Kauf")
            for paragraph in soup.find_all("a"):
                if r"/expose/" in str(paragraph.get("href")):
                    l.append(paragraph.get("href").split("#")[0])
                l = list(set(l))
    
            for item in l:
                try:
                    soup = bs.BeautifulSoup(urllib.request.urlopen('https://www.immobilienscout24.de'+item).read(),'lxml')
                    data = pd.DataFrame(json.loads(str(soup.find_all("script")).split("keyValues = ")[1].split("}")[0]+str("}")),index=[str(datetime.now())])
                    data["URL"] = str(item)
                    beschreibung = []
    
                    for i in soup.find_all("pre"):
                        beschreibung.append(i.text)
    
                    data["beschreibung"] = str(beschreibung)
                    df = df.append(data)
    
                except Exception as e: 
                    print(str(datetime.now())+": " + str(e))
                    l = list(filter(lambda x: x != item, l))
                    print("ID " + str(item) + " entfernt.")
                    
            print("Exportiert CSV")
            
            actualPath = dataPath + datetime.today().strftime('%y-%m-%d') +'\\'
            if not os.path.exists(actualPath):
                os.makedirs(actualPath)
            
            print(actualPath)
            df.to_csv(actualPath+str(datetime.now())[:19].replace(":","").replace(".","")+".csv",sep=";",decimal=",",encoding = "utf-8",index_label="timestamp")     
    
            print("Loop " + str(seite) + " endet.\n")
            
        except Exception as e: 
            print(str(datetime.now())+": " + str(e))
    
    print("FERTIG!")
    return actualPath

def mergeCSVfiles(dataPath, csvFolder):
    import os
    import pandas as pd
    
    df = pd.DataFrame()
    n=0
    for i in os.listdir(csvFolder):
        n+=1
        df = df.append(pd.read_csv(csvFolder+str(i),sep=";",decimal=",",encoding="utf-8"))
        print("Durchgang "+str(n))
    
    #remove duplicates
    df = df.drop_duplicates(subset="URL")
    df.to_csv(dataPath+datetime.today().strftime('%y-%m-%d') +".csv",sep=";",decimal=",",encoding = "utf-8",index_label="timestamp")     
    return

csvFolder = CrawlImmoscoutHouses(dataPath)
mergeCSVfiles(dataPath, csvFolder)