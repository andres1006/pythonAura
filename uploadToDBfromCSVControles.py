

from pymongo import MongoClient
from Tkinter import Tk, Label, Button, Radiobutton, IntVar
from tkFileDialog import askdirectory
from os import listdir
from os.path import isfile, join,isdir
import pandas as pd
import json
import sys
import time
import datetime
import numpy as np
import tkMessageBox
import Tkinter, tkFileDialog


reload(sys)
sys.setdefaultencoding('utf8')


def ask_multiple_choice_question(prompt, options):
    root = Tk()
    if prompt:
        Label(root, text=prompt).pack()
    v = IntVar()
    for i, option in enumerate(options):
        Radiobutton(root, text=option, variable=v, value=i).pack(anchor="w")
    Button(text="Submit", command=root.destroy).pack()
    root.mainloop()
    if v.get() == 0: return None
    return options[v.get()]


#mypath="/home/plozano/Procesamiento/Analisis-Pablo"
#root = Tkinter.Tk()
#mypath = tkFileDialog.askdirectory(parent=root,initialdir="~/",title='Please select a directory')
mypath="/Users/johnalexandergaleano/Documents/aura/Aura-Docs/Grupo_Control_Tests"
folders = [f for f in listdir(mypath) if isdir(join(mypath, f))]
df = {}
#uri = "mongodb://192.168.1.150"
#client = MongoClient(uri,ssl=True)
#client = MongoClient(uri, 27017, username="oscann",password="CuevaErikSilviaPablo", authSource='admin')
#uri = "mongodb://oscann:CuevaErikSilviaPablo@192.168.1.150:27017/?authSource=admin&authMechanism=SCRAM-SHA-1"
uri = "mongodb://localhost:27017"
#uri = "mongodb://oscann:CuevaErikSilviaPablo@localhost:27017/?authSource=admin&authMechanism=SCRAM-SHA-1"
client = MongoClient(uri)
#client = MongoClient('localhost', 27017, username="oscann",password="CuevaErikSilviaPablo", authSource='admin')

db = client.datos
result=0
yesToAll = 0
noToAll = 0
timeDomain=["TAS.csv","TSL","TFIX","TSV","TSM"]


for i,x in enumerate(folders):
    files =  [f for f in listdir(mypath + "/"+x) if isfile(join(mypath + "/"+x, f))]
    df[i]= pd.DataFrame()
    for y in files:
        print y
        newcol=[]
        prov = pd.read_csv(mypath + "/"+x+"/" + y,encoding = 'utf-8',skipinitialspace=True)
        #if ("TAS" not in y) and ("TSL" not in y) and ("TFIX" not in y) and ("TSV" not in y) and ("TSM" not in y):
        if "FRECUENCIA" in y:
            #newcol = [((if("FFT" in y) "FFT" else "HILBERT")+"_"+y.split("_")[0]+"_"+z) if z!="ID" else z  for z in prov.columns ]
            #newcol = [y.split("_")[2].split(".")[0]+"_"+y.split("_")[0]+"_"+z if z!="ID" else z  for z in prov.columns ]
            for z in prov.columns:
                if z!="ID":
                    if("FFT" in y):
                        newcol.append("FFT"+"_"+y.split("_")[0]+"_"+z)
                    else:
                        newcol.append("HILBERT"+"_"+y.split("_")[0]+"_"+z)
                else:
                    newcol.append(z)

        else:
            for z in prov.columns:
                if z!="ID":
                    newcol.append("TEMP"+"_"+z)
                else:
                    newcol.append(z)

        prov.columns= newcol

        #print prov.columns

        if len(df[i])>0:
            df[i] = pd.merge(df[i],prov, on="ID")
        else:
            df[i] = prov

    for index, row in df[i].iterrows():
        if (row.isnull().values.sum() > 0.8*len(row)):
            if result != "No to all" and result != "Yes to all" :
                #print result
                result = ask_multiple_choice_question(
                    str(row[0]) + " from " + x + " has more then 80% of empty values.\n Do you want to insert it to the DB?",
                    [                        "Yes",
                        "No",
                        "Yes to all",
                        "No to all"
                    ]
                )
            if result == "No" or result == "No to all" :
                df[i].drop(df[i].index[index])
    db[x].drop()
    if len(files) > 1:
        record=json.loads(df[i].T.to_json()).values()
        db[x].insert(record, {"ordered":True}  )
        time.sleep(0.5)
        #print(x)

db = client.operations.operations
post = {"operation":"datosUpload", "date": str(datetime.datetime.now()), "pathologiesInvolved":str(folders) }
#print post
db.insert_one(post, {"ordered":True}  )
