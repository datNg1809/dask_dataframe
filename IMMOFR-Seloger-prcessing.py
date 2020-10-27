# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 14:33:34 2020

@author: d.nguyen
"""

import dask.dataframe as dd
from dask import compute
from dask.distributed import Client, LocalCluster
from dask.diagnostics import ProgressBar
import os.path
import re
from pathlib import Path
from glob import glob
from timeit import default_timer as timer
from datetime import timedelta
import numpy as np
import tkinter as tk
from tkinter import filedialog
import pandas as pd
userhome = os.path.expanduser('~')
save_path = os.path.join( userhome,'Desktop')
save_path = os.path.join(save_path,'RESULT')
Path(save_path).mkdir(parents=True, exist_ok=True)
os.chdir(save_path)

root = tk.Tk()
root.withdraw()
filepath = filedialog.askopenfilename(title="Select ANNONCES data")
root.update()
annonces = dd.read_csv(filepath, header=0, sep='|', quoting=3, dtype='object', encoding='utf-8', na_values=([' ','']))

root = tk.Tk()
root.withdraw()
filepath = filedialog.askopenfilename(title="Select PROS data")
root.update()
df_pros = pd.read_csv(filepath, header=0, sep='|', quoting=3, dtype='str', encoding='utf-8', na_values=([' ',''])).fillna('')

#annonces = dd.read_csv(r'D:\25. Requests\IMMO_FR processing script\ANNONCES_2020_08.csv', header=0, sep='|', quoting=3, dtype='object', encoding='utf-8', na_values=([' ','']))
#df_pros = pd.read_csv(r'D:\25. Requests\IMMO_FR processing script\PRO_2020_08.csv', header=0, sep='|', quoting=3, dtype='str', encoding='utf-8', na_values=([' ',''])).fillna('')

start = timer()

print("Start exporting")
#EXPORT PERCENTAGE OF NULL/MISSING VALUES PER WEBSITES
print("Export: emptiness_per_website.xlsx")
column_list = annonces.columns.tolist()

websites = annonces['SITE_ANNONCE'].unique().compute().tolist()
print("There are {} websites".format(len(websites)))
pct_missing_per_website = pd.DataFrame()
with ProgressBar():
    for website in websites:
        print(website)
        temp_df = annonces[annonces['SITE_ANNONCE']==website].compute()
        temp_null_count = temp_df.isnull().sum()
        temp_site_count = len(temp_df.index)
        pct_missing_per_website[website] = round(temp_null_count/temp_site_count*100,2)
pct_missing_per_website.to_excel(os.path.join(save_path ,'emptiness_per_website.xlsx'))

#EXPORT PERCENTAGE OF TOTAL NULL/MISSING VALUES
print("Export: emptiness_total.xlsx")
with ProgressBar():
    null_count_total = annonces.isnull().sum().compute()
    total_length = len(annonces.index)
    null_percent_total = round(null_count_total/len(annonces.index),2)
total_missing = pd.concat([null_count_total, null_percent_total], axis = 1)
total_missing.columns = ['Empty_Count','Empty_pct']
total_missing.to_excel(os.path.join(save_path ,'emptiness_total.xlsx'))

#EXPORT GENERAL REPORT
print("Export: seloger_reporting_check_results.xlsx")
writer = pd.ExcelWriter(os.path.join(save_path, 'seloger_reporting_check_results.xlsx'), engine='xlsxwriter')
with ProgressBar():
    df_1 = annonces.groupby(["SITE_ANNONCE","TYPE_ANNONCEUR","TYPE_TRANSACTION","CATEGORIE_BIEN"])['URL_ANNONCE'].count().compute()
    df_2 = annonces["SITE_ANNONCE"].value_counts().compute()
    df_3 = annonces[annonces['ID_AUTOBIZ']!='0'].groupby('SITE_ANNONCE')['ID_AUTOBIZ'].nunique().compute()
    df_4 = df_pros["ACTIVITE_PRO"].value_counts()
df_1.to_excel(writer,sheet_name='ads_detailed_repartition')
df_2.to_excel(writer,sheet_name="nb_ads_per_website")
df_3.to_excel(writer,sheet_name="nb_dealers_per_website")
df_4.to_excel(writer,sheet_name="pros_activities")
writer.save()

#FUNCTIONS
def fix_utf8(line):
    if not pd.isnull(line):
        try: line=line.encode('latin-1').decode('utf-8')
        except: line=line.encode('ascii', 'ignore').decode('utf-8')
    else: line=''
    return line

def fix_encoding_error(df, columns):
    for column in columns:
        df[column] = df[column].apply(lambda x: fix_utf8(x))

def fix_encoding_error_dask(df, columns):
    for column in columns:
        df[column] = df[column].apply(lambda x: fix_utf8(x), meta=('str'))

def fix_general(df,column,arg,meta=None):
    if meta: return df[column].apply(lambda x: "" if eval(arg) else x, meta=meta)
    else: return df[column].apply(lambda x: "" if eval(arg) else x)

def write_to_excel(write_engine, sheetnames):
    for sheetname in sheetnames:
        (eval(sheetname)).to_excel(write_engine,sheet_name=sheetname)

def adresse_fix(name):
    try:
        if "www." in name: return ""
        else: return name
    except TypeError: return ""

def fix_digit(dd, columns):
    for column in columns:
        dd[column] = dd[column].apply(lambda x : np.int64(int(x)) if not pd.isnull(x) else "" ,meta=('np.int64'))

def DATE_CRAWL_ANNONCE(name):
    if re.match(r'10/[0-1][0-9]/2[0-9]',name) or re.match(r'15/[0-1][0-9]/2[0-9]',name):
        return name
    else: return name.replace(r'^\d{2}','15')
#PROCESSING PROS FILE
print("\n" + "Begin processing PROS")
'''
• ID_TIERS_AUTOBIZ : count if the variable has values=0 and show the line of the db
• colonne DATE_MAJ_FICHE_PRO : count if values = null or = 00/00/00 and show the line of the db
• ADRESSE1_PRO : count if something starts by www. or http. If yes, show the values and show the line of the db.
• ADRESSE2_PRO : idem
• CP_PRO : count the cells with values which have more than 5 Numbers. Show these values and show the line of the db.
• DEPT_PRO : we sometimes do have incoherent values (ve, th, su...) so we need to count them if something is different than 2 Numbers and show the line of the db.
• VILLE_PRO : check if there are Numbers. If yes, show the values and show the line of the db.
• TEL_PRO : Check if there is something different than Numbers. If yes, show the values and show the line of the db.
• EMAIL_PRO : Count if there are url (www.promoca.fr) or numbers (503676421). If yes, show the line of the db.
• SITE_WEB_PRO : Check if Something starts by Something different than "www". If yes, show the values and show the line of the db.
• CODE_NAF_PRO : Check if string is other than format of 3-4 numbers with 1 last character (3235A, 245C) If yes, show the values and show the line of the db.
*output delivery:
-If id_tiers_autobiz=0 delete the line
-When found errors as the ones mentioned above, substitute the value by na.
'''    
list_champ_pros=['ADRESSE1_PRO', 'ADRESSE2_PRO', 'VILLE_PRO','REGION_PRO','NOUVELLE_REGION_PRO','RAISON_SOCIALE','NOM_COMMERCIAL','ACTIVITE_PRO','NOM_CONTACT','PRENOM_CONTACT']
fix_encoding_error(df_pros,list_champ_pros)
id_tiers_autobiz = df_pros[df_pros['ID_TIERS_AUTOBIZ']=="0"]
adresse1_pro_error = df_pros['ADRESSE1_PRO'][df_pros['ADRESSE1_PRO'].str.contains("www.|http",na=False)]
adresse2_pro_error = df_pros['ADRESSE2_PRO'][df_pros['ADRESSE2_PRO'].str.contains("www.|http",na=False)]
cp_pro_error = df_pros['CP_PRO'][df_pros['CP_PRO'].str.len()>6]
dept_pro_error = df_pros['DEPT_PRO'][(df_pros['DEPT_PRO'].str.match('^\"[0-9]{2}$') == False) & (df_pros['DEPT_PRO'] != "")]
ville_pro_error = df_pros['VILLE_PRO'][df_pros['VILLE_PRO'].str.match(r'.*[0-9]')]
tel_pro_error = df_pros['TEL_PRO'][df_pros['TEL_PRO'].str.match(r'.*[a-zA-Z]')]
email_pro_error = df_pros['EMAIL_PRO'][(df_pros['EMAIL_PRO'].str.match(r'.*www')) | (df_pros['EMAIL_PRO'].str.match(r'^.*[0-9]{5,}$'))]
site_web_pro_error = df_pros['SITE_WEB_PRO'][(df_pros['SITE_WEB_PRO'].str.match(r'^.*[.com,.fr]')==False) & (df_pros['SITE_WEB_PRO'] != "")]
code_naf_pro_error = df_pros['CODE_NAF_PRO'][(df_pros['CODE_NAF_PRO'].str.match(r'^[0-9]{3,4}[A-Z]{1}$')==False) & (df_pros['CODE_NAF_PRO'] != "")]

writer_pro = pd.ExcelWriter(os.path.join(save_path, 'pro_check_results.xlsx'), engine='xlsxwriter', options={'strings_to_urls': False})  # assign excel file to write result
pro_check_list = ['id_tiers_autobiz', 'adresse1_pro_error', 'adresse2_pro_error', 'cp_pro_error', 'dept_pro_error',
                  'ville_pro_error', 'tel_pro_error', 'email_pro_error', 'site_web_pro_error', 'code_naf_pro_error']
write_to_excel(writer_pro,pro_check_list)
writer_pro.save()  
print("pro_check_results.xlsx has been written in RESULT folder")
df_pros['DATE_MAJ_FICHE_PRO'] = fix_general(df_pros,'DATE_MAJ_FICHE_PRO',"x == '00/00/00'")
df_pros['ADRESSE1_PRO'] = fix_general(df_pros,'ADRESSE1_PRO','("www." in x) or ("http" in x)')
df_pros['ADRESSE2_PRO'] = fix_general(df_pros,'ADRESSE2_PRO','("www." in x) or ("http" in x)')
df_pros['CP_PRO'] = fix_general(df_pros,'CP_PRO','len(x)>6')
df_pros['DEPT_PRO'] = fix_general(df_pros,'DEPT_PRO',"not re.match(r'^\"[0-9]{2}$',x)")
df_pros['VILLE_PRO'] = fix_general(df_pros,'VILLE_PRO',"re.match(r'.*[0-9]',x)")
df_pros['TEL_PRO'] = fix_general(df_pros,'TEL_PRO',"re.match(r'.*[a-zA-Z]',x)")
df_pros['EMAIL_PRO'] = fix_general(df_pros,'EMAIL_PRO',"(re.match(r'.*www',x)) or (re.match(r'^.*[0-9]{5,}$',x))")
df_pros['SITE_WEB_PRO'] = fix_general(df_pros,'SITE_WEB_PRO',"not re.match(r'^.*[.com,.fr]',x)")
df_pros['CODE_NAF_PRO'] = fix_general(df_pros,'CODE_NAF_PRO',"not re.match(r'^[0-9]{4}[A-Z]{1}$',x)")
#EXPORT PRO FILE
df_pros.to_csv('PROS_preventif.csv',sep="|",quoting=3,index=False)
print("PROS_preventif.csv has been saved in {}.".format(save_path) + "\n")
del df_pros

#PROCESSING ANNONCES FILE
print("\n" + "Begin processing ANNONCES")
'''
DATE_CRAWL_ANNONCE -> We must only have variables 10/mm/20 or 15/mm/20 . If Something different, transform to 15/mm/20
DESCRIPTION_ANNONCE-> substitute special characters for readeable ones
CP_ANNONCE -> if Something different to a number, substitute by emptyness
VILLE_ANNONCE-> substitute special characters for readeable ones. If something different to a name, make empty.
ADRESSE_ANNONCE-> substitute special characters for readeable ones. If there is www. Substitute by empty.
REGION_ANNONCE-> substitute special characters for readeable ones. If something different to a name, make empty.
NOUVELLE_REGION_ANNONCE-> substitute special characters for readeable ones. If something different to a name, make empty.
PRIX ;surface_terrain ;surface_bien ;nb_photos ;nb_pieces -> only digits format X and not X.XX
'''
annonces['DATE_CRAWL_ANNONCE'] = annonces['DATE_CRAWL_ANNONCE'].apply(lambda x: DATE_CRAWL_ANNONCE(x),meta=('object'))
annonces['CP_ANNONCE'] = fix_general(annonces,'CP_ANNONCE',"not re.match(r'^[\"]{0,1}[0-9]+$',str(x))", meta=('object'))
annonces['ADRESSE_ANNONCE'] = annonces['ADRESSE_ANNONCE'].apply(lambda x : adresse_fix(x), meta=('object'))
columns_to_fix_encoding = ['DESCRIPTION_ANNONCE','VILLE_ANNONCE','ADRESSE_ANNONCE','REGION_ANNONCE','NOUVELLE_REGION_ANNONCE']
columns_to_fix_digit = ['PRIX','SURFACE_TERRAIN','SURFACE_BIEN','NB_PHOTOS','NB_PIECES']
fix_encoding_error_dask(annonces, columns_to_fix_encoding)
fix_digit(annonces, columns_to_fix_digit)

#EXPORT ANNONCES FILE
print("Start writing ANNONCES final")                                                         
print("Length of original ANNONCES: " + str(total_length))
annonces = annonces.repartition(npartitions=50)
print("Numbers of partitions: " + str(annonces.npartitions))
print("Write partitions to RESULT folder")

with ProgressBar():
    annonces.to_csv(os.path.join(save_path,'ANNONCES_PREVENTIF*.csv'),sep="|",index=False)
print("Start concatenating {} partitions into 1 ANNONCES file".format(str(annonces.npartitions)))
filenames = glob(os.path.join(save_path,'ANNONCES_PREVENTIF*.csv'))
n=0
with open('ANNONCES_PREVENTIF_final.csv','a',encoding='utf-8') as out_file:
    for in_file in filenames:
        n+=1
        with open(in_file,'r',encoding='utf-8') as infile:
            if n>1:
               next(infile)
            for line in infile:
                if line[-1] != '\n': line += '\n'
                out_file.write(line)
        os.remove(os.path.join(save_path,in_file))
out_file.close()
print("ANNONCES_preventif_final.csv has been saved in {}.".format(save_path))
print("Checking if lengh of final matches with total length of {} partitions".format(str(annonces.npartitions)))
re_check = dd.read_csv('ANNONCES_PREVENTIF_final.csv', header=0, sep='|', quoting=3, dtype='object', encoding='utf-8', na_values=([' ','']))
print("Length of final output is:", len(re_check.index))

del re_check
del annonces
end = timer()
print("Total running time: ", timedelta(seconds=end-start))