# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 18:11:07 2019

@author: Krishna Biradar
Validates two csvs (current with yesday)
"""
import pandas as pd
import logging
from datetime import datetime

tday = datetime.now()
tday2 = tday.strftime('%d%b%Y')

month_map ={'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}

def month_year_sum(csvdf,nametype):
    logging.debug('------------------------------------------------------------')
    logging.debug('Module: csvValidate , SubModule: month_year_sum')
    
    csvdf['CheckIn'] = pd.to_datetime(csvdf['CheckIn'],format='%d-%b-%Y')    
    csvdf['Year'] = csvdf['CheckIn'].dt.strftime('%Y')
    csvdf['Month'] = csvdf['CheckIn'].dt.strftime('%b')
    csvdf['Mont_Num'] = csvdf['Month'].map(month_map)
    csvdf['KeyCol'] = csvdf['Year'].astype(str)+'|'+csvdf['Month'].astype(str)
    #---------------assign dtypes for Rooms and Revenue Column-----------------
    csvdf['No_of_Rooms'] = csvdf['No_of_Rooms'].astype(int) 
    csvdf['Total_Amount'] = csvdf['Total_Amount'].astype(float) 
    #--------------------------------------------------------------------------
    csvdf2  = pd.DataFrame(csvdf.groupby(['KeyCol'])['No_of_Rooms','Total_Amount'].sum())
    csvdf2.reset_index(inplace=True) 
    csvdf2.rename(columns={'No_of_Rooms':'Rooms_{}'.format(nametype),'Total_Amount':'Amount_{}'.format(nametype)},inplace=True)
    return(csvdf2)   
    

def csv_validate(histfile4,htlname,std_pth,filecondn):
    logging.debug('------------------------------------------------------------')
    logging.debug('Module: csvValidate , SubModule: csv_validate')
    
    backup_hist = std_pth+r'\Performance_Tracker\Backup'
    val_path = std_pth+r'\Performance_Tracker\Validation'
    tdayCSV = pd.DataFrame(histfile4)
    if filecondn == 'onlyFresh':
        logging.info("No Last Day CSV found as it is all Fresh Data")
        return(print("No Last Day CSV found as it is all Fresh Data"))
    else:
        pass
    try:
        yesdayCSV = pd.read_csv(backup_hist+'\\'+htlname+'_HistoricData_Last.csv',encoding = "utf-8")
    except:
        yesdayCSV = pd.read_csv(backup_hist + '\\' + htlname + '_HistoricData_Last.txt',sep='|', encoding="utf-8")
    
    logging.info('Last CSV read')
#    print('Last CSV read')
    
    #=================Month Year Room Rev Comparison===========================
    today_sum=month_year_sum(tdayCSV, 'current')    
    yesday_sum=month_year_sum(yesdayCSV, 'last')
    #-------------------merge--------------------------------------------------    
    mergedf = yesday_sum.merge(today_sum,on='KeyCol',how='outer')  
    mergedf.fillna(value=0,inplace=True)
    mergedf['Amount_current'] = mergedf['Amount_current'].astype(float) 
    mergedf['Amount_current'] = mergedf['Amount_current'].round(2)
    mergedf['Amount_last'] = mergedf['Amount_last'].astype(float)
    mergedf['Amount_last'] = mergedf['Amount_last'].round(2)
    
    mergedf['Room_Diff'] = mergedf['Rooms_current'] - mergedf['Rooms_last']
    mergedf['Rev_Diff'] = mergedf['Amount_current'] - mergedf['Amount_last']
    
    exdf =  mergedf['KeyCol'].str.split('|',expand=True)
    mergedf2 = pd.concat([exdf, mergedf],axis=1)
    mergedf2.rename(columns={0:'Year',1:'Month'},inplace=True)
    #===================Ascending Order by Month===============================
    mergedf3 = pd.DataFrame(mergedf2)    
    mergedf3['Year'] = mergedf3['Year'].astype(int)
    mergedf3.sort_values(by='Year',inplace=True)     
    
    mergedf3['month2'] = mergedf3['Month'].map(month_map)
    mergedf3.sort_values(by='month2',inplace=True)    
    
    mergedf3.drop(['KeyCol','month2'],axis=1,inplace=True)    
    mergedf3.to_csv(val_path+'\\'+htlname+'_Validation_Report_{}.csv'.format(tday2),index=False) 
    
    logging.info('Validation dumped in Validation Folder, plz check the report ::')
    logging.debug(mergedf3)
#    print('Validation dumped in Validation Folder, plz check the report.')   
    #==========================================================================
    
    

