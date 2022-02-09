# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 16:15:58 2019
@author: Krishna Biradar
Consolidation of all occupancy frames for performance tracking
"""

import pandas as pd
import logging
import sys
import os


def frame_consolidate(conspth,htl_city,htl_accman,htl_cap,consout,tday2,htl_cluster):    
    files = os.listdir(conspth)
    data=[]
    for f in files:
        df = pd.read_csv(conspth+'\\'+f,engine='python', encoding = "utf-8")
        data.append(df)
    
    data2 = pd.concat(data,ignore_index=True,sort=True)
    
    #----------------Account Manager Name Mapping---------------------------------
    data2['AccountMan'] = data2['Hotel'].map(htl_accman)
    data2['AccountMan'] = data2['AccountMan'].fillna(value='NotMapped')
    
    #------------------------------City Mapping-----------------------------------
    data2['City'] = data2['Hotel'].map(htl_city)
    data2['City'] = data2['City'].fillna(value='NotMapped')
    
    #-----------------------------Capacity Mapping--------------------------------
    data2['Capacity'] = data2['Hotel'].map(htl_cap)
    data2['Capacity'] = data2['Capacity'].fillna(value='NotMapped') 
    
    #----------------------------Cluster Name Mapping-----------------------------
    data2['ClusterName'] = data2['Hotel'].map(htl_cluster)
    data2['ClusterName'] = data2['ClusterName'].fillna(value='NotMapped')  
    #-----------------------------------------------------------------------------     
    data3=pd.DataFrame(data2.loc[:,['ChannelName','RevPD','occupancydate','No_of_Rooms','Dow','WDWE','Month','Year',
                                'ADR','AvgLead','Arrivals','LOS','statuscode','spitLY','RmsSTLY','RevSTLY','Hotel','AccountMan','City','Capacity','ClusterName']])
    data3.to_csv(consout+r'\Performance_{}.csv'.format(tday2), )
    print('\n================================================')
    logging.info('\n================================================')
    print('Consolidation Done for Performance Tracking !!!')
    logging.info('Consolidation Done for Performance Tracking !!!')
    print('================================================\n')
    logging.info('================================================\n')
    sys.exit()

