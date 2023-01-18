# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 14:53:00 2019
Auther: Krishna Biradar
Pass standard Names,channel manager, path to the functions
"""

import os
import sys
import consolidate as cons
import pandas as pd
import All_Occ_Fun
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


ddmmyy = datetime.now()
tday = ddmmyy.strftime("%d_%b_%Y")
tday2 = ddmmyy.strftime("%d%b%y")


def stly_pth(acc_man, condn2, isell_pth, std_pth, logflag):
    
    #================================Logger Addition===================================
    logpth = std_pth + '\\' + 'logs'
    import logging    
    #---------------------------------log flag-----------------------------------------------
    if logflag == 'DEBUG':
        logging.basicConfig(format='%(asctime)s %(message)s',filename=logpth+'\\'+'YPR_{}_Debug_{}.log'.format(tday, acc_man),level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(message)s',filename=logpth+'\\'+'YPR_{}_Info_{}.log'.format(tday, acc_man),level=logging.INFO)
    
    logging.warning('is when this event was logged by {}.'.format(acc_man))
    logging.warning('Logging Mode: {}.'.format(logflag))
    logging.info('=======================================================================')
    
    logging.debug('------------------------------------------------------------')
    logging.debug('Module:All_OccYPR, SubModule:stly_pth')
    #======================Read YPR Masters====================================
    outpth = std_pth+r'\Account_Manager\{}\Output_YPR_Frames'.format(acc_man)
    
    isell_master = pd.read_excel(std_pth+r'\YPR_masters\InputConditionMaster.xlsx')
    name_cm = dict(zip(isell_master['hotelname'],isell_master['ChannelMan']))

    
    cm_master = pd.read_excel(std_pth+r'\YPR_masters\cm_master_STLY.xlsx')
    
    fileform = pd.read_excel(std_pth+r'\YPR_masters\DateFormats_STLY.xlsx')
    stdnames = pd.read_excel(std_pth+r'\YPR_masters\standard_names.xlsx')
    names = list(set(stdnames['hotelname']))
    
    
    channelMap = pd.read_excel(std_pth+r'\YPR_masters\channelMap.xlsx')
    channelMap['Channel'] = channelMap['Channel'].str.strip()
    chnl_Map = dict(zip(channelMap['Channel'],channelMap['stdChannel']))
    
    statuscodes = pd.read_excel(std_pth+r'\YPR_masters\statuscodes.xlsx')
    statuscode = dict(zip(statuscodes['status'],statuscodes['code']))
    htl_amount = dict(zip(stdnames['hotelname'],stdnames['AmountColumn']))    
    
    #--------------------create output folders---------------------------------
    try:
        os.chdir(outpth)        
        os.mkdir(tday)
    except FileExistsError:
        pass
    #--------------------------------------------------------------------------    
    
    if condn2 == 1:        
        #==========================Consolidation or YPR Prepare=================================
        logging.info("\n1.YPR Frames Preparation using all iSell OTA Data for Performance Tracker\n2.Consolidation for Performance Tracking")
        print("\n1.YPR Frames Preparation using all iSell OTA Data for Performance Tracker\n2.Consolidation for Performance Tracking")
        ch=int(input("Please Enter Choice(1/2) :"))
        logging.info('Choice Entered : {}'.format(ch))
    else:
        ch = 1
    
    #--------------------Consolidation for Performance Tracker--------------------------------
    if ch == 2:
        conspth = std_pth+r'\Performance_Tracker\All_STLY_Occ_Frames'
        consout = std_pth+r'\Performance_Tracker\Consolidation'
        #-----------------------dictionaries--------------------------------------
        htl_city = dict(zip(isell_master['hotelname'],isell_master['City']))
        htl_accman = dict(zip(isell_master['hotelname'],isell_master['AccManager']))
        htl_cap = dict(zip(isell_master['hotelname'],isell_master['cap']))
        htl_cluster = dict(zip(isell_master['hotelname'],isell_master['ClusterName']))
        #--------------------------------------------------------------------------
        cons.frame_consolidate(conspth,htl_city,htl_accman,htl_cap,consout,tday2,htl_cluster)
        
    elif ch == 1:
        pass
    
    else:
        logging.info('######### Enter Correct Choice (1/2) ###########\n')
        print('######### Enter Correct Choice (1/2) ###########\n')
        sys.exit()   
        
    #-----------Reading all OTA files from OTA_data folder for PerformanceTracking(condn2 == 1)--------------------------    
    if condn2 == 1:
        logging.info('Reading all {} OTA data used for iSells'.format(tday))
        track_list2 = stdnames[stdnames['Performance_Track'] == 1]
        track_list = list(track_list2['hotelname'])
#        print(track_list)
        print('Reading all {} OTA data used for iSells'.format(tday))
        #----------------iSell OTA_Data Path----------------------- 
        pth = isell_pth+r'\InputData\OTA_Data\{}'.format(tday)   
        yprdata = track_list
    
        #------------------------------------------------------------------------------
        ##@Yadnesh Change the below line-code for auto-fetching the input data path
    #--------------------------------------------------------------------------------------------------------    
    else:
        pth = std_pth + '\Account_Manager/{}/InputData/'.format(acc_man)
        yprdata = os.listdir(pth)
        # ------------------------------------------------------------------------------------------------------------------------
        # pth = r'{}'.format(input("Pl enter path containing fresh YPR_Data Files with std names:"))
        # yprdata=os.listdir(pth)

    if len(yprdata) == 0:
        logging.info('\n#### Please Check Path Provided & Account Manager Name given in Tool\nPath provided contains No YPR_Data files ####\n')
        print('\n## Please Check Path Provided & Account Manager Name given in Tool\nPath provided contains No YPR_Data files ####\n')
        print('YPR input data file is not Available. \n Check the standard name "HotelName_YPRData"')
        sys.exit()
    else:
        pass
    
            
    for sr,yprf in enumerate(yprdata,start=1):
        
        if condn2 != 1 :
            #---------Extracting extension from file name-------------
            extn=os.path.splitext(pth+'\\'+yprf)[1]
            extlen = len('_YPRData{}'.format(extn))
            stdname = yprf[:-extlen]
        else:
            #---------No need to Extract extension from file name(Performance Tracker)-------------
            stdname = yprf
            
        #--------------------check std name in mapping files--------------------------
        try:
            CM = name_cm[stdname]
        except Exception as e: 
            print(e)
            logging.info('#### Hotel Name not present in InputConditionMaster or StandardNames master file ###\n')
            print('#### Hotel Name not present in InputConditionMaster or StandardNames master file ###\n')
            sys.exit()
        #----------------------------------------------------------------------------------
        datapath=std_pth+r'\Account_Manager\{}'.format(acc_man)          
        All_Occ_Fun.CM_All(sr, pth, datapath, CM, stdname, outpth, condn2, isell_pth, std_pth, fileform, htl_amount, cm_master, names, chnl_Map, statuscode,channelMap)
    
    logging.info("##### All Occupancy Frames Created ! Thank You ! ###### ")
    print("##### All Occupancy Frames Created ! Thank You ! ######")
            
