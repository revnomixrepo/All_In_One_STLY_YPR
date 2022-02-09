# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 14:53:00 2019
Auther: Krishna Biradar
Search Historic Data and Fresh Data
Date Format conversion
"""

import sys
import logging
import pandas as pd
import readTransData as rTrans

def CM_dateconv(cmfile2,form_dict,chman,hotelname,std_pth):
    logging.debug('------------------------------------------------------------')
    logging.debug('Module: Histfile_Search, SubModule: CM_dateconv')
    #============================ eZee date format setting ======================
    if chman == 'eZee':
        ezeedate = pd.read_excel(std_pth+r'\YPR_masters\ezeedate_format.xlsx')
        form_dict = dict(zip(ezeedate['CM'],ezeedate[hotelname]))
        logging.info("ezeedate_format mapping file read !!!")
        print("eZee date format read !!!")

    logging.debug('Date Format Dictionary for {} Channel Manager ::'.format(chman))
    logging.debug(form_dict)
    #============================================================================
    #--------------------------------------Booking Date----------------------------------#
    try:
        cmfile2['Booking_Date'] = pd.to_datetime(cmfile2['Booking_Date'], format="{}".format(form_dict['Booking']), errors = 'coerce')
    except:
        cmfile2['Booking_Date'] = pd.to_datetime(cmfile2['Booking_Date'])
    logging.debug('Booking_Date format set as per the Date Format Dictionary')
    #--------------------------------------CheckIn Date----------------------------------#
    try:
        cmfile2['CheckIn'] = pd.to_datetime(cmfile2['CheckIn'], format="{}".format(form_dict['CheckIn']))
    except:
        cmfile2['CheckIn'] = pd.to_datetime(cmfile2['CheckIn'])
    logging.debug('CheckIn format set as per the Date Format Dictionary')
    #--------------------------------------CheckOut Date----------------------------------#
    try:
        cmfile2['CheckOut'] = pd.to_datetime(cmfile2['CheckOut'], format="{}".format(form_dict['CheckOut']))
    except:
        cmfile2['CheckOut'] = pd.to_datetime(cmfile2['CheckOut'])
    logging.debug('CheckOut format set as per the Date Format Dictionary')
    #--------------------------------------UpdatedDate Date----------------------------------#
    try:
        cmfile2['UpdatedDate'] = pd.to_datetime(cmfile2['UpdatedDate'], format="{}".format(form_dict['UpdatedDate']),errors='coerce')
    except:
        cmfile2['UpdatedDate'] = pd.to_datetime(cmfile2['UpdatedDate'])

    logging.debug('UpdatedDate format set as per the Date Format Dictionary')
    
    cmfile2['CheckIn'] = pd.to_datetime(cmfile2['CheckIn'].dt.strftime("%Y-%m-%d"))                   # Y.K. 20"Dec added for the datetime to date conversion
    cmfile2['CheckOut'] = pd.to_datetime(cmfile2['CheckOut'].dt.strftime("%Y-%m-%d"))

    cmfile2['Booking_Date'] = pd.to_datetime(cmfile2['Booking_Date'], format="%Y-%m-%d",errors='coerce')
    cmfile2['UpdatedDate'] = pd.to_datetime(cmfile2['UpdatedDate'], format="%Y-%m-%d",errors='coerce')

    #=========================Blank Booking date setting===========================
    logging.debug('setting UpdatedDate date in blank Booking_Date column')
    cmfile2['Booking_Date'] = cmfile2['Booking_Date'].fillna(value = cmfile2['UpdatedDate'])

    logging.debug('setting CheckIn date in blank Booking_Date column if Updated date is also blank')
    cmfile2['Booking_Date'] = cmfile2['Booking_Date'].fillna(value = cmfile2['CheckIn'])

    logging.debug('finally setting blank updated date as booking date')
    cmfile2['UpdatedDate'] = cmfile2['UpdatedDate'].fillna(value=cmfile2['Booking_Date'])

    #Y.K.4'dec
    # try:
    #     cmfile2["Total_Amount"] = cmfile2["Total_Amount"].str.replace(',', '').astype(float)
    # except:
    #     pass

    #--------------------removing timestamp if present in booking date-----------------------
    cmfile2['Booking_Date'] = cmfile2['Booking_Date'].apply(lambda x:x.strftime('%Y-%m-%d'))
    cmfile2['UpdatedDate'] = cmfile2['UpdatedDate'].apply(lambda x:x.strftime('%Y-%m-%d'))

    #====================final date format set for all columns(yyyy-mm-dd)==================
    cmfile2['Booking_Date'] = pd.to_datetime(cmfile2['Booking_Date'], format="%Y-%m-%d")
    cmfile2['CheckIn'] = pd.to_datetime(cmfile2['CheckIn'], format="%Y-%m-%d")
    cmfile2['CheckOut'] = pd.to_datetime(cmfile2['CheckOut'],format ="%Y-%m-%d")
    cmfile2['UpdatedDate'] = pd.to_datetime(cmfile2['UpdatedDate'], format="%Y-%m-%d")
    #============================Debug Check=================================================
    logging.debug('All Date Columns set to Standard data format ("%Y-%m-%d")')
    logging.debug('Transaction Level Data before explode(occupancy conversion) ::')
    logging.debug(cmfile2)
    return(cmfile2)


def drop_duplicate_bookings(cmfile2,histfile2):
    logging.debug('------------------------------------------------------------')
    logging.debug('Module: Histfile_Search, SubModule: drop_duplicate_bookings')

    cmfile2['Booking_ID'] = cmfile2['Booking_ID'].astype(str)
    histfile2['Booking_ID'] = histfile2['Booking_ID'].astype(str)

    msids=set(cmfile2['Booking_ID'])
    slids=set(histfile2['Booking_ID'])

    dupids=list(msids.intersection(slids))  #Duplicate IDs

    if len(dupids) >= 1:
        histfile3 = pd.DataFrame(histfile2[~histfile2['Booking_ID'].isin(dupids)])
        logging.info("Duplicate bookings removed in Historic Data !")
#        print("Duplicate bookings removed in Historic Data !")
        histfile4 = cmfile2.append(histfile3,ignore_index=True,sort=True)
        logging.debug('FreshData and HistoricData Files appended')

        #-----------------std HotelName check----------------------------------
        htlnames=list(set(histfile4['Hotel']))
        if len(htlnames) == 1:
            pass
        else:
            logging.info('please check standard name!\nHotel Name missmatch between historic and fresh data')
            print('please check standard name!\nHotel Name missmatch between historic and fresh data')
            sys.exit()
        #----------------------------------------------------------------------
        return(histfile4)
        logging.info('Historic data appended with fresh data')
#        print('Historic data appended with fresh data')
    else:
        logging.info('No duplicate bookings in Historic data')
#        print('No duplicate bookings in Historic data')
        histfile4 = cmfile2.append(histfile2,ignore_index=True,sort=True)
        logging.debug('FreshData and HistoricData Files appended')

        #-----------------std HotelName check----------------------------------
        htlnames=list(set(histfile4['Hotel']))
        if len(htlnames) == 1:
            pass
        else:
            logging.info('please check standard name!\nHotel Name missmatch between historic and fresh data')
            print('please check standard name!\nHotel Name missmatch between historic and fresh data')
            sys.exit()
        #----------------------------------------------------------------------
        logging.info('Historic data appended with fresh data(histfile4) ::')
#        print('Historic data appended with fresh data')
        logging.debug(histfile4)
        return(histfile4)


def histsearch(hist_csvpth,freshpth,ch_man,htlname,condn2,backup_hist,std_pth):
    logging.debug('------------------------------------------------------------')
    logging.debug('Module: Histfile_Search, SubModule: histsearch')

    try:
        try:
            histcsv = pd.read_csv(hist_csvpth+'\\'+htlname+'_HistoricData.csv', encoding = "utf-8")
        except:
            histcsv = pd.read_csv(hist_csvpth + '\\' + htlname + '_HistoricData.txt', sep='|')
        logging.debug("Historic_CSV Found")
        #=====================================Validation Check=======================================================
        #=============dump histcsv read to backup folder as YesdayCSV(Not for Performance Tracking)=====================
        if condn2 == 0:
            logging.debug('Dumped existing HistoricCSV to Backup Folder renaming it Last.csv for CSV Validation ::')
            logging.debug(histcsv)
            histcsv.to_csv(backup_hist+'\\'+htlname+'_HistoricData_Last.txt',index=False, sep='|')
        else:
            pass
        try:
            histcsv['Booking_Date'] = pd.to_datetime(histcsv['Booking_Date'],format='%d-%b-%Y')
            histcsv['Booking_Date'] = pd.to_datetime(histcsv['Booking_Date'],format="%Y-%m-%d")

            histcsv['CheckIn'] = pd.to_datetime(histcsv['CheckIn'],format='%d-%b-%Y')
            histcsv['CheckIn'] = pd.to_datetime(histcsv['CheckIn'],format="%Y-%m-%d")

            histcsv['CheckOut'] = pd.to_datetime(histcsv['CheckOut'],format='%d-%b-%Y')
            histcsv['CheckOut'] = pd.to_datetime(histcsv['CheckOut'],format="%Y-%m-%d")
        except:
            pass

        try:
            histcsv['UpdatedDate'] = pd.to_datetime(histcsv['UpdatedDate'],format='%d-%b-%Y')
        except:
            histcsv['UpdatedDate'] = pd.to_datetime(histcsv['UpdatedDate'],format="%Y-%m-%d")

        logging.debug("Set all date columns of histcsv with Standard date format ('%Y-%m-%d')")
        logging.info("Historic_CSV Found for {} ! Searching for Fresh Data ...".format(htlname))
        print("Historic_CSV Found for {} ! Searching for Fresh Data ...".format(htlname))

        #==================================Read Fresh Data==========================================
        staahfile=rTrans.read_trans(freshpth,htlname,ch_man,condn2)

        logging.debug('\nBoth Fresh and Historic Data present to prepare YPR (1.staahfile,2.histcsv)::')
        logging.debug('1.staahfile ::')
        logging.debug(staahfile)

        logging.debug('2.histcsv ::')
        logging.debug(histcsv)

        return(staahfile,histcsv,'FreshwithCSV')
        #========================================================================================

    except FileNotFoundError:
        logging.info('Historic CSV not found for {} ! Searching for FreshData ...'.format(htlname))
        print('Historic CSV not found for {} ! Searching for FreshData ...'.format(htlname))
        blankdf=pd.DataFrame({})
        #==================================Read Fresh Data==========================================
        staahfile=rTrans.read_trans(freshpth,htlname,ch_man,condn2)

        logging.debug('Only Fresh Data present to prepare YPR (staahfile)::')
        logging.debug(staahfile)
        return(staahfile,blankdf,'onlyFresh')
        #================================