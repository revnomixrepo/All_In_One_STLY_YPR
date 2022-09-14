# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 14:53:00 2019
Auther: Krishna Biradar

updated on 20 Apr 2022
by Yadnesh Kolhe

Read different Channel Manager Transaction Data
"""

import re
import sys
import csv
import logging
import pandas as pd
import numpy as np


def read_trans(freshpth,htlname,ch_man,condn2):
    #==================================Read Fresh Data========================================
    if condn2 == 1:
        logging.debug('condn2 = 1 , read the _OTAData from InputData isell structure')
        dataname = '_OTAData'
    else:
        logging.debug('condn2 = 0 , read the _YPRData from the path provided')
        dataname = '_YPRData'

    try:
        #----------------------------------------------------------------------
        if ch_man == 'Staah':
            # staahfile = pd.read_excel(freshpth + '\{}'.format(htlname + str('{}.xls'.format(dataname)),encoding='unicode_escape'))
            try:
                staahfile = pd.read_csv(freshpth+'\{}'.format(htlname+str('{}.csv'.format(dataname))))
            except:
                staahfile = pd.read_excel(freshpth + '\{}'.format(htlname + str('{}.xls'.format(dataname)),encoding='unicode_escape'))

            staahfile.dropna(subset=['CheckIn Date', 'CheckOut Date'], inplace=True)

            logging.debug('{}{} Read ::'.format(htlname,dataname))

            logging.debug(staahfile)

        elif ch_man == 'RevSeed':
            # staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xls'.format(dataname))))
            staahfile = pd.read_csv(freshpth + '\{}'.format(htlname + str('{}.csv'.format(dataname))))


        elif ch_man == 'Staah Max':
            otafile = pd.read_excel(freshpth + '\{}'.format(htlname + str('{}.xlsx'.format(dataname))),header=2)
            otafile.dropna(subset=['Arrival Date', 'Departure Date'], inplace=True)
            staahfile = pd.DataFrame(otafile)
            staahfile['Arrival Date'] = staahfile['Arrival Date'].str.split(',', expand=True)[0]
            staahfile['Departure Date'] = staahfile['Departure Date'].str.split(',', expand=True)[0]
            logging.debug('{}{} Read ::'.format(htlname, dataname))
            logging.debug(staahfile)
            df = staahfile
            if htlname == "Hotel EnglishPoint & Spa":                                                                    #Y.K.11:Feb:2022 for currency conersion KES to usd($)
                df["Total Amount: (All Inclusive)"] = np.where(df['Room Type'].str.contains('KES', regex=True),
                                                               df["Total Amount: (All Inclusive)"] * 0.0088,
                                                               df["Total Amount: (All Inclusive)"])

        elif ch_man in ['Rategain', 'Rategain1']:
            staahfile = pd.read_excel(freshpth + '\{}'.format(htlname + str('{}.xlsx'.format(dataname))))
            staahfile = staahfile.dropna(how='all', subset=['Check-in Date'])
            staahfile = staahfile.dropna(how='all', subset=['Check-out Date'])
            staahfile = staahfile.dropna(how='all', subset=['Date Booked'])
            logging.debug('{}{} Read ::'.format(htlname, dataname))
            logging.debug(staahfile)

        elif ch_man == 'Eglobe':
            try:
                staahfile = pd.read_excel(freshpth + '\{}'.format(htlname + str('{}.xlsx'.format(dataname))))
            except:
                staahfile = pd.read_csv(freshpth + '\{}'.format(htlname + str('{}.csv'.format(dataname))))
            staahfile['Nights']=1
            staahfile.dropna(subset=['Status'], inplace=True)

        elif ch_man == 'Phobs':
            staahfile = pd.read_excel(freshpth + '\{}'.format(htlname + str('{}.xlsx'.format(dataname))), header=1)
            staahfile = staahfile.dropna(how = 'all',axis=1)
            staahfile['Total'] = staahfile['Total'].str.split(",").str[0]
            staahfile['Rooms'] = 1
            logging.debug('{}{} Read ::'.format(htlname, dataname))
            logging.debug(staahfile)

        elif ch_man == 'AxisRooms':
            try:
                staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xls'.format(dataname))))
            except:
                staahfile = pd.read_csv(freshpth+'\{}'.format(htlname+str('{}.xls'.format(dataname))),encoding = 'unicode_escape')

            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

            staahfile['Product'] = staahfile['Product'].str.strip()
            staahfile = pd.DataFrame(staahfile[staahfile['Product'] == htlname])

            #---------------------------add stlycol as Booking Date itself--------------------------------------
            staahfile['STLYDateCol'] = staahfile['Booking Time']
            #----------------------------------------------------------------------------------

        elif ch_man == 'Maximojo':
            staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xlsx'.format(dataname))))

            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'Djubo':
            staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xlsx'.format(dataname))),skiprows=1)
            #======================Setting Your Web and Direct Bookings===============================
            staahfile['Agency Name'].fillna(value='BLANK',inplace=True)
            staahfile['Agency Name'] = np.where(staahfile['Agency Name'] == 'BLANK',
                                               np.where(staahfile['Source Type'] == 'OTA', 'YourWeb',staahfile['Agency Name']),staahfile['Agency Name'])
            staahfile['Agency Name'] = np.where(staahfile['Agency Name'] == 'BLANK',
                                               np.where(staahfile['Source Type'] != 'OTA', 'Hotel Direct',staahfile['Agency Name']),staahfile['Agency Name'])
            #======================Setting Your Web and Direct Bookings===============================
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'eZee':
            staahfile = pd.read_csv(freshpth+'\{}'.format(htlname+str('{}.csv'.format(dataname))))
            staahfile.dropna(axis=0,subset=['Arrival','Dept'],inplace=True)
            staahfile['Rooms']=1
            #---------------------------add stlycol as Booking Date itself--------------------------------------
            staahfile['STLYDateCol'] = staahfile['Booking Date']
            #----------------------------------------------------------------------------------
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'UK':
            staahfile = pd.read_csv(freshpth+'\{}'.format(htlname+str('{}.csv'.format(dataname))), delimiter =",", index_col=False, header=0, low_memory=False, quoting=csv.QUOTE_ALL,encoding='utf8')

            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man in ['TB','TB1']:
            staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xlsx'.format(dataname))))
            staahfile['Rooms']=1
            #---------------------------add stlycol as Booking Date itself--------------------------------------
            try:
                staahfile['STLYDateCol'] = staahfile['Reservation date']
            except:
                pass
            #----------------------------------------------------------------------------------
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)
        elif ch_man == 'TravelBook':
            staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xlsx'.format(dataname))))
            staahfile['Rooms']=1
            #---------------------------add stlycol as Booking Date itself--------------------------------------
            staahfile['STLYDateCol'] = staahfile['Date']
            #----------------------------------------------------------------------------------
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'SiteMinder':
            staahfile = pd.read_csv(freshpth+'\{}'.format(htlname+str('{}.csv'.format(dataname))))

            try:
                staahfile.drop('Affiliated Channel',axis=1,inplace=True)
            except:
                pass
            staahfile.dropna(axis=0,subset=['Check In','Check Out'],inplace=True)
#            staahfile['Total Amount'] = staahfile['Total Amount'].str.extract('(\d+)').astype(int)
            staahfile['Total Amount'] = staahfile['Total Amount'].str.extract('([-+]?\d*\.\d+|\d+)')
            staahfile['Total Amount'].fillna(value=0,inplace=True)
            staahfile['Total Amount'] = staahfile['Total Amount'].astype(float)
            staahfile['Rooms']=1
            #---------------------------add stlycol as Booking Date itself--------------------------------------
            staahfile['STLYDateCol'] = staahfile['Date Created']
            #----------------------------------------------------------------------------------
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'RevSeed':                                                         # Y.K. 21"Dec Added from ReVseed Data make for YPR
            try:
                staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xlsx'.format(dataname))))
            except:
                staahfile = pd.read_csv(freshpth + '\{}'.format(htlname + str('{}.csv'.format(dataname))))


        elif ch_man == 'AsiaTech1':                                                         # Y.K. 06"Dec
            staahfile = pd.read_csv(freshpth+'\{}'.format(htlname+str('{}.csv'.format(dataname))), delimiter =",", index_col=False, header=0)
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'StayFlexi':                                                           # Y.K. 06"Dec

            staahfile = pd.read_csv(freshpth+'\{}'.format(htlname+str('{}.csv'.format(dataname))), delimiter =",", index_col=False, header=0,skipfooter=1)
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'Synxis':                                                                # Y.K. 07"Dec
            # staahfile = pd.read_csv(freshpth + '\{}'.format(htlname + str('{}.csv'.format(dataname))), delimiter=",",index_col=False, header=0, skipfooter=2)
            staahfile = pd.read_csv(freshpth + '\{}'.format(htlname + str('{}.csv'.format(dataname))), delimiter=",",index_col=False, header=0)
            staahfile = staahfile.drop(staahfile.index[-2:])            #Y.K. 07"Dec added for the last two row remove bcz there are unwanted values.
            logging.debug('{}{} Read ::'.format(htlname, dataname))
            logging.debug(staahfile)

        elif ch_man == 'BookingCentre':
            staahfile2 = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xlsx'.format(dataname))),skiprows=9)
            staahfile2['Cost'] = staahfile2['Cost'].str.replace('₨','')
            staahfile2['Cost'] = pd.to_numeric(staahfile2['Cost'].apply(lambda x: re.sub(',', '', str(x))))
            staahfile2['Rooms']=1
            staahfile2['Status']=staahfile2['Status'].fillna(value=1)
            staahfile =  pd.DataFrame(staahfile2[staahfile2.Status != 1])
            #---------------------------add stlycol as Booking Date itself--------------------------------------
            staahfile['STLYDateCol'] = staahfile['Date Booked']
            #----------------------------------------------------------------------------------
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'ResAvenue':
            staahfile2 = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xls'.format(dataname))),skiprows=2)
            staahfile2['Booking Status']=staahfile2['Booking Status'].fillna(value=1)
            staahfile =  pd.DataFrame(staahfile2[staahfile2['Booking Status'] != 1])
            #------------------------------------------------------------------------------------
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'RezNext':
            staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xlsx'.format(dataname))))
            #---------------------------add stlycol as Booking Date itself--------------------------------------
            staahfile['STLYDateCol'] = staahfile['BookingDate']
            #----------------------------------------------------------------------------------

        elif ch_man == 'TravelBook_Nirobi':
            staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xlsx'.format(dataname))))
            staahfile['Rooms']=1
            #---------------------------add stlycol as Booking Date itself--------------------------------------
            staahfile['Reservation date'] = pd.to_datetime(staahfile['Reservation date'],format='%m/%d/%Y %H:%M%p')
            staahfile['Reservation date'] = pd.to_datetime(staahfile['Reservation date'],format='%Y-%m-%d')
            staahfile['STLYDateCol'] = staahfile['Reservation date']
            #----------------------------------------------------------------------------------------------------
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'TravelClick':
            staahfile2 = pd.read_csv(freshpth+'\{}'.format(htlname+str('{}.csv'.format(dataname))))
            #======================remove House entries from Rate Plane column for YO1 ================
            if htlname == "YO1 India's Holistic Wellness Center":
                staahfile = pd.DataFrame(staahfile2[staahfile2['Rate Plan']!='House'])
            else:
                staahfile = pd.DataFrame(staahfile2)

            #=======================Taking Subchannel Desc Values as Channel Name========================
            staahfile['Subchannel Desc'].fillna(value='blankval',inplace=True)
            staahfile['Subchannel Desc'] = np.where(staahfile['Subchannel Desc'] == 'blankval', staahfile['Channel Name'],staahfile['Subchannel Desc'])
            staahfile.drop('Channel Name',axis=1,inplace=True)
            #---------------renamed 'Subchannel Desc' as Channel Name---------------------
            staahfile.rename(columns={'Subchannel Desc':'Channel Name'},inplace=True)

            staahfile['Rooms']=1
            #---------------------------add stlycol as Booking Date itself--------------------------------
            staahfile['STLYDateCol'] = staahfile['Book Date']
            #----------------------------------------------------------------------------------------------
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'BookingHotel':
            staahfile = pd.read_excel(freshpth+'\{}'.format(htlname+str('{}.xlsx'.format(dataname))))
            #---------------------------add stlycol as Booking Date itself--------------------------------------
            staahfile['STLYDateCol'] = staahfile['Booking Date']
            #----------------------------------------------------------------------------------
            logging.debug('{}{} Read ::'.format(htlname,dataname))
            logging.debug(staahfile)

        elif ch_man == 'Ease Room':   # Hemlata added by 21 APRIL 2022

            staahfile = pd.read_excel(freshpth + '\{}'.format(htlname + str('{}.xlsx'.format(dataname))))
            logging.debug('{}{} Read ::'.format(htlname, dataname))
            logging.debug(staahfile)

        else:
            logging.info("'{}': No such Channel Manager Added in masters !!!".format(ch_man))
            print("'{}': No such Channel Manager Added in masters !!!".format(ch_man))
            sys.exit()

        logging.info("Fresh Data Found for {} ! Preparing YPR ...".format(htlname))
        print("Fresh Data Found for {} ! Preparing YPR ...".format(htlname))
        return(staahfile)


    except FileNotFoundError:
        logging.info('Fresh Data not found for {} ! \nFresh Data file is must for YPR, \npl keep it and run tool again. Thanks'.format(htlname))
        print('Fresh Data not found for {} ! \nFresh Data file is must for YPR, \npl keep it and run tool again. Thanks'.format(htlname))
        sys.exit()
