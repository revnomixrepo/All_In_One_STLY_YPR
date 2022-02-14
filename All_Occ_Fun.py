# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 14:53:00 2019
Auther: Krishna Biradar
Exploding data and transform in to standard format
"""

import sys
import os
import logging
import numpy as np
import pandas as pd
import csvValidate as csv_val
import Histfile_Search as find_hist
from datetime import datetime,timedelta
import Revenomix_Append


ddmmyy = datetime.now()
tday = ddmmyy.strftime("%d_%b_%Y")
tday2 = ddmmyy.strftime("%d%b%y")


#----------lydate-----------
lydate2 = ddmmyy - timedelta(days=364)
lyDate = lydate2.strftime('%Y-%m-%d')

WDWE = {'Mon':'WD','Tue':'WD','Wed':'WD','Thu':'WD','Fri':'WE','Sat':'WE','Sun':'WE'}


#-------------------------------------------------------------------------------------------------------------------------
#--------------------------------------One Function for All CMs-----------------------------------------------------------

def CM_All(sr,cmpth, cmdatapth, chman, hotelname, outpth, condn2, isell_pth, std_pth, fileform, htl_amount, cm_master, names, chnl_Map, statuscode):
    logging.debug('------------------------------------------------------------')
    logging.debug('Module:All_Occ_Fun, SubModule:CM_All')
    #----------------Date Format Dictionary------------------------------------
    form_dict = dict(zip(fileform['CM'],fileform[chman]))  
    #-----------------Select Amount Column-------------------------------------
    amt_name = htl_amount[hotelname]
    #-----------------channel manager columns dict {cmnames : stdnames}--------
    cm_namecol = dict(zip(cm_master[chman],cm_master['stdname']))
    cm_namecol[amt_name] = 'Total_Amount'    
    
    std_cols = list(cm_master['stdname']) + ['Total_Amount']
    cm_histcsvpth= std_pth+r'\Performance_Tracker\Historic_STLY_CSVs'
    backup_hist= std_pth+r'\Performance_Tracker\Backup'
    
    if hotelname not in names:
        logging.info("Fresh Data file is not with standard name !, pl rename it as 'Hotelname_YPRData'")
        print("Fresh Data file is not with standard name !, pl rename it as 'Hotelname_YPRData'")
        sys.exit()
    else:
        pass
        
        
    #===========================Fresh Data & Historic Data Search===============================    
    cmfile,histfile,filecondn=find_hist.histsearch(cm_histcsvpth,cmpth,chman,hotelname,condn2,backup_hist,std_pth)
    
    if filecondn == 'FreshwithCSV':
        cmfile.rename(columns=cm_namecol,inplace=True)
        cmfile22 = pd.DataFrame(cmfile.loc[:,std_cols])
        #----------------std Hotelname added from filename to the fresh data-----------------
        cmfile22['Hotel'] = hotelname
        #------------------------------------------------------------------------------------
        cmfile242 =find_hist.CM_dateconv(cmfile22,form_dict,chman,hotelname,std_pth)  # Fresh data for occupancy converssion as well as historic CSV
        histfile2 = pd.DataFrame(histfile)                                 #........HistCSV taken as it is
        #----------------Removing Duplicate Bookings in Historic Data -----------------------------
        histfile4=find_hist.drop_duplicate_bookings(cmfile242,histfile2) # data for  historic CSV
        
    elif filecondn == 'onlyFresh':
        cmfile.rename(columns=cm_namecol,inplace=True)
        cmfile22 = pd.DataFrame(cmfile.loc[:,std_cols])
        #----------------std Hotelname added from filename to the fresh data----------------
        # cmfile22['Hotel'] = hotelname
        cmfile22['Hotel'] = hotelname

        #------------------------------------------------------------------------------------        
        cmfile242=find_hist.CM_dateconv(cmfile22,form_dict,chman,hotelname,std_pth)
        #Fresh data for occupancy converssion as well as historic CSV
        histfile4 = pd.DataFrame(cmfile242)# data for  historic CSV
    # hist_dump = pd.DataFrame(histfile4)
    # hist_dump['Booking_Date'] = hist_dump['Booking_Date'].apply(lambda x: x.strftime("%d-%b-%Y"))   # change date format
    # hist_dump['CheckIn'] = hist_dump['CheckIn'].apply(lambda x: x.strftime("%d-%b-%Y"))
    # hist_dump['CheckOut'] = hist_dump['CheckOut'].apply(lambda x: x.strftime("%d-%b-%Y"))
    # hist_dump['UpdatedDate'] = hist_dump['UpdatedDate'].apply(lambda x: x.strftime("%d-%b-%Y"))
    # # ----------------------------set Room Rev dtypes------------------------------------------
    # hist_dump['Total_Amount'] = hist_dump['Total_Amount'].fillna(value=0)
    # hist_dump['Total_Amount'] = hist_dump['Total_Amount'].astype(float)
    #
    # hist_dump['No_of_Rooms'] = hist_dump['No_of_Rooms'].fillna(value=0)
    # hist_dump['No_of_Rooms'] = hist_dump['No_of_Rooms'].astype(int)
    # hist_dump.to_csv(cm_histcsvpth + '\\' + '{}_HistoricData.txt'.format(hotelname), index=False, sep='|')
    # hist_dump.to_csv(backup_hist + '\\' + '{}_HistoricData_{}.txt'.format(hotelname, tday2), index=False, sep='|')


    occ_histpath = std_pth + "\Performance_Tracker\Occupancy_Historic_CSVs"

    # Checking for the occupancy history if present only fresh data will be used only for occupancy converssion otherwise histfile4( fresh data appended to history ) will be used.

    if os.path.isfile(occ_histpath + "/{}_OCC_Hist.txt".format(hotelname)) == True:
        print("Occupancy History present for {}".format(hotelname))
        Occdf = Occ_conv(cmfile242, chman, hotelname, isell_pth, std_pth,statuscode)  # Fresh data forwarded to occupancy converssion
    else:
        Occdf = Occ_conv(histfile4, chman, hotelname, isell_pth, std_pth, statuscode)  # histfile4 (Fresh data appended the history) forwarded to occupancy converssion

    #==================Channel Name Mapping====================================
    Occdf['Channel'] = Occdf['Channel'].astype(str)
    Occdf['Channel'] = Occdf['Channel'].str.strip()    
    Occdf['ChannelName'] = Occdf['Channel'].map(chnl_Map)
    Occdf['ChannelName'].fillna(value='#blank',inplace=True)
    
    #--------------------checking channel mapping------------------------------
    blnk_ch = pd.DataFrame(Occdf[Occdf['ChannelName'] == '#blank'])
    blnk_ch2 = list(blnk_ch['Channel'].unique())
    if len(blnk_ch2) == 0:
        logging.info('All Channel names are mapped with Standard Names')
#        print('All Channel names are mapped with Standard Names')
    else:
        logging.info('##### ChannelNames not mapped for :{} \n Please check unmapped_data folder ######'.format(hotelname)) 
        print('##### ChannelNames not mapped for :{} \n Please check unmapped_data folder ######'.format(hotelname))        
        blnk_ch2 = pd.DataFrame(blnk_ch.loc[:,['Channel','ChannelName']])
        blnk_ch2.drop_duplicates(subset='Channel',inplace=True)
        blnk_ch2.to_csv(std_pth+r'\unmapped_data\channelMAP_{}_{}.csv'.format(hotelname,tday2))
        logging.debug('Unmapped Channel Names in data are ::')
        logging.debug(blnk_ch2)
        sys.exit()   
    #------------------------------set positive pace--------------------------------------------
    Occdf['AvgLead'] = np.where(Occdf['AvgLead'] < 0,0,Occdf['AvgLead'])    
    
    #==================================OCC Frame Dump========================================    
    Occdf['occupancydate'] = Occdf['occupancydate'].apply(lambda x: x.strftime("%d-%b-%Y"))  
    Occdf['Booking_Date'] = Occdf['Booking_Date'].apply(lambda x: x.strftime("%d-%b-%Y"))      
    Occdf['CheckIn'] = Occdf['CheckIn'].apply(lambda x: x.strftime("%d-%b-%Y"))  
    Occdf['CheckOut'] = Occdf['CheckOut'].apply(lambda x: x.strftime("%d-%b-%Y"))  
    Occdf['UpdatedDate'] = Occdf['UpdatedDate'].apply(lambda x: x.strftime("%d-%b-%Y"))
     
    Occdf2 = pd.DataFrame(Occdf.loc[:,['ChannelName','RevPD','occupancydate','No_of_Rooms','Dow','WDWE','Month','Year','ADR','AvgLead','Arrivals','LOS','statuscode','spitLY','RmsSTLY','RevSTLY','Hotel']])  
    Occdf2.to_csv(outpth+'\\{}\\'.format(tday)+'{}_STLY_Frame_{}.csv'.format(hotelname,tday2))            
    
    #--------------------------OverWriteOccFrames for Performance Tracking---------------------
    Occdf2.to_csv(std_pth+r'\Performance_Tracker\All_STLY_Occ_Frames\{}_STLY_Frame.csv'.format(hotelname))

    #------------------------------------------------------------------------------------------
    
    Occdf3 = pd.DataFrame(Occdf2[Occdf2['statuscode'] == 1])
    Occdf4 = pd.DataFrame(Occdf3.loc[:,['ChannelName','RevPD','occupancydate','No_of_Rooms','Dow','WDWE','Month','Year','ADR','AvgLead','Arrivals','LOS']])
    Occdf4.to_csv(outpth+'\\{}\\'.format(tday)+'{}_Normal_Frame_{}.csv'.format(hotelname,tday2))    
    logging.info('{}.{}_{} STLY & Normal Frames created !'.format(sr,hotelname,chman))        
    print('{}.{}_{} STLY & Normal Frames created !'.format(sr,hotelname,chman))
    
    #================================Historic CSV Dump(Not for PerformanceTracker)======================================== 
    #Note: This step over writes Historic CSV in Historic_STLY_CSVs folder if condn2 = 0, means Manual YPR Preparation 
    #condn2 = 1 means Performance Tracker, it runs daily reads all input files and prepares All YPRs, 
    #it will not over write the Historic CSV files and Validation Check on daily basis. 
    #To Consider both conditions you can use "condn2 in [1,0]" inside if condition. Uncomment below if condition.
    
    if condn2 in [0,1]:
    # if condn2 == 0:
        histfile4['Booking_Date'] = histfile4['Booking_Date'].apply(lambda x: x.strftime("%d-%b-%Y"))
        histfile4['CheckIn'] = histfile4['CheckIn'].apply(lambda x: x.strftime("%d-%b-%Y"))
        histfile4['CheckOut'] = histfile4['CheckOut'].apply(lambda x: x.strftime("%d-%b-%Y"))
        histfile4['UpdatedDate'] = histfile4['UpdatedDate'].apply(lambda x: x.strftime("%d-%b-%Y"))  
        #----------------------------set Room Rev dtypes------------------------------------------
        histfile4['Total_Amount'] = histfile4['Total_Amount'].fillna(value=0)
        histfile4['Total_Amount'] = histfile4['Total_Amount'].astype(float)
        
        histfile4['No_of_Rooms'] = histfile4['No_of_Rooms'].fillna(value=0)
        histfile4['No_of_Rooms'] = histfile4['No_of_Rooms'].astype(int)          
        #-----------------------------------------------------------------------------------------
        histfile4.to_csv(cm_histcsvpth+'\\'+'{}_HistoricData.txt'.format(hotelname),index=False, sep='|')
        histfile4.to_csv(backup_hist+'\\'+'{}_HistoricData_{}.txt'.format(hotelname,tday2),index=False, sep='|')
        logging.info("Historic CSV Dumped in to csv !!!")  
#        print("Historic CSV Dumped in to csv !!!")          
        #==============================Call csvValidation Function==========================================
        logging.info('CSV Validation Check started ...')
#        print('CSV Validation Check started ...')
        if condn2 == 0:
                    csv_val.csv_validate(histfile4,hotelname,std_pth,filecondn)
        else:
            pass
        logging.info('-------------------------------------------------------------------------\n')
        print('-------------------------------------------------------------------------\n')
        #===================================================================================================       
    else:
        pass

   
#-----------------------------------------------------------------------------------------------------------------------   
#--------------------------------------Ocupancy Conversion--------------------------------------------------------------
            
def Occ_conv(inputfile,chman,htl,isell_pth,std_pth,statuscode):
    logging.debug('------------------------------------------------------------')
    logging.debug('Module:All_Occ_Fun, SubModule:Occ_conv')
    #===================drop rows containing CheckIn CheckOut blank======================
    inputfile.dropna(axis=0,subset=['CheckIn','CheckOut'],inplace=True)
    logging.debug('dropped rows containing CheckIn CheckOut blank')
    #====================================================================================
    df_date1=pd.concat([pd.DataFrame({
            'occupancydate' : pd.date_range(row.CheckIn, row.CheckOut),
            'Booking_ID'  : row.Booking_ID,                      
            'Booking_Date' : row.Booking_Date,
            'Hotel' : row.Hotel,
            'Channel' : row.Channel,         
            'Status' : row.Status,
            'CheckIn': row.CheckIn,
            'CheckOut': row.CheckOut,
            'UpdatedDate': row.UpdatedDate,
            'No_of_Rooms':row.No_of_Rooms,
            'Total_Amount': row.Total_Amount
            },
    columns=['occupancydate','Booking_ID','Booking_Date', 'Hotel', 'Channel', 'Status', 'CheckIn', 'CheckOut','UpdatedDate',
       'No_of_Rooms','Total_Amount']) for i, row in inputfile.iterrows()], ignore_index=True)

    logging.debug('Occupancy Conversion done, data exploaded')
    #========================Set Date Format====================================
    df_date1['Booking_Date'] = pd.to_datetime(df_date1['Booking_Date'], format="%Y-%m-%d")
    df_date1['CheckIn'] = pd.to_datetime(df_date1['CheckIn'], format="%Y-%m-%d")
    df_date1['CheckOut'] = pd.to_datetime(df_date1['CheckOut'], format="%Y-%m-%d")
    logging.debug('Set standard date format ("%Y-%m-%d") to datecolumn')
    
    #========================Set Room Rev Formats==============================
    # df_date1['No_of_Rooms'] = df_date1['No_of_Rooms'].astype("Int32")
    df_date1['No_of_Rooms'] = df_date1['No_of_Rooms'].astype(int)
    df_date1['Total_Amount'] = df_date1['Total_Amount'].astype(float)
    logging.debug('Set datatypes for No_of_Rooms and Total_Amount, int and float respectively')

    # ===================== Occupancy History section ===============================
    #  create occy history
    #  try to read occy history file if not then write error  msg and pass and dump new occy history
    occ_histpath = std_pth + "\Performance_Tracker\Occupancy_Historic_CSVs"
    # Searching for occupancy History if any then appending it to fresh data else process with fresh data
    try:
        occ_hist = pd.read_csv(occ_histpath + "/{}_OCC_Hist.txt".format(htl), sep='|')
        print("Occupancy History present for {}".format(htl))

        occ_hist['Booking_Date'] = pd.to_datetime(occ_hist['Booking_Date'], format="%Y-%m-%d")
        occ_hist['CheckIn'] = pd.to_datetime(occ_hist['CheckIn'], format="%Y-%m-%d")
        occ_hist['CheckOut'] = pd.to_datetime(occ_hist['CheckOut'], format="%Y-%m-%d")
        occ_hist['occupancydate'] = pd.to_datetime(occ_hist['occupancydate'], format="%Y-%m-%d")
        occ_hist['UpdatedDate'] = pd.to_datetime(occ_hist['UpdatedDate'], format="%Y-%m-%d")
        occ_hist['No_of_Rooms'] = occ_hist['No_of_Rooms'].astype(int)
        occ_hist['Total_Amount'] = occ_hist['Total_Amount'].astype(float)

        revappend = Revenomix_Append.IDSAppend()
        # creating a key column combination of occupancydate and Booking_ID and appending it on the basis of key column and removing duplicates
        keydf1 = revappend.key_generator(df=df_date1, columns=["occupancydate", "Booking_ID"])
        keydf2 = revappend.key_generator(df=occ_hist, columns=["occupancydate", "Booking_ID"])

        df_date1 = revappend.append(keydf2, keydf1, key='key_col') # Removing duplicates
        df_date= df_date1.drop(['key_col'], axis = 1)
        df_date  = pd.DataFrame(df_date)

        # df_date = df_daten.drop_duplicates()
        # df_date = df_date1.append(occ_hist, ignore_index=True, sort=False)
        print("Occupancy History is appended with fresh data for {}".format(htl))

    except Exception as E:
        print(E)
        print("Occupancy History not found for {} , creating a new Occupancy History".format(htl))
        df_date = df_date1

    df_date.to_csv(occ_histpath + "/{}_OCC_Hist.txt".format(htl), index=False, sep='|')   # Dump for  Occupancy_History

    #============================================================================

    #============================================================================
    df_date['dtDif'] = (df_date['CheckOut']-df_date['occupancydate']).apply(lambda x: x/np.timedelta64(1,'D'))
    df_date['Arrivals'] = np.where((df_date['CheckIn']==df_date['occupancydate']), 1 , 0)
    df_date['Arrivals'] = df_date['Arrivals']*df_date['No_of_Rooms']

# ============================Average Lead Calculation=======================
    df_date['AvgLead'] = (df_date['CheckIn'] - df_date['Booking_Date']).apply(lambda x: x/np.timedelta64(1,'D'))  
    df_date['AvgLead'] = df_date['AvgLead'].astype(int)
    logging.debug('Calculated dtDif, Arrivals and AvgLead')

    df_date['Channel'].fillna(value='YourWeb',inplace=True)
    logging.debug("Replaced Blank Channel values with 'YourWeb'")
    
    df_date['Status'].fillna(value='Cancelled',inplace=True)
    logging.debug("Replaced Blank Status values with 'Cancelled'")
    # ===========================Binary StatusCode Column ======================
    df_date['statuscode'] = df_date['Status'].map(statuscode)
    logging.debug("Mapped statuscode mapping on Status column")
    
    #------------------------check status mapping------------------------------
    df_date['statuscode'] = df_date['statuscode'].fillna(value=2)
    df_date['statuscode'] = df_date['statuscode'].astype(int)
    st_vals = list(set(df_date['statuscode']))
    
    if 2 in st_vals:
        logging.info('#### Status Value in Column not mapped for {}, \npl check unmapped folder. Thanks ####'.format(htl))
        print('#### Status Value in Column not mapped for {}, \npl check unmapped folder. Thanks ####'.format(htl))
        unmapdf = pd.DataFrame(df_date[df_date['statuscode'] == 2])
        unmapdf2 = pd.DataFrame(unmapdf.loc[:,['Status','statuscode']])
        unmapdf2.drop_duplicates(subset='Status',inplace=True)
        unmapdf2['statuscode'] = 'NotMapped'
        unmapdf2.to_csv(std_pth+r'\unmapped_data\statusMAP_{}_{}.csv'.format(htl,tday2))
        logging.debug('Status should be 1 or 0, status with value 2 are unmapped in mapping ::')
        logging.debug(unmapdf2)    
        sys.exit()
    else:
        pass
    #--------------------------------------------------------------------------
    logging.info('Mapped statuscode binary column to Status')
#    print('Mapped statuscode binary column to Status')
    df_date2 = df_date.query('(dtDif > 0)')
    logging.debug('Sliced dataframe where (dtDif > 0) where dtDif = CheckOut - occupancydate')

    df_date3 = pd.DataFrame(df_date2)

    #==========================Add LOS , ADR, Rev PD, Rate, Revenue==============================
    df_date3['LOS'] = (df_date3['CheckOut'] - df_date3['CheckIn']).apply(lambda x: x/np.timedelta64(1,'D'))
    df_date3['LOS'] = df_date3['LOS'].astype(int)
    logging.debug("LOS column added to dataframe, where LOS = CheckOut - CheckIn")
    
    df_date3['RevPD'] = df_date3['Total_Amount'] / np.where(df_date3['LOS'] == 0,1,df_date3['LOS'])
    df_date3['RevPD'] = df_date3['RevPD'].astype(float)
    df_date3['RevPD'].fillna(value=0,inplace=True)
    logging.debug("RevPD column added, where RevPD = Total_Amount/LOS")
    
    df_date3['ADR'] = df_date3['Total_Amount'].div(np.where(df_date3['LOS'] == 0,1,df_date3['LOS'])*df_date3['No_of_Rooms'])
    logging.debug("ADR column added, where ADR = Total_Amount/(LOS x No_of_Rooms)")
    
    df_date3['Rate'] = df_date3['ADR']
    
    df_date3['Revenue'] = df_date3['Total_Amount']
    df_date3['Dow'] = df_date3['occupancydate'].apply(lambda x: x.strftime('%a'))
    df_date3['Month'] = df_date3['occupancydate'].apply(lambda x: x.strftime('%b'))
    df_date3['Year'] = df_date3['occupancydate'].apply(lambda x: x.strftime('%Y'))
    df_date3['WDWE'] = df_date3['Dow'].map(WDWE)   
    logging.debug("WDWE dictionary mapped on Dow")
    
    df_etl_data0 = pd.DataFrame(df_date3)
    
    #=============================# STLY Calculation #============================
    #=============================================================================
    logging.info("Identifying the transactions that were alive during last year same time")
#    print("Identifying the transactions that were alive during last year same time")
    #====================================================================================
    df_etl_data0['spitLY'] = np.where(df_etl_data0['Booking_Date'] <= lyDate, 1, 0)      
    
    df_etl_data0['spitLY'] = np.where(df_etl_data0['UpdatedDate'] <= lyDate, 
                                np.where(df_etl_data0['statuscode'] == 0, 0, 
                                         df_etl_data0['spitLY']), df_etl_data0['spitLY'])
    
    logging.debug("spitLY binary Column added from Booking_Date and UpdatedDate")
# =============================================================================  
   
    df_etl_data0['RmsSTLY'] = np.where(df_etl_data0['statuscode'] == 0,
                                      np.where(df_etl_data0['occupancydate'] < lyDate, 0, df_etl_data0['No_of_Rooms']),df_etl_data0['No_of_Rooms'])
    
    df_etl_data0['RmsSTLY'] = np.where(df_etl_data0['spitLY'] == 0, 0, df_etl_data0['RmsSTLY']) 
    
    logging.debug("RmsSTLY Column calculated")               
                                   
# =============================================================================
#    print("Calculating the revenue for such bookings")
    #==========================================================================
    df_etl_data0['RevSTLY'] =np.where(df_etl_data0['spitLY']==1,
                                    np.where(df_etl_data0['Rate'] <= 0, df_etl_data0['Revenue']/np.where(df_etl_data0['LOS']==0, 1, df_etl_data0['LOS']), 
                                                         df_etl_data0['Rate'] * df_etl_data0['No_of_Rooms']), 0)
     
#    print("Calculated the Revenue for Day Use for SPITLY")
    #==============================================
    df_etl_data0['RevSTLY'] =np.where(df_etl_data0['spitLY']==1, 
                                   np.where(df_etl_data0['LOS'] == 0, df_etl_data0['Revenue'], 
                                         df_etl_data0['RevSTLY']), df_etl_data0['RevSTLY'])
    
    df_etl_data0['RevSTLY'] =np.where(df_etl_data0['RevSTLY'] < 0 , 0, df_etl_data0['RevSTLY'])
    
# =============================================================================
   

    df_etl_data0['RevSTLY'] = np.where(df_etl_data0['spitLY'] == 1, 
                                   np.where(df_etl_data0['occupancydate'] < lyDate, 
                                         np.where(df_etl_data0['statuscode'] == 0, 0, df_etl_data0['RevPD']),df_etl_data0['RevSTLY']), 0)
    
    logging.debug("RevSTLY Column calculated")        
    return(df_etl_data0)




