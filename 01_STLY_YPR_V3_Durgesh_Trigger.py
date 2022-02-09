# -*- coding: utf-8 -*-


import os

#==========================std paths===========================================
#-------------------------src path---------------------------------------------
src_pth = r'C:\All_In_One_STLY_YPR\src'
#--------------------------isell setup path -----------------------------------
isell_pth = r'C:\All_In_One_iSell'

#--------------------------ypr setup path--------------------------------------

ypr_pth = r'C:\All_In_One_STLY_YPR'
#==============================================================================

os.chdir(src_pth)
import All_OccYPR as stly_ypr

#------------Enter Standard Account Manager Name-------------------------------
acc_man = 'Durgesh'
#------------------------------------------------------------------------------
#-----------------------Log:DEBUG/INFO-----------------------------------------
log_flag = 'INFO'
perf_Track = 0

stly_ypr.stly_pth(acc_man, perf_Track, isell_pth, ypr_pth, log_flag)