#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 13:09:10 2019

@author: leizhao
"""
import pandas as pd
from datetime import datetime
import time
import doppio_modules as dpo
import gomofs_modules as gmf
import numpy as np
try:
    import cPickle as pickle
except ImportError:
    import pickle
import re
import FVCOM_modules as fv
    
def read_telemetrystatus(path_name):
    """read the telementry_status, then return the useful data"""
    data=pd.read_csv(path_name)
    #find the data lines number in the file('telemetry_status.csv')
    for i in range(len(data['Unnamed: 0'])):
        if data['Unnamed: 0'].isnull()[i]:
            data_line_number=i
            break
    #read the data about "telemetry_status.csv"
    telemetrystatus_df=pd.read_csv(path_name,nrows=data_line_number)
    telemetrystatus_df.columns=['Boat', 'Status (as 13 Nov 2018)','Vessel#', 'Funding', 'Program', 'Captain',
       'email address', 'phone', 'Port', 'Techs', 'Visit_Dates for telemetry',
       'Aquatec-SN', 'Lowell-SN', 'logger_change',
       'ESN','Other ESNs used', 'wifi?', 'wants weather?',
       'Notes (see individual tabs for historical notes)', 'add mail address!',
       'LI Firmware', 'image_file}', 'Fixed vs. Mobile','length of vessel', 'AP3 Batch',
       'weather_code']
    for i in range(len(telemetrystatus_df)):
        telemetrystatus_df['Boat'][i]=telemetrystatus_df['Boat'][i].replace("'","")
        if not telemetrystatus_df['Lowell-SN'].isnull()[i]:
            telemetrystatus_df['Lowell-SN'][i]=telemetrystatus_df['Lowell-SN'][i].replace('，',',')
        if not telemetrystatus_df['logger_change'].isnull()[i]:
            telemetrystatus_df['logger_change'][i]=re.sub('，',',',telemetrystatus_df['logger_change'][i])
    return telemetrystatus_df


def read_telemetry(path='https://www.nefsc.noaa.gov/drifter/emolt.dat',endtime=datetime.now()):
    """read the telemetered data and fix a standard format, the return the standard data"""
    while True:
        tele_df=pd.read_csv(path,sep='\s+',names=['vessel_n','esn','month','day','Hours','minates','fracyrday',\
                                          'lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])
        if int(tele_df['year'][len(tele_df)-2])==endtime.year:
            break
        else:
            print('check the web:https://www.nefsc.noaa.gov/drifter/emolt.dat.')
            time.sleep(600)
    return tele_df

def classify_by_boat(telemetry_status,start_time,end_time,dict):
    
    try:
        tele_dict=dict['tele_dict']
        Doppio_dict=dict['Doppio']
        GoMOLFs_dict=dict['GoMOLFs']
        FVCOM_dict=dict['FVCOM']
        start_time=dict['end_time']  # if dict['end_time'] is wrong, please comment this code
        
    except:
        dict={}
        tele_dict={}
        Doppio_dict={}
        GoMOLFs_dict={}
        FVCOM_dict={}
    telemetrystatus_df=read_telemetrystatus(telemetry_status)
    #download the data of telementry
    tele_df=read_telemetry()#path='/home/jmanning/Desktop/telementry.csv')   #tele_df means telemeterd data
    #screen out the data of telemetry in interval
    valuable_tele_df=pd.DataFrame(data=None,columns=['vessel_n','esn','time','lon','lat','depth','temp'])#use to save the data from start time to end time
    for i in range(len(tele_df)):
        tele_time_str=str(tele_df['year'].iloc[i])+'-'+str(tele_df['month'].iloc[i])+'-'+str(tele_df['day'].iloc[i])+' '+\
                                         str(tele_df['Hours'].iloc[i])+':'+str(tele_df['minates'].iloc[i])+':'+'00'
        tele_time=datetime.strptime(tele_time_str,'%Y-%m-%d %H:%M:%S')
        if start_time<tele_time<=end_time:
            valuable_tele_df=valuable_tele_df.append(pd.DataFrame(data=[[tele_df['vessel_n'][i],tele_df['esn'][i],tele_time,tele_df['lon'][i],\
                                                                         tele_df['lat'][i],tele_df['depth'][i],tele_df['temp'][i]]],\
                                                                        columns=['vessel_n','esn','time','lon','lat','depth','temp']))
    #clean the index of valuable telementry data
    if len(valuable_tele_df)>0:
        valuable_tele_df.index=range(len(valuable_tele_df)) 
        dict['end_time']=valuable_tele_df['time'][len(valuable_tele_df)-1]
    print(len(valuable_tele_df))
    for j in range(len(telemetrystatus_df)):
        if telemetrystatus_df['Boat'][j] not in tele_dict.keys():
            tele_dict[telemetrystatus_df['Boat'][j]]=pd.DataFrame(data=None,columns=['time','temp','depth','lat','lon'])
        if telemetrystatus_df['Boat'][j] not in Doppio_dict.keys():
            Doppio_dict[telemetrystatus_df['Boat'][j]]=pd.DataFrame(data=None,columns=['time','temp','depth','lat','lon'])
        if telemetrystatus_df['Boat'][j] not in GoMOLFs_dict.keys():
            GoMOLFs_dict[telemetrystatus_df['Boat'][j]]=pd.DataFrame(data=None,columns=['time','temp','depth','lat','lon'])
        if telemetrystatus_df['Boat'][j] not in FVCOM_dict.keys():
            FVCOM_dict[telemetrystatus_df['Boat'][j]]=pd.DataFrame(data=None,columns=['time','temp','depth','lat','lon'])
        
        dop_nu=[]
        gmf_nu=[]
        tele_nu=[]
        fvc_nu=[]
        for i in valuable_tele_df.index:  #valuable_tele_df is the valuable telemetry data during start time and end time 
            if int(valuable_tele_df['vessel_n'][i].split('_')[1])==telemetrystatus_df['Vessel#'][j]:
                dtime=valuable_tele_df['time'][i]
                latp=float(valuable_tele_df['lat'][i])
                lonp=float(valuable_tele_df['lon'][i])
                tele_nu.append([dtime,latp,lonp,float(valuable_tele_df['temp'][i]),float(valuable_tele_df['depth'][i])])
                
                try:     #try to get doppio data in the same locattion
#                    print('dpo:',lat,lon,dtime)
                    dpo_temp,dpo_depth=dpo.get_doppio(lat=latp,lon=lonp,depth='bottom',time=dtime.strftime("%Y-%m-%d %H:%M:%S"),fortype='tempdepth') 
                except:
                    dpo_temp,dpo_depth=np.nan,np.nan
                dop_nu.append([dtime,latp,lonp,dpo_temp,dpo_depth])
                try:    #try to get the gomofs data in the same location
                    gmf_temp,gmf_depth=gmf.get_gomofs(date_time=dtime,lat=latp,lon=lonp,depth='bottom',fortype='tempdepth')
                except:
                    gmf_temp,gmf_depth=np.nan,np.nan
                gmf_nu.append([dtime,latp,lonp,gmf_temp,gmf_depth])
                try:
                    FV_temp,FV_depth=fv.get_FVCOM_bottom_temp(lati=latp,loni=lonp,dtime=dtime,layer=-1)
                except:
                    FV_temp,FV_depth=np.nan,np.nan
                fvc_nu.append([dtime,latp,lonp,FV_temp,FV_depth])
                valuable_tele_df=valuable_tele_df.drop(i)  #if this line has been classify, delete this line
        if len(dop_nu)>0:
            Doppio_dict[telemetrystatus_df['Boat'][j]]=Doppio_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=dop_nu,\
                            columns=['time','lat','lon','temp','depth']),ignore_index=True)
        if len(gmf_nu)>0:
            GoMOLFs_dict[telemetrystatus_df['Boat'][j]]=GoMOLFs_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=gmf_nu,\
                            columns=['time','lat','lon','temp','depth']),ignore_index=True)
        if len(tele_nu)>0:
            tele_dict[telemetrystatus_df['Boat'][j]]=tele_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=tele_nu,\
                            columns=['time','lat','lon','temp','depth']),ignore_index=True)
        if len(fvc_nu)>0:
            FVCOM_dict[telemetrystatus_df['Boat'][j]]=tele_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=fvc_nu,\
                            columns=['time','lat','lon','temp','depth']),ignore_index=True)
        
    dict['tele_dict']=tele_dict
    dict['Doppio']=Doppio_dict
    dict['GoMOLFs']=GoMOLFs_dict
    dict['FVCOM']=FVCOM_dict
    return dict

          
telemetry_status='/home/jmanning/Downloads/telemetry_status - fitted.csv'
start_time_str='2018-7-1'
#end_time_str='2019-4-2'
start_time=datetime.strptime(start_time_str,'%Y-%m-%d')
#end_time=datetime.strptime(end_time_str,'%Y-%m-%d') 
end_time=datetime.now()
filepath='/home/jmanning/Desktop/data_dict/dict_obsdpogmf44.p'
#filepath2='/home/jmanning/Desktop/data_dict/dict_obsdpogmf.pkl'
try:
#    dict=pd.read_pickle(filepath2)
    with open(filepath,'rb') as fp:
        dict = pickle.load(fp)
except:
    dict={}  
obsdpogmf=classify_by_boat(telemetry_status,start_time,end_time,dict)
#for i in obsdpogmf['tele_dict'].keys():
#    if len(obsdpogmf['tele_dict'][i])>0:
#        obsdpogmf['tele_dict'][i].drop_duplicates(subset=['time'],keep='last',inplace=True)
#        obsdpogmf['tele_dict'][i].index=range(len(obsdpogmf['tele_dict'][i]))
#        obsdpogmf['Doppio'][i].drop_duplicates(subset=['time'],keep='last',inplace=True)
#        obsdpogmf['Doppio'][i].index=range(len(obsdpogmf['Doppio'][i]))
#        obsdpogmf['GoMOLFs'][i].drop_duplicates(subset=['time'],keep='last',inplace=True)
#        obsdpogmf['GoMOLFs'][i].index=range(len(obsdpogmf['GoMOLFs'][i]))
#        obsdpogmf['FVCOM'][i].drop_duplicates(subset=['time'],keep='last',inplace=True)
#        obsdpogmf['FVCOM'][i].index=range(len(obsdpogmf['FVCOM'][i]))
#with open(filepath,'wb') as fp:
#    pickle.dump(obsdpogmf,fp,protocol=pickle.HIGHEST_PROTOCOL)
#obsdpogmf.to_pickle(filepath2)

#FVCOM_dict=dict['FVCOM']
#for i in dict['FVCOM'].keys():
#    if len(FVCOM_dict[i])>0:
#        for j in range(len(FVCOM_dict[i])):
#            lat=FVCOM_dict[i]['lat'][j]
#            lon=FVCOM_dict[i]['lon'][j]
#            dtime=FVCOM_dict[i]['time'][j]
#            try:
#                FV_temp,FV_depth=fv.get_FVCOM_bottom_temp(lati=lat,loni=lon,dtime=dtime,layer=-1)
#            except:
#                FV_temp,FV_depth=np.nan,np.nan
#            FVCOM_dict[i]['temp'][j],FVCOM_dict[i]['depth'][j]=FV_temp,FV_depth
#    
#dict['FVCOM']=FVCOM_dict
#filepath='/home/jmanning/Desktop/data_dict/dict_obsdpogmfneww.p'
#
#with open(filepath,'wb') as fp:
#    pickle.dump(dict,fp,protocol=pickle.HIGHEST_PROTOCOL)
#obsdpogmf.to_pickle(filepath2)

















