#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 13:09:10 2019

@author: jmanning
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
import json
    
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
            telemetrystatus_df['logger_change'][i]=telemetrystatus_df['logger_change'][i].replace('，',',')
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
        start_time=dict['end_time']  # if dict['end_time'] is wrong, please comment this code
    except:
        dict={}
        tele_dict={}
    telemetrystatus_df=read_telemetrystatus(telemetry_status)
    #download the data of telementry
    tele_df=read_telemetry(path='/home/jmanning/Desktop/telementry.csv')   #tele_df means telemeterd data
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

    for j in range(len(telemetrystatus_df)):
        try:
            tele_dict[telemetrystatus_df['Boat'][j]]
        except:
            tele_dict[telemetrystatus_df['Boat'][j]]=pd.DataFrame(data=None,\
                     columns=['time','temp','depth','lat','lon','dpo_temp','dpo_depth','gmf_temp','gmf_depth'])
        for i in valuable_tele_df.index:  #valuable_tele_df is the valuable telemetry data during start time and end time 
            if int(valuable_tele_df['vessel_n'][i].split('_')[1])==telemetrystatus_df['Vessel#'][j]:
                print()
                try:     #try to get doppio data in the same locattion
                    dpo_temp,dpo_depth=dpo.get_doppio(lat=float(valuable_tele_df['lat'][i]),lon=float(valuable_tele_df['lon'][i]),depth='bottom',\
                                                      time=valuable_tele_df['time'][i].strftime("%Y-%m-%d %H:%M:%S"),fortype='tempdepth') 
                except:
                    dpo_temp,dpo_depth=np.nan,np.nan
                try:    #try to get the gomofs data in the same location
                    gmf_temp,gmf_depth=gmf.get_gomofs(lat=float(valuable_tele_df['lat'][i]),lon=float(valuable_tele_df['lon'][i]),depth='bottom',\
                                                      date_time=valuable_tele_df['time'][i],fortype='tempdepth')
                except:
                    gmf_temp,gmf_depth=np.nan,np.nan
                tele_dict[telemetrystatus_df['Boat'][j]]=tele_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=[[valuable_tele_df['time'][i],\
                         float(valuable_tele_df['temp'][i]),float(valuable_tele_df['depth'][i]),float(valuable_tele_df['lat'][i]),\
                         float(valuable_tele_df['lon'][i]),dpo_temp,dpo_depth,gmf_temp,gmf_depth]],\
                            columns=['time','temp','depth','lat','lon','dpo_temp','dpo_depth','gmf_temp','gmf_depth']).iloc[0],ignore_index=True)
                valuable_tele_df=valuable_tele_df.drop(i)  #if this line has been classify, delete this line
    dict['tele_dict']=tele_dict
    return dict
          
telemetry_status='/home/jmanning/Downloads/telemetry_status - fitted.csv'
start_time_str='2018-7-3'
end_time_str='2018-9-1'
start_time=datetime.strptime(start_time_str,'%Y-%m-%d')
end_time=datetime.strptime(end_time_str,'%Y-%m-%d') 
filepath='/home/jmanning/Desktop/data_dict/dict_obsdpogmf.p'

try:
    with open(filepath,'rb') as fp:
        dict = pickle.load(fp)
except:
    dict={}  
obsdpogmf=classify_by_boat(telemetry_status,start_time,end_time,dict)
for i in obsdpogmf['tele_dict'].keys():
    if len(obsdpogmf['tele_dict'][i])>0:
        obsdpogmf['tele_dict'][i].drop_duplicates(subset=['time'],keep='first',inplace=True)
        obsdpogmf['tele_dict'][i].index=range(len(obsdpogmf['tele_dict'][i]))
with open(filepath,'wb') as fp:
    pickle.dump(obsdpogmf,fp,protocol=pickle.HIGHEST_PROTOCOL)


