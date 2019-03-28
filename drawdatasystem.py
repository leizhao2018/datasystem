#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 14:12:41 2019

@author: jmanning
"""
from datetime import datetime,timedelta
try:
    import cPickle as pickle
except ImportError:
    import pickle
import matplotlib.pyplot as plt
import numpy as np
import rawdatamodules as rdm
import zlconversions as zl
import time
import os
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(os.path.join(conda_dir, 'share'), 'proj')
os.environ["PROJ_LIB"] = proj_lib
from mpl_toolkits.basemap import Basemap  

def time_series_plot(dict,ax1,ax2,start_time,end_time,size,dictlabel='telemetered',double=False,name='Default'):
    """use in draw time plot series function""" 
    if double==True:
        fig=plt.figure(figsize=(size,0.625*size))
        fig.suptitle(name,fontsize=3*size, fontweight='bold')
        size=min(fig.get_size_inches())
        ax1=fig.add_axes([0.1, 0.1, 0.8,0.33])
        ax2=fig.add_axes([0.1, 0.50, 0.8,0.33])
    ax1.plot_date(dict['time'],dict['temp'],linestyle='-',color='b',alpha=0.5,label='OBS',marker='d',markerfacecolor='b')
    ax2.plot_date(dict['time'],dict['depth'],linestyle='-',color='b',alpha=0.5,label='OBS',marker='d',markerfacecolor='b')
    max_t,min_t=np.nanmax(dict['temp'].values),np.nanmin(dict['temp'].values)
    max_d,min_d=np.nanmax(dict['depth'].values),np.nanmin(dict['depth'].values)
    try:
        doppio_dict=dict[['time','dpo_temp','dpo_depth']]
        doppio_dict=doppio_dict.dropna()
        if len(doppio_dict)>0:
            ax1.plot_date(doppio_dict['time'],doppio_dict['dpo_temp'],linestyle='--',color='y',alpha=0.5,label='DOPPIO',marker='d',markerfacecolor='y')
            ax2.plot_date(doppio_dict['time'],doppio_dict['dpo_depth'],linestyle='--',color='y',alpha=0.5,label='DOPPIO',marker='d',markerfacecolor='y')
        doppiomax_t,doppiomin_t=np.nanmax(doppio_dict['dpo_temp'].values),np.nanmin(doppio_dict['dpo_temp'].values)
        doppiomax_d,doppiomin_d=np.nanmax(doppio_dict['dpo_depth'].values),np.nanmin(doppio_dict['dpo_depth'].values) 
    except:
        doppiomax_t,doppiomin_t,doppiomax_d,doppiomin_d=-9999,9999,-99999,99999
    try: 
        gomofs_dict=dict[['time','gmf_temp','gmf_depth']]
        gomofs_dict=gomofs_dict.dropna()   
        if len(gomofs_dict)>0:         
            ax1.plot_date(gomofs_dict['time'],gomofs_dict['gmf_temp'],linestyle='-.',color='r',alpha=0.5,label='GOMOFS',marker='d',markerfacecolor='r')
            ax2.plot_date(gomofs_dict['time'],gomofs_dict['gmf_depth'],linestyle='-.',color='r',alpha=0.5,label='GOMOFS',marker='d',markerfacecolor='r')
        gomofsmax_t,gomofsmin_t=np.nanmax(gomofs_dict['gmf_temp'].values),np.nanmin(gomofs_dict['gmf_temp'].values)
        gomofsmax_d,gomofsmin_d=np.nanmax(gomofs_dict['gmf_depth'].values),np.nanmin(gomofs_dict['gmf_depth'].values) 
    except:
        gomofsmax_t,gomofsmin_t,gomofsmax_d,gomofsmin_d=-9999,9999,-99999,99999
            
    max_temp=max(max_t,doppiomax_t,gomofsmax_t)
    min_temp=min(min_t,doppiomin_t,gomofsmin_t)
    max_depth=max(max_d,doppiomax_d,gomofsmax_d)
    min_depth=min(min_d,doppiomin_d,gomofsmin_d)
    diff_temp=max_temp-min_temp
    diff_depth=max_depth-min_depth
    if diff_temp==0:
        textend_lim=0.1
    else:
        textend_lim=diff_temp/8.0
    if diff_depth==0:
        dextend_lim=0.1
    else:
        dextend_lim=diff_depth/8.0 

    ax1.legend(prop={'size': 0.75*size})
    ax1.set_title(dictlabel)
    ax1.set_ylabel('Celius',fontsize=size)  
    ax1.set_ylim(min_temp-textend_lim,max_temp+textend_lim)
    ax1.axes.title.set_size(2*size)
    if len(dict)==1:
        ax1.set_xlim((dict['time'][0]-timedelta(days=3)),(dict['time'][0]+timedelta(days=4)))
    ax1.axes.get_xaxis().set_visible(False)
    ax1.tick_params(labelsize=0.75*size)
    ax12=ax1.twinx()
    ax12.set_ylabel('Fahrenheit',fontsize=size)
    #conversing the Celius to Fahrenheit
    ax12.set_ylim((max_temp+textend_lim)*1.8+32,(min_temp-textend_lim)*1.8+32)
    ax12.invert_yaxis()
    ax12.tick_params(labelsize=0.75*size)
    #The parts of lable
    ax2.legend(prop={'size': 0.75*size})
    ax2.set_ylabel('depth(m)',fontsize=size)
    ax2.set_ylim(min_depth-dextend_lim,max_depth+dextend_lim)
    if len(dict)==1:
        ax2.set_xlim(dict['time'][0]-timedelta(days=3),dict['time'][0]+timedelta(days=4))
    ax2.tick_params(labelsize=0.75*size)
    ax22=ax2.twinx()
    ax22.set_ylabel('depth(feet)',fontsize=size)
    ax22.set_ylim((max_depth+dextend_lim)*3.28084,(min_depth-dextend_lim)*3.28084)
    ax22.invert_yaxis()
    ax22.tick_params(labelsize=0.75*size)
    
    
def draw_time_series_plot3(tele_dict,name,start_time,end_time,path_picture_save,dpi=300,mindepth=10):  
    """
    if don't want to report the record file, let the record_file=False
    use to draw time series plot """

    for i in range(len(tele_dict)):
        if type(tele_dict['time'][i])==str:
            tele_dict['time'][i]=datetime.strptime(tele_dict['time'][i],'%Y-%m-%d %H:%M:%S')
        if start_time<=tele_dict['time'][i]<=end_time:
            continue
        else:
            tele_dict=tele_dict.drop(i)
    tele_dict.index=range(len(tele_dict))
    if len(tele_dict)>0:  
        for i in range(len(tele_dict)):
            if abs(tele_dict['depth'][i])<abs(mindepth):
                tele_dict=tele_dict.drop(i)
        tele_dict.index=range(len(tele_dict)) 
    if len(tele_dict)>0:
        if tele_dict['depth'][0]>0:
            tele_dict[['depth','dpo_depth','gmf_depth']]=-1*tele_dict[['depth','dpo_depth','gmf_depth']]

        fig=plt.figure(figsize=(14,8))
        size=max(fig.get_size_inches())
        fig.suptitle(name,fontsize=2*size, fontweight='bold')
        ax1=fig.add_axes([0.1, 0.5, 0.8,0.32])
        ax2=fig.add_axes([0.1, 0.1, 0.8,0.32])
        time_series_plot(tele_dict,ax1,ax2,start_time,end_time,size,dictlabel='Telemetered')
    
        if not os.path.exists(path_picture_save+'/picture/'+name+'/'):
            os.makedirs(path_picture_save+'/picture/'+name+'/')
        plt.savefig(path_picture_save+'/picture/'+name+'/'+start_time.strftime('%Y-%m-%d %H:%M:%S')+'_'+end_time.strftime('%Y-%m-%d %H:%M:%S')+'test.png',dpi=dpi)

    
def draw_map(tele_dict,name,start_time_local,end_time_local,path_picture_save,dpi=300):
    """
    the type of start_time_local and end time_local is datetime.datetime
    use to draw the location of raw file and telemetered produced"""
    #creat map
    #Create a blank canvas  
    fig=plt.figure(figsize=(8,8.5))
    fig.suptitle('F/V '+name,fontsize=24, fontweight='bold')
    if len(tele_dict)>0:
        start_time=tele_dict['time'][0]
        end_time=tele_dict['time'][len(tele_dict)-1]
    if type(start_time)!=str:
        start_time=start_time.strftime('%Y-%m-%d')
        end_time=end_time.strftime('%Y-%m-%d')
    ax=fig.add_axes([0.02,0.02,0.9,0.9])
    ax.set_title(start_time+'-'+end_time)
    ax.axes.title.set_size(16)
    
    min_lat=min(tele_dict['lat'])
    max_lat=max(tele_dict['lat'])
    max_lon=min(tele_dict['lon'])
    min_lon=max(tele_dict['lon'])
    #keep the max_lon-min_lon>=2
    if (max_lon-min_lon)<=2:
        max_lon=1-(max_lon-min_lon)/2.0+(max_lon+min_lon)/2.0
        min_lon=max_lon-2
    #adjust the max and min,let map have the same width and height 
    if (max_lon-min_lon)>(max_lat-min_lat):
        max_lat=max_lat+((max_lon-min_lon)-(max_lat-min_lat))/2.0
        min_lat=min_lat-((max_lon-min_lon)-(max_lat-min_lat))/2.0
    else:
        max_lon=max_lon+((max_lat-min_lat)-(max_lon-min_lon))/2.0
        min_lon=min_lon-((max_lat-min_lat)-(max_lon-min_lon))/2.0
    #if there only one data in there
    while(not zl.isConnected()):
        time.sleep(120)
    try:
        service = 'Ocean_Basemap'
        xpixels = 5000 
        #Build a map background
        map=Basemap(projection='mill',llcrnrlat=min_lat-0.1,urcrnrlat=max_lat+0.1,llcrnrlon=min_lon-0.1,urcrnrlon=max_lon+0.1,\
                resolution='f',lat_0=(max_lat+min_lat)/2.0,lon_0=(max_lon+min_lon)/2.0,epsg = 4269)
        map.arcgisimage(service=service, xpixels = xpixels, verbose= False)
        if max_lat-min_lat>=3:
            step=int((max_lat-min_lat)/5.0*10)/10.0
        else:
            step=0.5
        
        # draw parallels.
        parallels = np.arange(0.,90.0,step)
        map.drawparallels(parallels,labels=[0,1,0,0],fontsize=10,linewidth=0.0)
        # draw meridians
        meridians = np.arange(180.,360.,step)
        map.drawmeridians(meridians,labels=[0,0,0,1],fontsize=10,linewidth=0.0)
        #Draw a scatter plot
        tele_lat,tele_lon=rdm.to_list(tele_dict['lat'],tele_dict['lon'])
        tele_x,tele_y=map(tele_lon,tele_lat)
        ax.plot(tele_x,tele_y,'b*',markersize=6,alpha=0.5,label='telemetry')
        ax.legend()
        if not os.path.exists(path_picture_save+'/picture/'+name+'/'):
            os.makedirs(path_picture_save+'/picture/'+name+'/')
        plt.savefig(path_picture_save+'/picture/'+name+'/'+'location'+'_'+start_time_local.strftime('%Y-%m-%d')+'_'+end_time_local.strftime('%Y-%m-%d')+'.png',dpi=dpi)
        print(name+' finished draw!')
    except:
        print(name+' need redraw!')


dictionary_path='/home/jmanning/Desktop/data_dict/dictdatatest2019_nn.p'
picture_save='/home/jmanning/Desktop/qwe/' #use
end_time=datetime.now()
start_time=end_time-timedelta(days=30)  #30 means 30 days data

with open(dictionary_path, 'rb') as fp:
    dict = pickle.load(fp)   
tele_dict=dict['tele_dict']
index=tele_dict.keys()
for i in index:
    try:
        draw_map(tele_dict[i],i,start_time,end_time,picture_save,dpi=300)
        draw_time_series_plot3(tele_dict[i],i,start_time,end_time,picture_save,dpi=300)
        print(i+' finished time series plot!')
    except:
        print(i+' no data!')

