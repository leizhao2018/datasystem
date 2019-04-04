"""
Created on Fri Mar 15 13:09:10 2019

@author: leizhao
"""
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime,timedelta
import zlconversions as zl
import time
import os
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(os.path.join(conda_dir, 'share'), 'proj')
os.environ["PROJ_LIB"] = proj_lib
from mpl_toolkits.basemap import Basemap


#from datetime import datetime
import pickle
#import raw_tele_modules as rdm




def check_time(df,time_header,start_time,end_time):
    '''keep the type of time is datetime
    input start time and end time, return the data between start time and end time'''
    for i in range(len(df)):
        if type(df[time_header][i])==str:
            df[time_header][i]=datetime.strptime(df[time_header][i],'%Y-%m-%d %H:%M:%S')
        if start_time<=df[time_header][i]<=end_time:
            continue
        else:
            df=df.drop(i)
    df.index=range(len(df))
    return df


def check_depth(df,mindepth):
    '''keep the depth is out of 10 and correct the format of depth for example:-20 '''
    if len(df)>0:  
        for i in df.index:
            if abs(df['depth'][i])<abs(mindepth):
                df=df.drop(i)
        df.index=range(len(df))
    for i in range(len(df)):
        if df['depth'][i]>0:
            df['depth'][i]=-1*df['depth'][i]
    return df


def plot(df,ax1,ax2,linestyle='--',color='y',alpha=0.5,label='Telemetered',marker='d',markerfacecolor='y',**kwargs):
    try:
        if len(df)>0:  #the length of dataframe must be bigger than 0
            ax1.plot_date(df['time'],df['temp'],linestyle=linestyle,color=color,alpha=alpha,label=label,marker=marker,markerfacecolor=markerfacecolor)
            ax2.plot_date(df['time'],df['depth'],linestyle=linestyle,color=color,alpha=alpha,label=label,marker=marker,markerfacecolor=markerfacecolor)
        max_t,min_t=np.nanmax(df['temp'].values),np.nanmin(df['temp'].values)
        max_d,min_d=np.nanmax(df['depth'].values),np.nanmin(df['depth'].values) 
    except:
        max_t,min_t,max_d,min_d=-9999,9999,-99999,99999 
    return  max_t,min_t,max_d,min_d
def draw_time_series_plot(dict,name,dtime,path_picture_save,timeinterval=30,dpi=300,mindepth=10):  
    """
    import the dictionary, this dictionary have data about Doppio,GoMOLFs,FVCOM and telemetered or this dict from create_obs_dpo_gmf_dict.py
    the unit of time interval is days
    use to draw time series plot """
    #get the latest time of get data, and back 30 days as start time
    if dtime>dict['tele_dict'][name]['time'][len(dict['tele_dict'][name])-1]:
        start_time=dict['tele_dict'][name]['time'][len(dict['tele_dict'][name])-1]-timedelta(days=timeinterval)
    else:
        start_time=dtime-timedelta(days=timeinterval)
    #through the start time and end time to screen data
    tele_df=check_time(df=dict['tele_dict'][name],time_header='time',start_time=start_time,end_time=dtime).dropna()
    Doppio_df=check_time(df=dict['Doppio'][name],time_header='time',start_time=start_time,end_time=dtime).dropna()
    GoMOLFs_df=check_time(df=dict['GoMOLFs'][name],time_header='time',start_time=start_time,end_time=dtime).dropna()
    FVCOM_df=check_time(df=dict['FVCOM'][name],time_header='time',start_time=start_time,end_time=dtime).dropna()
    #through the parameter of mindepth to screen the data, make sure the depth is out of ten
    tele_dft=check_depth(df=tele_df,mindepth=mindepth)
    Doppio_dft=check_depth(df=Doppio_df,mindepth=mindepth)
    GoMOLFs_dft=check_depth(df=GoMOLFs_df,mindepth=mindepth)
    FVCOM_dft=check_depth(df=FVCOM_df,mindepth=mindepth)
    #if every dataframe is no data,it will print the message that there is no data  and return zero.
    if len(tele_dft)==0 and len(Doppio_dft)==0 and len(GoMOLFs_dft)==0 and len(FVCOM_dft)==0:
        print(name+': no valuable data')
        return 0
    #start to draw the picture.
    fig=plt.figure(figsize=(14,14))
    size=min(fig.get_size_inches())        
    fig.suptitle(name,fontsize=2*size, fontweight='bold')
    ax1=fig.add_axes([0.1, 0.5, 0.8,0.32])
    ax2=fig.add_axes([0.1, 0.1, 0.8,0.32])
    #draw Graph for every module
    Tmax_t,Tmin_t,Tmax_d,Tmin_d=plot(df=tele_dft,ax1=ax1,ax2=ax2,linestyle='--',color='y',alpha=0.5,label='Telemetered',marker='d',markerfacecolor='y')
    Dmax_t,Dmin_t,Dmax_d,Dmin_d=plot(df=Doppio_dft,ax1=ax1,ax2=ax2,linestyle='--',color='r',alpha=0.5,label='DOPPIO',marker='d',markerfacecolor='r')
    Gmax_t,Gmin_t,Gmax_d,Gmin_d=plot(df=GoMOLFs_dft,ax1=ax1,ax2=ax2,linestyle='--',color='b',alpha=0.5,label='GoMOLFs',marker='d',markerfacecolor='b')
    Fmax_t,Fmin_t,Fmax_d,Fmin_d=plot(df=FVCOM_dft,ax1=ax1,ax2=ax2,linestyle='--',color='black',alpha=0.5,label='FVCOM',marker='d',markerfacecolor='black')
    #calculate the max and min of temperature and depth
    MAX_T=max(Tmax_t,Dmax_t,Gmax_t,Fmax_t)
    MIN_T=min(Tmin_t,Dmin_t,Gmin_t,Fmin_t)
    MAX_D=max(Tmax_d,Dmax_d,Gmax_d,Fmax_d)
    MIN_D=min(Tmin_d,Dmin_d,Gmin_d,Fmin_d)
    
    #calculate the limit value of depth and temperature 
    diff_temp=MAX_T-MIN_T
    diff_depth=MAX_D-MIN_D
    if diff_temp==0:
        textend_lim=0.1
    else:
        textend_lim=diff_temp/8.0
    if diff_depth==0:
        dextend_lim=0.1
    else:
        dextend_lim=diff_depth/8.0
    #the parts of label
#    ax1.legend(prop={'size': 0.75*size})
    ax1.legend()
    ax1.set_ylabel('Celius',fontsize=size)  
    ax1.set_ylim(MIN_T-textend_lim,MAX_T+textend_lim)
    ax1.axes.title.set_size(2*size)
    if len(tele_df)==1: #if there only one data, we need change the time limit as one week 
        ax1.set_xlim((tele_df['time'][0]-timedelta(days=3)),(tele_df['time'][0]+timedelta(days=4)))
    ax1.axes.get_xaxis().set_visible(False)
    ax1.tick_params(labelsize=0.75*size)
    ax12=ax1.twinx()
    ax12.set_ylabel('Fahrenheit',fontsize=size)
    #conversing the Celius to Fahrenheit
    ax12.set_ylim((MAX_T+textend_lim)*1.8+32,(MIN_T-textend_lim)*1.8+32)
    ax12.invert_yaxis()
    ax12.tick_params(labelsize=0.75*size)
#    ax2.legend(prop={'size': 0.75*size})
    ax2.legend()
    ax2.set_ylabel('depth(m)',fontsize=size)
    ax2.set_ylim(MIN_D-dextend_lim,MAX_D+dextend_lim)
    if len(tele_df)==1:   #if there only one data, we need change the time limit as one week
        ax2.set_xlim(tele_df['time'][0]-timedelta(days=3),tele_df['time'][0]+timedelta(days=4))
    ax2.tick_params(labelsize=0.75*size)
    ax22=ax2.twinx()
    ax22.set_ylabel('depth(feet)',fontsize=size)
    ax22.set_ylim((MAX_D+dextend_lim)*3.28084,(MIN_D-dextend_lim)*3.28084)
    ax22.invert_yaxis()
    ax22.tick_params(labelsize=0.75*size)
    
    #check the path is exist,if not exist,create it
    if not os.path.exists(path_picture_save+'/picture/'):
        os.makedirs(path_picture_save+'/picture/')
    plt.savefig(path_picture_save+'/picture/'+name+'_tsp_'+dtime.strftime('%Y-%m-%d')+'.png',dpi=dpi)
    print(name+' finished time series plot!')

def draw_map(df,name,dtime,path_picture_save,timeinterval=30,mindepth=10,dpi=300):
    """
    the type of start_time_local and end time_local is datetime.datetime
    use to draw the location of raw file and telemetered produced"""
    #creat map
    #Create a blank canvas 
    df=check_depth(df.dropna(),mindepth=10) #screen out the data 
    #make sure the start time through the latest time of get data
    if dtime>df['time'][len(df)-1]:
        start_time=df['time'][len(df)-1]-timedelta(days=timeinterval)
    else:
        start_time=dtime-timedelta(days=timeinterval)
    
    df=check_time(df,'time',start_time,dtime) #screen out the valuable data that we need through the time
    if len(df)==0:  #if the length of 
        print(name+': no value data!')
        return 0
    fig=plt.figure(figsize=(8,8.5))
    fig.suptitle('F/V '+name,fontsize=24, fontweight='bold')
    if len(df)>0:
        start_time=df['time'][0]
        end_time=df['time'][len(df)-1]
    if type(start_time)!=str:
        start_time=start_time.strftime('%Y-%m-%d')
        end_time=dtime.strftime('%Y-%m-%d')
    ax=fig.add_axes([0.02,0.02,0.9,0.9])
    ax.set_title(start_time+'-'+end_time)
    ax.axes.title.set_size(16)
    
    min_lat=min(df['lat'])
    max_lat=max(df['lat'])
    max_lon=max(df['lon'])
    min_lon=min(df['lon'])
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
        if len(df)>0:
            tele_lat,tele_lon=to_list(df['lat'],df['lon'])
            tele_x,tele_y=map(tele_lon,tele_lat)
            ax.plot(tele_x,tele_y,'b*',markersize=6,alpha=0.5,label='telemetry')
            ax.legend()
        #if the path of the picture save is not there, creat the folder
        if not os.path.exists(path_picture_save+'/picture/'):
            os.makedirs(path_picture_save+'/picture/')
        plt.savefig(path_picture_save+'/picture/'+name+'_map'+'_'+dtime.strftime('%Y%m%d')+'.png',dpi=dpi) #save picture
        print(name+' finished draw!')
    except:
        print(name+' need redraw!')


def to_list(lat,lon):
    "transfer the format to list"
    x,y=[],[]
    for i in range(len(lat)):
        x.append(lat[i])
        y.append(lon[i])
    return x,y




dictionary_path='/home/jmanning/Desktop/data_dict/dict_obsdpogmf.p'
start_time_str='2018-7-1'
end_time_str='2019-3-21'
picture_save='/home/jmanning/Desktop/qwe/' #use


start_time=datetime.strptime(start_time_str,'%Y-%m-%d')
end_time=datetime.strptime(end_time_str,'%Y-%m-%d')
end_time=datetime.now()
with open(dictionary_path, 'rb') as fp:
    dict= pickle.load(fp)   
tele_dict=dict['tele_dict']
index=tele_dict.keys()
#index=['Sao_Paulo']
#for i in index: #
#    if len(tele_dict[i])==0:
#        continue
#    else: 
#        draw_time_series_plot(dict,name=i,dtime=end_time,path_picture_save=picture_save,dpi=300)
#        draw_map(tele_df=tele_dict[i],name=i,dtime=end_time,path_picture_save=picture_save,dpi=300)


