
"""
Created on Wed Oct  3 12:39:15 2018
update 2/21 2019
update some the draw_time_series_plot fix the label of y axis
@author: leizhao
"""
import conversions as cv
import ftplib
import glob
import matplotlib.pyplot as plt
import os
import pandas as pd
import numpy as np
import sys
import zlconversions as zl
from datetime import datetime,timedelta
from pylab import mean, std
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(os.path.join(conda_dir, 'share'), 'proj')
os.environ["PROJ_LIB"] = proj_lib
from mpl_toolkits.basemap import Basemap
import time
import doppio_modules as dpo
import gomofs_modules as gmf
try:
    import cPickle as pickle
except ImportError:
    import pickle

#HARDCODES

    
def check_reformat_data(input_dir,output_dir,telemetry_status_file,raw_data_name_file,Lowell_SN_2='7a',similarity=0.7,mindepth=10):
    """
    check reformat of data
    check:vessel name,vessel number,serial number, lat,lon
    add VP_NUM
    if the file is test file, continue next file
    """
    #read the file of the vessel_number
    telemetrystatus_df=read_telemetrystatus(telemetry_status_file)
    raw_data_name_df=pd.read_csv(raw_data_name_file,sep='\t') 

    #produce a dataframe that use to caculate the number of items
    total_df=pd.concat([telemetrystatus_df.loc[:,['Boat']][:],pd.DataFrame(data=[['Total']],columns=['Boat'])],ignore_index=True)
    total_df.insert(1,'file_total',0)
    #get all the files under the input folder
    #screen out the file of '.csv',and put the path+name in the fil_lists
    allfile_lists=zl.list_all_files(input_dir)
    file_lists=[]
    for file in allfile_lists:
        if file[len(file)-4:]=='.csv':
            file_lists.append(file)

    #start check the data and save in the output_dir
    for file in file_lists:
        fpath,fname=os.path.split(file)  #get the file's path and name
        #fix the file name
        fname=file.split('/')[len(file.split('/'))-1]
        if len(fname.split('_')[1])==2:# if the serieal number is only 2 digits make it 4
            new_fname=fname[:3]+Lowell_SN_2+fname[3:]
        else:
            new_fname=fname
        # now, read header and data
        try:
            df_head=zl.nrows_len_to(file,2,name=['key','value'])
            df=zl.skip_len_to(file,2) #data
        except:
            print("unvaluable file:"+file)
            continue
        #the standard data have 6 columns, sometimes the data possible lack of the column of the HEADING.If lack, fixed it
        if len(df.iloc[0])==5: # some files didn't have the "DATA" in the first column
            df.insert(0,'HEADING','DATA')
        df.columns = ['HEADING','Datet(GMT)','Lat','Lon','Temperature(C)','Depth(m)']  #rename the name of conlum of data
        
        #check if the data is the test data; Is the vessel number right?(test data's vessel number is 99)
        df['Depth(m)'] = df['Depth(m)'].map(lambda x: '{0:.2f}'.format(float(x)))  #keep two decimal fraction
        df['Temperature(C)'] = df['Temperature(C)'].map(lambda x: '{0:.2f}'.format(float(x)))
        #keep the lat and lon data format is right,such as 00000.0000w to 0000.0000
        df['Lon'] = df['Lon'].map(lambda x: '{0:.4f}'.format(float(format_lat_lon(x))))
        df['Lat'] = df['Lat'].map(lambda x: '{0:.4f}'.format(float(format_lat_lon(x))))#keep four decimal fraction
        datacheck,count=1,0
        for i in range(len(df['Depth(m)'])):  #the value of count is 0 if the data is test data
            count=count+(float(df['Depth(m)'][i])>mindepth)# keep track of # of depths>mindepth
            if count>5:
                if count==i+1:
                    print('please change the file:'+file+' make sure the logger is work well!' )
                    datacheck=0
                break
        if datacheck==0:
            continue
        vessel_name=fpath.split('/')[len(fpath.split('/'))-1:][0] #get the vessel name
        for j in range(len(df_head)):
            if df_head['key'][j].lower()=='Vessel Number'.lower():
                LOC_V_number=j
                #check and fix the vessel number              
                if count!=0:      
                    for i in range(len(telemetrystatus_df)):
                        if telemetrystatus_df['Vessel#'][i]==vessel_name:
                            df_head['value'][j]=str(telemetrystatus_df['Vessel#'][i])
                            break
                        else:
                            continue
                else:   
                    df_head['value'][j]='99' #the value of the vessel number is 99 if the data is test data
                break
    
        if df_head['value'][LOC_V_number]=='99':
            df_head=df_head.replace(vessel_name,'Test')
            print ("test file:"+file)#if the file is test file,print it
            continue
        #check the header file whether exist or right,if not,repair it
        header_file_fixed_key=['Date Format','Time Format','Temperature','Depth'] 
        header_file_fixed_value=['YYYY-MM-DD','HH24:MI:SS','C','m']
        loc=0
        EXIST=0
        for fixed_t in header_file_fixed_key:
            for k in range(len(df_head['key'])):
                if fixed_t.lower()==df_head['key'][k].lower():
                    break
                else:
                    EXIST=1
                    count=k+1
            if EXIST==1:
                df_head=pd.concat([df_head[:count],pd.DataFrame(data=[[fixed_t,header_file_fixed_value[loc]]],columns=['key','value'])],ignore_index=True)
            loc=loc+1 
        #caculate the number of every vessel and boat files
        for i in range(len(total_df['Boat'])):
            if total_df['Boat'][i].lower()==vessel_name.lower():
                total_df['file_total'][i]=total_df['file_total'][i]+1

        #if the vessel name and serial number are exist, find the location of them 
        vessel_name_EXIST=0 
        S_number_EXIST=0  
        for k in range(len(df_head['key'])):           
            if df_head['key'][k].lower()=='Vessel Name'.lower():
                vessel_name_EXIST=1
                df_head['value'][k]=vessel_name
            if df_head['key'][k].lower()=='Serial Number'.lower():
                if len(df_head['value'][k].split(':'))>1:
                    df_head['value'][k]=df_head['value'][k].replace(':','')
                S_number_EXIST=1
        #check and fix the vessel name and serial number 
        if S_number_EXIST==0:
            df_head=pd.concat([df_head[:1],pd.DataFrame(data=[['Serial Number',new_fname.split('_')[1]]],columns=['key','value']),df_head[1:]],ignore_index=True)
        if vessel_name_EXIST==0:#
            df_head=pd.concat([df_head[:2],pd.DataFrame(data=[['Vessel Name',vessel_name]],columns=['key','value']),df_head[2:]],ignore_index=True)
        for i in range(len(df_head['key'])):
            if df_head['key'][i].lower()=='Vessel Number'.lower():
                loc_vp_header=i+1
                break
        for i in range(len(raw_data_name_df['VESSEL_NAME'])):
            ratio=zl.str_similarity_ratio(vessel_name.lower(),raw_data_name_df['VESSEL_NAME'][i].lower())
            ratio_best=0
            if ratio>similarity:
                if ratio>ratio_best:
                    ratio_best=ratio
                    loc_vp_file=i
        df_head=pd.concat([df_head[:loc_vp_header],pd.DataFrame(data=[['VP_NUM',raw_data_name_df['VP_NUM'][loc_vp_file]]],columns=['key','value']),df_head[loc_vp_header:]],ignore_index=True)
        #creat the path and name of the new_file and the temperature file  
        output_path=fpath.replace(input_dir,output_dir)
        if not os.path.exists(output_path):   #check the path of the save file is exist,make it if not
            os.makedirs(output_path)
        df_head.to_csv(output_path+'/'+new_fname,index=0,header=0)
        df.to_csv(output_path+'/df_tem.csv',index=0)  #produce the temperature file  
        #add the two file in one file and delet the temperature file
        os.system('cat '+output_path+'/df_tem.csv'+' >> '+output_path+'/'+new_fname)
        os.remove(output_path+'/df_tem.csv')
#    #caculate the total of all files and print save as a file.
    try:
        for i in range(len(total_df)-1):
            total_df['file_total'][len(total_df)-1]=total_df['file_total'][len(total_df)-1]+total_df['file_total'][i]
        total_df.to_csv(output_dir+'/items_number.txt',index=0)
    except:
        print("no valuable file!")

def classify_by_boat(input_dir,output_dir,telemetry_status_path_name):
    """
    this code used to know which boat get the data
    and put the data file to the right folder
    notice:this code is suitable for matching data after 2000
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if os.listdir(output_dir):
        print ('please input a empty directory!')
        sys.exit()
    #read the file of the telementry_status
    df=read_telemetrystatus(telemetry_status_path_name)

    #fix the format of time about logger_change
    for i in range(len(df)):
        if df['logger_change'].isnull()[i]:
            continue
        else:
            date_logger_change=df['logger_change'][i].split(',')   #get the time data of the logger_change
            for j in range(0,len(date_logger_change)):
                if len(date_logger_change[j])>4:     #keep the date have the month and year such as 1/17
                    date_logger_change[j]=zl.transform_date(date_logger_change[j]) #use the transform_date(date) to fix the date
            df['logger_change'][i]=date_logger_change
            
    #get the path and name of the file that need to match
    file_lists=glob.glob(os.path.join(input_dir,'*.csv'))
    #match the file        
    for file in file_lists:
        #time conversion, GMT time to local time
        time_str=file.split('/')[len(file.split('/'))-1:][0].split('.')[0].split('_')[2]+' '+file.split('/')[len(file.split('/'))-1:][0].split('.')[0].split('_')[3]
        #GMT time to local time of file
        time_local=zl.gmt_to_eastern(time_str[0:4]+'-'+time_str[4:6]+'-'+time_str[6:8]+' '+time_str[9:11]+':'+time_str[11:13]+':'+time_str[13:15]).strftime("%Y%m%d")
        #math the SN and date
        for i in range(len(df['Lowell-SN'])):
            if df['Lowell-SN'].isnull()[i] or df['logger_change'].isnull()[i]:  #we will enter the next line if SN or date is not exist 
                continue
            else:
                for j in range(len(df['Lowell-SN'][i].split(','))):   
                    fname_len_SN=len(file.split('/')[len(file.split('/'))-1:][0].split('_')[1]) #the length of SN in the file name
                    len_SN=len(df['Lowell-SN'][i].split(',')[j]) #the length of SN in the culumn of the Lowell-SN inthe file of the telemetry_status.csv
                    if df['Lowell-SN'][i].split(',')[j][len_SN-fname_len_SN:]==file.split('/')[len(file.split('/'))-1:][0].split('_')[1]:
                        fpath,fname=os.path.split(file)    #seperate the path and name of the file
                        dstfile=(fpath).replace(input_dir,output_dir+'/'+df['Boat'][i]+'/'+fname) #produce the path+filename of the destination
                        dstfile=dstfile.replace('//','/')
                        #copy the file to the destination folder
                        try:
#                            if len(df['Lowell-SN'][i].split(','))<len(df['logger_change'][i]):
                            if j<len(df['logger_change'][i])-1:
                                if df['logger_change'][i][j]<=time_local<=df['logger_change'][i][j+1]:
                                    zl.copyfile(file,dstfile)  
                            else:
                                if df['logger_change'][i][j]<=time_local:
                                    zl.copyfile(file,dstfile) 
                        except:
                            print("please check the data of telemetry status!")

def classify_tele_raw_by_boat(input_dir,path_save,telemetry_status,start_time,end_time,telemetry_path='https://www.nefsc.noaa.gov/drifter/emolt.dat',dpi=300):
    """
    the type of start tiem and end time is str :'%Y-%m-%d'
    match the file and telementy.
    we can known how many file send to the satallite and output the figure
    return a dictionary, in this dictionary include raw dictinary and teledictionary and record_file_df
    """
    #read the file of the telementry_status
    telemetrystatus_df=read_telemetrystatus(telemetry_status)
    #st the record file use to write minmum maxmum and average of depth and temperature,the numbers of file, telemetry and successfully matched
    record_file_df=telemetrystatus_df.loc[:,['Boat','Vessel#']].reindex(columns=['Boat','Vessel#','matched_number','file_number','tele_num','max_diff_depth',\
                                      'min_diff_depth','average_diff_depth','max_diff_temp','min_diff_temp','average_diff_temp','sum_diff_depth','sum_diff_temp',\
                                      'min_lat','max_lat','min_lon','max_lon'],fill_value=None)
    #transfer the time format of string to datetime 
    start_time_local=datetime.strptime(start_time,'%Y-%m-%d')
    end_time_local=datetime.strptime(end_time,'%Y-%m-%d')
    allfile_lists=zl.list_all_files(input_dir)
    ######################
    file_lists=[]
    for file in allfile_lists:
        if file[len(file)-4:]=='.csv' and start_time<=file[len(file)-19:len(file)-11]<=end_time:
            file_lists.append(file)
    #download the data of telementry
    tele_df=read_telemetry(telemetry_path)
    #screen out the data of telemetry in interval
    valuable_tele_df=pd.DataFrame(data=None,columns=['vessel_n','esn','time','lon','lat','depth','temp'])#use to save the data during start time and end time
    for i in range(len(tele_df)):
        tele_time=str(tele_df['year'].iloc[i])+'-'+str(tele_df['month'].iloc[i])+'-'+str(tele_df['day'].iloc[i])+' '+\
                                         str(tele_df['Hours'].iloc[i])+':'+str(tele_df['minates'].iloc[i])+':'+'00'
        if zl.local2utc(start_time_local)<=datetime.strptime(tele_time,'%Y-%m-%d %H:%M:%S')<zl.local2utc(end_time_local):
            valuable_tele_df=valuable_tele_df.append(pd.DataFrame(data=[[tele_df['vessel_n'][i],tele_df['esn'][i],tele_time,tele_df['lon'][i],tele_df['lat'][i],tele_df['depth'][i],\
                                                       tele_df['temp'][i]]],columns=['vessel_n','esn','time','lon','lat','depth','temp']))
    valuable_tele_df.index=range(len(valuable_tele_df))
    #whether the data of file and telemetry is exist
    if len(valuable_tele_df)==0 and len(file_lists)==0:
        print('please check the data website of telementry and the directory of raw_data is exist!')
        #sys.exit()
    elif len(valuable_tele_df)==0:
        print('please check the data website of telementry!')
       # sys.exit()
    elif len(file_lists)==0:
        print('please check the directory raw_data is exist!')
        #sys.exit()
    #match the file
    index=telemetrystatus_df['Boat'] #set the index for dictionary
    raw_dict={}    #the dictinary about raw data, use to write the data about 'time','filename','mean_temp','mean_depth'
    tele_dict={}  #the dictionary about telementry data,use to write the data about'time','mean_temp','mean_depth'
    for i in range(len(index)):  #loop every boat
        raw_dict[index[i]]=pd.DataFrame(data=None,columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon'])
        tele_dict[index[i]]=pd.DataFrame(data=None,columns=['time','mean_temp','mean_depth','mean_lat','mean_lon'])
    for file in file_lists: # loop raw files
        fpath,fname=os.path.split(file)  #get the file's path and name
        # now, read header and data of every file  
        header_df=zl.nrows_len_to(file,2,name=['key','value']) #only header 
        data_df=zl.skip_len_to(file,2) #only data
        #caculate the mean temperature and depth of every file
        value_data_df=data_df.ix[(data_df['Depth(m)']>0.85*mean(data_df['Depth(m)']))]  #filter the data
        value_data_df=value_data_df.ix[2:]   #delay several minutes to let temperature sensor record the real bottom temp
        value_data_df=value_data_df.ix[(value_data_df['Temperature(C)']>mean(value_data_df['Temperature(C)'])-3*std(value_data_df['Temperature(C)'])) & \
                   (value_data_df['Temperature(C)']<mean(value_data_df['Temperature(C)'])+3*std(value_data_df['Temperature(C)']))]  #Excluding gross error
        value_data_df.index = range(len(value_data_df))  #reindex
        for i in range(len(value_data_df['Lat'])):
            value_data_df['Lat'][i],value_data_df['Lon'][i]=cv.dm2dd(value_data_df['Lat'][i],value_data_df['Lon'][i])
        min_lat=min(value_data_df['Lat'].values)
        max_lat=max(value_data_df['Lat'].values)
        min_lon=min(value_data_df['Lon'].values)
        max_lon=max(value_data_df['Lon'].values)
        mean_lat=str(round(mean(value_data_df['Lat'].values),4))
        mean_lon=str(round(mean(value_data_df['Lon'].values),4)) #caculate the mean depth
        mean_temp=str(round(mean(value_data_df['Temperature(C)'][1:len(value_data_df)]),2))
        mean_depth=str(abs(int(round(mean(value_data_df['Depth(m)'].values))))).zfill(3)   #caculate the mean depth
        
        #get the vessel number of every file
        for i in range(len(header_df)):
            if header_df['key'][i].lower()=='vessel number'.lower():
                vessel_number=int(header_df['value'][i])
                break
        #caculate the number of raw files in every vessel,and min,max of lat and lon
        for i in range(len(record_file_df)):
            if record_file_df['Vessel#'][i]==vessel_number:
                if record_file_df['file_number'].isnull()[i]:
                    record_file_df['min_lat'][i]=min_lat
                    record_file_df['max_lat'][i]=max_lat
                    record_file_df['min_lon'][i]=min_lon
                    record_file_df['max_lon'][i]=max_lon
                    record_file_df['file_number'][i]=1
                else:
                    record_file_df['file_number'][i]=int(record_file_df['file_number'][i]+1)
                    if record_file_df['min_lat'][i]>min_lat:
                        record_file_df['min_lat'][i]=min_lat
                    if record_file_df['max_lat'][i]<max_lat:
                        record_file_df['max_lat'][i]=max_lat
                    if record_file_df['min_lon'][i]>min_lon:
                        record_file_df['min_lon'][i]=min_lon
                    if record_file_df['max_lon'][i]<max_lon:
                        record_file_df['max_lon'][i]=max_lon
       
        #match rawdata and telementry data
        time_str=fname.split('.')[0].split('_')[2]+' '+fname.split('.')[0].split('_')[3]
        time_str=time_str[:4]+'-'+time_str[4:6]+'-'+time_str[6:11]+':'+time_str[11:13]+':'+time_str[13:15]
        #write the data of raw file to dict
        for i in range(len(telemetrystatus_df)):
            if telemetrystatus_df['Vessel#'][i]==vessel_number:
                raw_dict[telemetrystatus_df['Boat'][i]]=raw_dict[telemetrystatus_df['Boat'][i]].append(pd.DataFrame(data=[[time_str,\
                                    fname,float(mean_temp),float(mean_depth),float(mean_lat),float(mean_lon)]],\
                                    columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon']).iloc[0],ignore_index=True)                             
    #write 'time','mean_temp','mean_depth' of the telementry to tele_dict             
    for i in valuable_tele_df.index:  #valuable_tele_df is the valuable telemetry data during start time and end time 
        for j in range(len(telemetrystatus_df)):
            if int(valuable_tele_df['vessel_n'][i].split('_')[1])==telemetrystatus_df['Vessel#'][j]:
                #count the numbers by boats
                if record_file_df['tele_num'].isnull()[j]:
                    record_file_df['tele_num'][j]=1
                else:
                    record_file_df['tele_num'][j]=record_file_df['tele_num'][j]+1
                if record_file_df['max_lat'].isnull()[j]:
                    record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                else:
                    if record_file_df['min_lat'][j]>valuable_tele_df['lat'][i]:
                        record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['max_lat'][j]<valuable_tele_df['lat'][i]:
                        record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['min_lon'][j]>valuable_tele_df['lon'][i]:
                        record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    if record_file_df['max_lon'][j]<valuable_tele_df['lon'][i]:
                        record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                #write 'time','mean_temp','mean_depth' of the telementry to tele_dict
                tele_dict[telemetrystatus_df['Boat'][j]]=tele_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=[[valuable_tele_df['time'][i],\
                         float(valuable_tele_df['temp'][i]),float(valuable_tele_df['depth'][i]),float(valuable_tele_df['lat'][i]),float(valuable_tele_df['lon'][i])]],\
                            columns=['time','mean_temp','mean_depth','mean_lat','mean_lon']).iloc[0],ignore_index=True)
    print("finish the calculate of min_lat and min_lon!")
    for i in range(len(record_file_df)):
        if not record_file_df['matched_number'].isnull()[i]:
            record_file_df['average_diff_depth'][i]=round(record_file_df['sum_diff_depth'][i]/record_file_df['matched_number'][i],4)
            record_file_df['average_diff_temp'][i]=round(record_file_df['sum_diff_temp'][i]/record_file_df['matched_number'][i],4)
        else:
            record_file_df['matched_number'][i]=0
        if record_file_df['tele_num'].isnull()[i]:
            record_file_df['tele_num'][i]=0
        if record_file_df['file_number'].isnull()[i]:
            record_file_df['file_number'][i]=0
    for i in index:#loop every boat,  i represent the name of boat
        raw_dict[i]=raw_dict[i].sort_values(by=['time'])
        raw_dict[i].index=range(len(raw_dict[i]))
    record_file_df=record_file_df.drop(['sum_diff_depth','sum_diff_temp'],axis=1)
    #save the record file
    record_file_df.to_csv(path_save+'/'+start_time+'_'+end_time+' statistics.csv',index=0) 
    dict={}
    dict['raw_dict']=raw_dict
    dict['tele_dict']=tele_dict
    dict['record_file_df']=record_file_df
    return  dict

def download(save_path,start_time=datetime.strptime('2000-1-1','%Y-%m-%d'),end_time=datetime.now()):
    """download the rawdata from web"""
    ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
    print ('Logging in.')
    ftp.cwd('/Matdata')
    print ('Accessing files')
    filenames = ftp.nlst() # get filenames within the directory OF REMOTE MACHINE
    start_time_gmt=zl.local2utc(start_time)  #time tranlate from local to UTC
    end_time_gmt=zl.local2utc(end_time)
    # MAKE THIS A LIST OF FILENAMES THAT WE NEED DOWNLOAD
    download_files=[]
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    for file in filenames:
        if len(file.split('_'))==4:
            if start_time_gmt<=datetime.strptime(file.split('_')[2]+file.split('_')[3].split('.')[0],'%Y%m%d%H%M%S')<end_time_gmt:
                download_files.append(file)
    for filename in download_files: # DOWNLOAD FILES   
        local_filename = os.path.join(save_path, filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR '+ filename, file.write)
        file.close()
    ftp.quit() # This is the “polite” way to close a connection
    print ('New files downloaded')


 
def draw_time_series_plot(raw_dict,tele_dict,name,start_time_local,end_time_local,path_picture_save,record_file,dpi=300):
    """use to draw the time series plot"""
    
    fig=plt.figure(figsize=(7,4))
    fig.suptitle(name+'\n'+'matches:'+str(int(record_file['matched_number']))+'   telemetered:'+\
                         str(int(record_file['tele_num']))+'   raw_data_uploaded:'+str(int(record_file['file_number'])),fontsize=8, fontweight='bold')
    ax2=fig.add_subplot(212)
#    fig.autofmt_xdate(bottom=0.18)
    fig.subplots_adjust(left=0.10)
    ax1=fig.add_subplot(211)    
    if len(raw_dict)>0 and len(tele_dict)>0:
        tele_dict['mean_depth']=-1*tele_dict['mean_depth']
        raw_dict['mean_depth']=-1*raw_dict['mean_depth']
        ax1.plot_date(raw_dict['time'],raw_dict['mean_temp'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
        ax1.plot_date(tele_dict['time'],tele_dict['mean_temp'],linestyle='-',alpha=0.5,label='telemetry',marker='^')
        if record_file['matched_number']!=0:
            ax1.set_title('temperature differences of min:'+str(round(record_file['min_diff_temp'],2))+'  max:'+str(round(record_file['max_diff_temp'],2))+\
                          '  average:'+str(round(record_file['average_diff_temp'],3)))
        ax2.plot_date(raw_dict['time'],raw_dict['mean_depth'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
        ax2.plot_date(tele_dict['time'],tele_dict['mean_depth'],linestyle='-',alpha=0.5,label='telemetry',marker='^')
        if record_file['matched_number']!=0:
            ax2.set_title('depth differences of min:'+str(round(record_file['min_diff_depth'],2))+'  max:'+str(round(record_file['max_diff_depth'],2))+\
                          '  average:'+str(round(record_file['average_diff_depth'],3)))
        max_temp=max(np.nanmax(raw_dict['mean_temp'].values),np.nanmax(tele_dict['mean_temp'].values))
        min_temp=min(np.nanmin(raw_dict['mean_temp'].values),np.nanmin(tele_dict['mean_temp'].values))
        max_depth=max(np.nanmax(raw_dict['mean_depth'].values),np.nanmax(tele_dict['mean_depth'].values))
        min_depth=min(np.nanmin(raw_dict['mean_depth'].values),np.nanmin(tele_dict['mean_depth'].values))
    else:
        labels='raw_data'
        markers='d'
        if len(tele_dict)>0:
            raw_dict=tele_dict
            labels='telemetered'
            markers='^'  
        raw_dict['mean_depth']=-1*raw_dict['mean_depth']
        ax2.plot_date(raw_dict['time'],raw_dict['mean_depth'],linestyle='-',alpha=0.5,label=labels,marker=markers)
        ax1.plot_date(raw_dict['time'],raw_dict['mean_temp'],linestyle='-',alpha=0.5,label=labels,marker=markers)    
        max_temp=np.nanmax(raw_dict['mean_temp'].values)
        min_temp=np.nanmin(raw_dict['mean_temp'].values)
        max_depth=np.nanmax(raw_dict['mean_depth'].values)
        min_depth=np.nanmin(raw_dict['mean_depth'].values)
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
    ax2.legend()
    ax2.set_ylabel('depth(m)',fontsize=8)
    ax2.set_ylim(min_depth-dextend_lim,max_depth+dextend_lim)
    ax2.axes.title.set_size(8)
    ax2.set_xlim(start_time_local,end_time_local)
    ax2.tick_params(labelsize=6)
    ax22=ax2.twinx()
    ax22.set_ylabel('depth(feet)',fontsize=8)
    ax22.set_ylim((max_depth+dextend_lim)*3.28084,(min_depth-dextend_lim)*3.28084)
    ax22.invert_yaxis()
    ax22.tick_params(labelsize=6)
    ax1.legend()
    ax1.set_ylabel('Celius',fontsize=8)  
    ax1.set_ylim(min_temp-textend_lim,max_temp+textend_lim)
    ax1.axes.title.set_size(8)
    ax1.set_xlim(start_time_local,end_time_local)
    ax1.axes.get_xaxis().set_visible(False)
    ax1.tick_params(labelsize=6)
    ax12=ax1.twinx()
    ax12.set_ylabel('Fahrenheit',fontsize=10)
    #conversing the Celius to Fahrenheit
    ax12.set_ylim((max_temp+textend_lim)*1.8+32,(min_temp-textend_lim)*1.8+32)
    ax12.invert_yaxis()
    ax12.tick_params(labelsize=6)
    
    if not os.path.exists(path_picture_save+'/picture/'+name+'/'):
        os.makedirs(path_picture_save+'/picture/'+name+'/')
    plt.savefig(path_picture_save+'/picture/'+name+'/'+start_time_local.strftime('%Y-%m-%d')+'_'+end_time_local.strftime('%Y-%m-%d')+'.png',dpi=dpi)
 
def draw_time_series_plot3(raw_dict,tele_dict,name,start_time,end_time,path_picture_save,record_file,dpi=300,mindepth=10):  
    """
    if don't want to report the record file, let the record_file=False
    use to draw time series plot """
    for i in range(len(raw_dict)):
        if type(raw_dict['time'][i])==str:
            raw_dict['time'][i]=datetime.strptime(raw_dict['time'][i],'%Y-%m-%d %H:%M:%S')
        if start_time<=raw_dict['time'][i]<=end_time:
            continue
        else:
            raw_dict=raw_dict.drop(i)
    raw_dict.index=range(len(raw_dict))
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
            if abs(tele_dict['mean_depth'][i])<abs(mindepth):
                tele_dict=tele_dict.drop(i)
        tele_dict.index=range(len(tele_dict)) 
    if len(raw_dict)>0:
        if raw_dict['mean_depth'][0]>0:    
            raw_dict[['mean_depth','doppio_depth','gomofs_depth']]=-1*raw_dict[['mean_depth','doppio_depth','gomofs_depth']] 
    if len(tele_dict)>0:
        if tele_dict['mean_depth'][0]>0:
            tele_dict[['mean_depth','doppio_depth','gomofs_depth']]=-1*tele_dict[['mean_depth','doppio_depth','gomofs_depth']]
    fig=plt.figure(figsize=(14,14))
    size=min(fig.get_size_inches())        
    try:
        fig.suptitle(name+'\n'+'matches:'+str(int(record_file['matched_number']))+'   telemetered:'+\
                         str(int(record_file['tele_num']))+'   raw_data_uploaded:'+str(int(record_file['file_number'])),fontsize=2*min(fig.get_size_inches()), fontweight='bold')
    except:
        fig.suptitle(name,fontsize=2*size, fontweight='bold')
    if len(raw_dict)>0 and len(tele_dict)>0:
        ax1=fig.add_axes([0.1, 0.70, 0.8,0.16])
        ax2=fig.add_axes([0.1, 0.52, 0.8,0.16])
        ax3=fig.add_axes([0.1, 0.28, 0.8,0.16])
        ax4=fig.add_axes([0.1, 0.1, 0.8,0.16])
        time_series_plot(raw_dict,ax1,ax2,start_time,end_time,size,dictlabel='Raw_data')
        time_series_plot(tele_dict,ax3,ax4,start_time,end_time,size,dictlabel='Telemetered')
    elif len(raw_dict)==0 and len(tele_dict)==0:
        print(name+': no data of telemetered and raw_data')
    else:
        fig=plt.figure(figsize=(14,8))
        size=max(fig.get_size_inches())
        fig.suptitle(name,fontsize=2*size, fontweight='bold')
        ax1=fig.add_axes([0.1, 0.5, 0.8,0.32])
        ax2=fig.add_axes([0.1, 0.1, 0.8,0.32])
        if len(raw_dict)>0:
            time_series_plot(raw_dict,ax1,ax2,start_time,end_time,size,dictlabel='Raw_data')
        else:
            time_series_plot(tele_dict,ax1,ax2,start_time,end_time,size,dictlabel='Telemetered')
    
    if not os.path.exists(path_picture_save+'/picture/'+name+'/'):
        os.makedirs(path_picture_save+'/picture/'+name+'/')
    plt.savefig(path_picture_save+'/picture/'+name+'/'+start_time.strftime('%Y-%m-%d %H:%M:%S')+'_'+end_time.strftime('%Y-%m-%d %H:%M:%S')+'test.png',dpi=dpi)


def draw_map(raw_dict,tele_dict,name,start_time_local,end_time_local,path_picture_save,record_file,dpi=300):
    """
    the type of start_time_local and end time_local is datetime.datetime
    use to draw the location of raw file and telemetered produced"""
    #creat map
    #Create a blank canvas  
    fig=plt.figure(figsize=(8,8.5))
    fig.suptitle('F/V '+name,fontsize=24, fontweight='bold')
    if len(raw_dict)>0 and len(tele_dict)>0:
        start_time=min(raw_dict['time'][0],tele_dict['time'][0])
        end_time=max(raw_dict['time'][len(raw_dict)-1],tele_dict['time'][len(tele_dict)-1])
    elif len(raw_dict)>0:
        start_time=raw_dict['time'][0]
        end_time=raw_dict['time'][len(raw_dict)-1]
    else:
        start_time=tele_dict['time'][0]
        end_time=tele_dict['time'][len(tele_dict)-1]
    if type(start_time)!=str:
        start_time=start_time.strftime('%Y-%m-%d')
        end_time=end_time.strftime('%Y-%m-%d')
    ax=fig.add_axes([0.02,0.02,0.9,0.9])
    ax.set_title(start_time+'-'+end_time)
    ax.axes.title.set_size(16)
    
    min_lat=record_file['min_lat']
    max_lat=record_file['max_lat']
    max_lon=record_file['max_lon']
    min_lon=record_file['min_lon']
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
                resolution='f',lat_0=(record_file['min_lat']+record_file['max_lat'])/2.0,lon_0=(record_file['max_lon']+record_file['min_lon'])/2.0,epsg = 4269)
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
        if len(raw_dict)>0 and len(raw_dict)>0:
            raw_lat,raw_lon=to_list(raw_dict['mean_lat'],raw_dict['mean_lon'])
            raw_x,raw_y=map(raw_lon,raw_lat)
            ax.plot(raw_x,raw_y,'ro',markersize=6,alpha=0.5,label='raw_data')
            tele_lat,tele_lon=to_list(tele_dict['mean_lat'],tele_dict['mean_lon'])
            tele_x,tele_y=map(tele_lon,tele_lat)
            ax.plot(tele_x,tele_y,'b*',markersize=6,alpha=0.5,label='telemetry')
            ax.legend()
        else:
            if len(raw_dict)>0:
                raw_lat,raw_lon=to_list(raw_dict['mean_lat'],raw_dict['mean_lon'])
                raw_x,raw_y=map(raw_lon,raw_lat)
                ax.plot(raw_x,raw_y,'ro',markersize=6,alpha=0.5,label='raw_data')
                ax.legend()
            else:
                tele_lat,tele_lon=to_list(tele_dict['mean_lat'],tele_dict['mean_lon'])
                tele_x,tele_y=map(tele_lon,tele_lat)
                ax.plot(tele_x,tele_y,'b*',markersize=6,alpha=0.5,label='telemetry')
                ax.legend()
    
        if not os.path.exists(path_picture_save+'/picture/'+name+'/'):
            os.makedirs(path_picture_save+'/picture/'+name+'/')
        plt.savefig(path_picture_save+'/picture/'+name+'/'+'location'+'_'+start_time_local.strftime('%Y-%m-%d')+'_'+end_time_local.strftime('%Y-%m-%d')+'.png',dpi=dpi)
        print(name+' finished draw!')
    except:
        print(name+' need redraw!')
 
def format_lat_lon(data):
    """fix the data of lat and lon"""
    if len(str(data).split('.')[0])>4 or 'A'<=str(data).split('.')[1][len(str(data).split('.')[1])-1:]<='Z':
        data=str(data).split('.')[0][len(str(data).split('.')[0])-4:]+'.'+str(data).split('.')[1][:4]
    return data

#def insert_doppio_gomofs_data(dict,index,dictionary_path='/home/jmanning/Desktop/test/dgdata.p',delta=20,diff_distance=2):
def insert_doppio_gomofs_data(dict,dictionary_path='/home/jmanning/Desktop/test/dgdata.p',delta=20,diff_distance=2,module_index=['doppio','gomofs']):
    '''
    input a dict, according the lat, lon, time in dict to insert values frome doppio and gomofs
    '''
    index=dict.keys()
    if not os.path.exists(dictionary_path):
        modules_data={}
        for i in index:
            modules_data[str(i)]={}
            for j in module_index:
                modules_data[str(i)][j]={}
    else:
        #pickle load
        with open(dictionary_path, 'rb') as fp:
            modules_data = pickle.load(fp)
    for i in index:
        i=str(i)
        dict[i].insert(0,'doppio_temp',np.nan)
        dict[i].insert(0,'gomofs_temp',np.nan)
        dict[i].insert(0,'doppio_depth',np.nan)
        dict[i].insert(0,'gomofs_depth',np.nan)
        
        if len(dict[i])==0:
            continue
        if i not in modules_data:
            modules_data[i]={}
        for j in module_index:  #
            if j not in modules_data[i]:
                modules_data[i][j]={}
        for k in range(len(dict[i]['doppio_temp'])):
            if type(dict[i]['time'][k])==str:
                utc_time=datetime.strptime(dict[i]['time'][k],'%Y-%m-%d %H:%M:%S')
                utc_time_str=utc_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                utc_time=dict[i]['time'][k]
                utc_time_str=utc_time.strftime('%Y-%m-%d %H:%M:%S')
            #the part of caculate the doppio temperature and depth
            if utc_time_str[:7] not in modules_data[i]['doppio']:
                modules_data[i]['doppio'][utc_time_str[:7]]=pd.DataFrame(data=None,columns=['time','lat','lon','temperature','depth'])
            recalculate=0
            try:
                for l in range(len(modules_data[i]['doppio'][utc_time_str[:7]])):
                    data=modules_data[i]['doppio'][utc_time_str[:7]][l]    
                    if abs(utc_time-datetime.strptime(data['time'],'%Y-%m-%d %H:%M:%S'))<timedelta(minutes=delta):
                        if zl.dist(lat1=data['lat'],lon1=data['lon'],lat2=dict[i]['mean_lat'][k],lon2=dict[i]['mean_lon'][k])<=diff_distance:
                            dpo_temp,dpo_depth=data['temperature'],data['depth']
                            recalculate=1
            except:
                recalculate=0
                
            if recalculate==0:
                try:
                    dpo_temp,dpo_depth=dpo.get_doppio(lat=dict[i]['mean_lat'][k],lon=dict[i]['mean_lon'][k],depth='bottom',time=utc_time_str,fortype='tempdepth')
                    dpoarow=pd.DataFrame(data=[[utc_time_str,dict[i]['mean_lat'][k],dict[i]['mean_lon'][k],dpo_temp,dpo_depth]],columns=['time','lat','lon','temperature','depth']) 
                    modules_data[i]['doppio'][utc_time_str[:7]]= modules_data[i]['doppio'][utc_time_str[:7]].append(dpoarow)
                except:
                    dpo_temp,dpo_depth=np.nan,np.nan
             
            #the part of caculate the gomofs temperature and depth
            if utc_time_str[:7] not in modules_data[i]['gomofs']:
                modules_data[i]['gomofs'][utc_time_str[:7]]=pd.DataFrame(data=None,columns=['time','lat','lon','temperature','depth'])
            recalculate=0
            try:
                for l in range(len(modules_data[i]['gomofs'][utc_time_str[:7]])):
                    data=modules_data[i]['gomofs'][utc_time_str[:7]][l]    
                    if abs(utc_time-datetime.strptime(data['time'],'%Y-%m-%d %H:%M:%S'))<timedelta(minutes=delta):
                        if zl.dist(lat1=data['lat'],lon1=data['lon'],lat2=dict[i]['mean_lat'][k],lon2=dict[i]['mean_lon'][k])<=diff_distance:
                            gmf_temp,gmf_depth=data['temperature'],data['depth']
                            recalculate=1
            except:
                recalculate=0
            if recalculate==0:
                try:
                    gmf_temp,gmf_depth=gmf.get_gomofs(lat=dict[i]['mean_lat'][k],lon=dict[i]['mean_lon'][k],depth='bottom',date_time=utc_time,fortype='tempdepth')
                    dpoarow=pd.DataFrame(data=[[utc_time_str,dict[i]['mean_lat'][k],dict[i]['mean_lon'][k],gmf_temp,gmf_depth]],columns=['time','lat','lon','temperature','depth']) 
                    modules_data[i]['gomofs'][utc_time_str[:7]]=modules_data[i]['doppio'][utc_time_str[:7]].append(dpoarow)
                except:
                    gmf_temp,gmf_depth=np.nan,np.nan
            if dpo_temp!=np.nan and dpo_depth!=np.nan:
                dict[i]['doppio_temp'][k]=dpo_temp
                dict[i]['doppio_depth'][k]=dpo_depth
            if gmf_temp!=np.nan and gmf_depth!=np.nan:
                dict[i]['gomofs_temp'][k]=gmf_temp
                dict[i]['gomofs_depth'][k]=gmf_depth  
    with open(dictionary_path,'wb') as fp:
        pickle.dump(modules_data,fp,protocol=pickle.HIGHEST_PROTOCOL)
    return dict
            

def match_tele_raw(input_dir,path_save,telemetry_status,start_time,end_time,telemetry_path='https://www.nefsc.noaa.gov/drifter/emolt.dat',\
                   accept_minutes_diff=20,acceptable_distance_diff=2,dpi=300):
    """
    match the file and telementy.
    we can known how many file send to the satallite and output the figure
    the type of start time and end time is datetime.datetime
    """
    conversion_factor = 0.62137119   #1km=0.62137119mile
    diff_degree=3*acceptable_distance_diff/(11.132*conversion_factor)*0.01     #the the difference of degree is 0.01, the distance around about 11.132km  
    #read the file of the telementry_status
    telemetrystatus_df=read_telemetrystatus(telemetry_status)
    #st the record file use to write minmum maxmum and average of depth and temperature,the numbers of file, telemetry and successfully matched
    record_file_df=telemetrystatus_df.loc[:,['Boat','Vessel#']].reindex(columns=['Boat','Vessel#','matched_number','file_number','tele_num','max_diff_depth',\
                                      'min_diff_depth','average_diff_depth','max_diff_temp','min_diff_temp','average_diff_temp','sum_diff_depth','sum_diff_temp',\
                                      'min_lat','max_lat','min_lon','max_lon'],fill_value=None)
    #transfer the time format of string to datetime 
    allfile_lists=zl.list_all_files(input_dir)
    ######################
    file_lists=[]
    for file in allfile_lists:
        if file[len(file)-4:]=='.csv' and start_time<=file[len(file)-19:len(file)-11]<=end_time:
            file_lists.append(file)
    #download the data of telementry
    tele_df=read_telemetry(telemetry_path)
    #screen out the data of telemetry in interval
    valuable_tele_df=pd.DataFrame(data=None,columns=['vessel_n','esn','time','lon','lat','depth','temp'])#use to save the data during start time and end time
    for i in range(len(tele_df)):
        tele_time=str(tele_df['year'].iloc[i])+'-'+str(tele_df['month'].iloc[i])+'-'+str(tele_df['day'].iloc[i])+' '+\
                                         str(tele_df['Hours'].iloc[i])+':'+str(tele_df['minates'].iloc[i])+':'+'00'
        if start_time<=tele_time<end_time:
            valuable_tele_df=valuable_tele_df.append(pd.DataFrame(data=[[tele_df['vessel_n'][i],tele_df['esn'][i],tele_time,tele_df['lon'][i],tele_df['lat'][i],tele_df['depth'][i],\
                                                       tele_df['temp'][i]]],columns=['vessel_n','esn','time','lon','lat','depth','temp']))
    valuable_tele_df.index=range(len(valuable_tele_df))
    #whether the data of file and telemetry is exist
    if len(valuable_tele_df)==0 and len(file_lists)==0:
        print('please check the data website of telementry and the directory of raw_data is exist!')
        sys.exit()
    elif len(valuable_tele_df)==0:
        print('please check the data website of telementry!')
        sys.exit()
    elif len(file_lists)==0:
        print('please check the directory raw_data is exist!')
        sys.exit()
    #match the file
    index=telemetrystatus_df['Boat'] #set the index for dictionary
    raw_dict={}    #the dictinary about raw data, use to write the data about 'time','filename','mean_temp','mean_depth'
    tele_dict={}  #the dictionary about telementry data,use to write the data about'time','mean_temp','mean_depth'
    matched_dict={}
    for i in range(len(index)):  #loop every boat
        raw_dict[index[i]]=pd.DataFrame(data=None,columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon'])
        matched_dict[index[i]]=pd.DataFrame(data=None,columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon'])
        tele_dict[index[i]]=pd.DataFrame(data=None,columns=['time','mean_temp','mean_depth','mean_lat','mean_lon'])
    for file in file_lists: # loop raw files
        fpath,fname=os.path.split(file)  #get the file's path and name
        # now, read header and data of every file  
        header_df=zl.nrows_len_to(file,2,name=['key','value']) #only header 
        data_df=zl.skip_len_to(file,2) #only data
        #caculate the mean temperature and depth of every file
        value_data_df=data_df.ix[(data_df['Depth(m)']>0.85*mean(data_df['Depth(m)']))]  #filter the data
        value_data_df=value_data_df.ix[2:]   #delay several minutes to let temperature sensor record the real bottom temp
        value_data_df=value_data_df.ix[(value_data_df['Temperature(C)']>mean(value_data_df['Temperature(C)'])-3*std(value_data_df['Temperature(C)'])) & \
                   (value_data_df['Temperature(C)']<mean(value_data_df['Temperature(C)'])+3*std(value_data_df['Temperature(C)']))]  #Excluding gross error
        value_data_df.index = range(len(value_data_df))  #reindex
        for i in range(len(value_data_df['Lat'])):
            value_data_df['Lat'][i],value_data_df['Lon'][i]=cv.dm2dd(value_data_df['Lat'][i],value_data_df['Lon'][i])
        min_lat=min(value_data_df['Lat'].values)
        max_lat=max(value_data_df['Lat'].values)
        min_lon=min(value_data_df['Lon'].values)
        max_lon=max(value_data_df['Lon'].values)
        mean_lat=str(round(mean(value_data_df['Lat'].values),4))
        mean_lon=str(round(mean(value_data_df['Lon'].values),4)) #caculate the mean depth
        mean_temp=str(round(mean(value_data_df['Temperature(C)'][1:len(value_data_df)]),2))
        mean_depth=str(abs(int(round(mean(value_data_df['Depth(m)'].values))))).zfill(3)   #caculate the mean depth
        #get the vessel number of every file
        for i in range(len(header_df)):
            if header_df['key'][i].lower()=='vessel number'.lower():
                vessel_number=int(header_df['value'][i])
                break
        #caculate the number of raw files in every vessel,and min,max of lat and lon
        for i in range(len(record_file_df)):
            if record_file_df['Vessel#'][i]==vessel_number:
                if record_file_df['file_number'].isnull()[i]:
                    record_file_df['min_lat'][i]=min_lat
                    record_file_df['max_lat'][i]=max_lat
                    record_file_df['min_lon'][i]=min_lon
                    record_file_df['max_lon'][i]=max_lon
                    record_file_df['file_number'][i]=1
                else:
                    record_file_df['file_number'][i]=int(record_file_df['file_number'][i]+1)
                    if record_file_df['min_lat'][i]>min_lat:
                        record_file_df['min_lat'][i]=min_lat
                    if record_file_df['max_lat'][i]<max_lat:
                        record_file_df['max_lat'][i]=max_lat
                    if record_file_df['min_lon'][i]>min_lon:
                        record_file_df['min_lon'][i]=min_lon
                    if record_file_df['max_lon'][i]<max_lon:
                        record_file_df['max_lon'][i]=max_lon
       
        #match rawdata and telementry data
        time_str=fname.split('.')[0].split('_')[2]+' '+fname.split('.')[0].split('_')[3]
        time_str=time_str[:4]+'-'+time_str[4:6]+'-'+time_str[6:11]+':'+time_str[11:13]+':'+time_str[13:15]
        #GMT time to local time of file
#        time_local=zl.gmt_to_eastern(time_str[0:4]+'-'+time_str[4:6]+'-'+time_str[6:8]+' '+time_str[9:11]+':'+time_str[11:13]+':'+time_str[13:15]) 
        time_gmt=datetime.strptime(time_str,"%Y-%m-%d %H:%M:%S")
        #transfer the format latitude and longitude
        lat,lon=value_data_df['Lat'][len(value_data_df)-1],value_data_df['Lon'][len(value_data_df)-1]
        #caculate the numbers of successful matchs and the minimum,maximum and average different of temperature and depth, and write this data to record file
        raw_save=0
        for i in valuable_tele_df.index:
            if valuable_tele_df['vessel_n'][i].split('_')[1]==str(vessel_number):     
                if abs(datetime.strptime(valuable_tele_df['time'][i],"%Y-%m-%d %H:%M:%S")-time_gmt)<=timedelta(minutes=accept_minutes_diff):  #time match
                    if abs(float(valuable_tele_df['lat'][i])-lat)>diff_degree or abs(float(valuable_tele_df['lon'][i])-lon)>diff_degree:
                        continue
                    if zl.dist(lat1=lat,lon1=lon,lat2=float(valuable_tele_df['lat'][i]),lon2=float(valuable_tele_df['lon'][i]))<=acceptable_distance_diff:  #distance match               
                        for j in range(len(record_file_df)):
                            if record_file_df['Vessel#'][j]==vessel_number:
                                
                                matched_dict[telemetrystatus_df['Boat'][j]]=raw_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=[[time_str,\
                                    fname,float(mean_temp),float(mean_depth),float(mean_lat),float(mean_lon)]],\
                                    columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon']).iloc[0],ignore_index=True) 
                                raw_save=1
                                
                                diff_temp=round((float(mean_temp)-float(valuable_tele_df['temp'][i])),4)
                                diff_depth=round((float(mean_depth)-float(valuable_tele_df['depth'][i])),4)
                                valuable_tele_df=valuable_tele_df.drop([i])
                                if record_file_df['matched_number'].isnull()[j]:
                                    record_file_df['matched_number'][j]=1
                                    record_file_df['sum_diff_temp'][j]=diff_temp
                                    record_file_df['max_diff_temp'][j]=diff_temp
                                    record_file_df['min_diff_temp'][j]=diff_temp
                                    record_file_df['sum_diff_depth'][j]=diff_depth
                                    record_file_df['max_diff_depth'][j]=diff_depth
                                    record_file_df['min_diff_depth'][j]=diff_depth
                                    break
                                else:
                                    record_file_df['matched_number'][j]=int(record_file_df['matched_number'][j]+1)
                                    record_file_df['sum_diff_temp'][j]=record_file_df['sum_diff_temp'][j]+diff_temp
                                    record_file_df['sum_diff_depth'][j]=record_file_df['sum_diff_depth'][j]+diff_depth
                                    if record_file_df['max_diff_temp'][j]<diff_temp:
                                        record_file_df['max_diff_temp'][j]=diff_temp
                                    if record_file_df['min_diff_temp'][j]>diff_temp:
                                        record_file_df['min_diff_temp'][j]=diff_temp
                                    if record_file_df['max_diff_depth'][j]<diff_depth:
                                        record_file_df['max_diff_depth'][j]=diff_depth
                                    if record_file_df['min_diff_depth'][j]>diff_depth:
                                        record_file_df['min_diff_depth'][j]=diff_depth
                                    break
        #write the data of raw file to dict
        if raw_save==0:
            for i in range(len(telemetrystatus_df)):
                if telemetrystatus_df['Vessel#'][i]==vessel_number:
                    raw_dict[telemetrystatus_df['Boat'][i]]=raw_dict[telemetrystatus_df['Boat'][i]].append(pd.DataFrame(data=[[time_str,\
                                    fname,float(mean_temp),float(mean_depth),float(mean_lat),float(mean_lon)]],\
                                    columns=['time','filename','mean_temp','mean_depth','mean_lat','mean_lon']).iloc[0],ignore_index=True) 
                                    
    #write 'time','mean_temp','mean_depth' of the telementry to tele_dict             
    for i in valuable_tele_df.index:  #valuable_tele_df is the valuable telemetry data during start time and end time 
        for j in range(len(telemetrystatus_df)):
            if int(valuable_tele_df['vessel_n'][i].split('_')[1])==telemetrystatus_df['Vessel#'][j]:
                  #count the numbers by boats
                if record_file_df['tele_num'].isnull()[j]:
                    record_file_df['tele_num'][j]=1
                else:
                    record_file_df['tele_num'][j]=record_file_df['tele_num'][j]+1
                if record_file_df['max_lat'].isnull()[j]:
                    record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                else:
                    if record_file_df['min_lat'][j]>valuable_tele_df['lat'][i]:
                        record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['max_lat'][j]<valuable_tele_df['lat'][i]:
                        record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['min_lon'][j]>valuable_tele_df['lon'][i]:
                        record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    if record_file_df['max_lon'][j]<valuable_tele_df['lon'][i]:
                        record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                #write 'time','mean_temp','mean_depth' of the telementry to tele_dict
                tele_dict[telemetrystatus_df['Boat'][j]]=tele_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=[[valuable_tele_df['time'][i],\
                         float(valuable_tele_df['temp'][i]),float(valuable_tele_df['depth'][i]),float(valuable_tele_df['lat'][i]),float(valuable_tele_df['lon'][i])]],\
                            columns=['time','mean_temp','mean_depth','mean_lat','mean_lon']).iloc[0],ignore_index=True)
    print("finish the calculate of min_lat and min_lon!")
    for i in range(len(record_file_df)):
        if not record_file_df['matched_number'].isnull()[i]:
            record_file_df['average_diff_depth'][i]=round(record_file_df['sum_diff_depth'][i]/record_file_df['matched_number'][i],4)
            record_file_df['average_diff_temp'][i]=round(record_file_df['sum_diff_temp'][i]/record_file_df['matched_number'][i],4)
        else:
            record_file_df['matched_number'][i]=0
        if record_file_df['tele_num'].isnull()[i]:
            record_file_df['tele_num'][i]=0
        if record_file_df['file_number'].isnull()[i]:
            record_file_df['file_number'][i]=0

    for i in index:#loop every boat,  i represent the name of boat
        raw_dict[i]=raw_dict[i].sort_values(by=['time'])
        raw_dict[i].index=range(len(raw_dict[i]))
    record_file_df=record_file_df.drop(['sum_diff_depth','sum_diff_temp'],axis=1)
    #save the record file
    try:
        record_file_df.to_csv(path_save+'/'+start_time.strftime('%Y-%m-%d')+'_'+end_time.strftime('%Y-%m-%d')+' statistics.csv',index=0) 
#    return raw_dict,tele_dict,record_file_df,index,start_time_local,end_time_local,path_save
    except:
        print('test.match_tele_raw 1179')
    dict={}
    dict['matched_dict']=matched_dict
    dict['raw_dict']=raw_dict
    dict['tele_dict']=tele_dict
    dict['record_file_df']=record_file_df
    return dict

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

def read_telemetry(path):
    """read the telemetered data and fix a standard format, the return the standard data"""
    tele_df=pd.read_csv(path,sep='\s+',names=['vessel_n','esn','month','day','Hours','minates','fracyrday',\
                                          'lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])
    return tele_df
def time_series_plot(dict,ax1,ax2,start_time,end_time,size,dictlabel='telemetered',double=False,name='Default'):
    """use in draw time plot series function""" 
    if double==True:
        fig=plt.figure(figsize=(size,0.625*size))
        fig.suptitle(name,fontsize=3*size, fontweight='bold')
        size=min(fig.get_size_inches())
        ax1=fig.add_axes([0.1, 0.1, 0.8,0.33])
        ax2=fig.add_axes([0.1, 0.50, 0.8,0.33])
    ax1.plot_date(dict['time'],dict['mean_temp'],linestyle='-',color='b',alpha=0.5,label='OBS',marker='d',markerfacecolor='b')
    ax2.plot_date(dict['time'],dict['mean_depth'],linestyle='-',color='b',alpha=0.5,label='OBS',marker='d',markerfacecolor='b')
    max_t,min_t=np.nanmax(dict['mean_temp'].values),np.nanmin(dict['mean_temp'].values)
    max_d,min_d=np.nanmax(dict['mean_depth'].values),np.nanmin(dict['mean_depth'].values)

    try:
        doppio_dict=dict[['time','doppio_temp','doppio_depth']]
        doppio_dict=doppio_dict.dropna()
        if len(doppio_dict)>0:
            ax1.plot_date(doppio_dict['time'],doppio_dict['doppio_temp'],linestyle='--',color='y',alpha=0.5,label='DOPPIO',marker='d',markerfacecolor='y')
            ax2.plot_date(doppio_dict['time'],doppio_dict['doppio_depth'],linestyle='--',color='y',alpha=0.5,label='DOPPIO',marker='d',markerfacecolor='y')
        doppiomax_t,doppiomin_t=np.nanmax(doppio_dict['doppio_temp'].values),np.nanmin(doppio_dict['doppio_temp'].values)
        doppiomax_d,doppiomin_d=np.nanmax(doppio_dict['doppio_depth'].values),np.nanmin(doppio_dict['doppio_depth'].values) 
    except:
        doppiomax_t,doppiomin_t,doppiomax_d,doppiomin_d=-9999,9999,-99999,99999
    try: 
        gomofs_dict=dict[['time','gomofs_temp','gomofs_depth']]
        gomofs_dict=gomofs_dict.dropna()   
        if len(gomofs_dict)>0:         
            ax1.plot_date(gomofs_dict['time'],gomofs_dict['gomofs_temp'],linestyle='-.',color='r',alpha=0.5,label='GOMOFS',marker='d',markerfacecolor='r')
            ax2.plot_date(gomofs_dict['time'],gomofs_dict['gomofs_depth'],linestyle='-.',color='r',alpha=0.5,label='GOMOFS',marker='d',markerfacecolor='r')
        gomofsmax_t,gomofsmin_t=np.nanmax(gomofs_dict['gomofs_temp'].values),np.nanmin(gomofs_dict['gomofs_temp'].values)
        gomofsmax_d,gomofsmin_d=np.nanmax(gomofs_dict['gomofs_depth'].values),np.nanmin(gomofs_dict['gomofs_depth'].values) 
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
    
    
def to_list(lat,lon):
    "transfer the format to list"
    x,y=[],[]
    for i in range(len(lat)):
        x.append(lat[i])
        y.append(lon[i])
    return x,y

