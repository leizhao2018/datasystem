
from datetime import datetime
import pickle
import rawdatamodules as rdm

dictionary_path='/home/jmanning/Desktop/data_dict/dictdatatest201833_n.p'
start_time_str='2018-7-1'
end_time_str='2019-3-21'
picture_save='/home/jmanning/Desktop/qwe/' #use


start_time=datetime.strptime(start_time_str,'%Y-%m-%d')
end_time=datetime.strptime(end_time_str,'%Y-%m-%d')
with open(dictionary_path, 'rb') as fp:
    dict = pickle.load(fp)   
tele_dict=dict['tele_dict']
raw_dict=dict['raw_dict']
record_file_df=dict['record_file_df']
index=tele_dict.keys()
for i in index:
    for j in range(len(record_file_df)): #find the location of data of this boat in record file 
        if i.lower()==record_file_df['Boat'][j].lower():
            break
    if len(raw_dict[i])==0 and len(tele_dict[i])==0:   
        continue
    else:
        rdm.draw_map(raw_dict[i],tele_dict[i],i,start_time,end_time,picture_save,record_file_df.iloc[j],dpi=300)
        rdm.draw_time_series_plot3(raw_dict[i],tele_dict[i],i,start_time,end_time,picture_save,record_file_df.iloc[j],dpi=300)
        print(i+' finished time series plot!')



