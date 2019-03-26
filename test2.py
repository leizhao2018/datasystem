
from datetime import datetime
import pickle
import test
#import rawtelemodules as rdm

dictionary_path='/home/jmanning/Desktop/teledictdatatest2018_n.p'
start_time_str='2018-7-1'
end_time_str='2019-3-21'
picture_save='/home/jmanning/Desktop/qwe/' #use


start_time=datetime.strptime(start_time_str,'%Y-%m-%d')
end_time=datetime.strptime(end_time_str,'%Y-%m-%d')
#
#with open(dictionary_path, 'rb') as fp:
#    modules_data = pickle.load(fp)
with open('/home/jmanning/Desktop/data_dict/dictdatatest201833_n.p', 'rb') as fp:
    dict = pickle.load(fp)   
##record_file=dict['record_file_df']
#for i in range(len(record_file)):
#    if record_file['Boat'][i].lower()=='Tenacious_II'.lower():
#        break
#tele_dict=modules_data['Tenacious_II']
#
#raw_dict={}
#tele_dict=tele_dict.drop(0)
#tele_dict.index=range(len(tele_dict))
#test.draw_time_series_plot_test(raw_dict,tele_dict,name,start_time,end_time,picture_save,record_file=record_file.iloc[i])
#test.draw_map(raw_dict,tele_dict,name,start_time,end_time,picture_save,record_file.iloc[i],dpi=400)
#draw_time_series_plot_test(raw_dict,tele_dict,name,start_time,end_time,path_picture_save,record_file=False,dpi=300)

#for i in range(len(modules_data)):
#    modules_data['time'][i]=datetime.strptime(modules_data['time'][i],'%Y-%m-%d %H:%M:%S')
#dict=modules_data
#dict1=dict
#
##draw_time_series_plot_test(modules_data,dict1,i,start_time,end_time,picture_save,dpi=300)
#test.draw_time_series_plot_test(,modules_data,'lians sddaad',start_time,end_time,picture_save,dpi=300)


#def PunctuationUS2CH(string):
#    table = {ord(f):ord(t) for f,t in zip(u'，。！？【】（）％＃＠＆１２３４５６７８９０',u',.!?[]()%#@&1234567890')}
#    return string.translate(table)
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
#        rdm.draw_map(raw_dict[i],tele_dict[i],i,start_time,end_time,picture_save,record_file_df.iloc[j],dpi=300)
        test.draw_time_series_plot3(raw_dict[i],tele_dict[i],i,start_time,end_time,picture_save,record_file_df.iloc[j],dpi=300)
        print(i+' finished time series plot!')
#        test.draw_time_series_plot(raw_dict[i],tele_dict[i],i,start_time,end_time,picture_save,record_file=record_file_df.iloc[j])



