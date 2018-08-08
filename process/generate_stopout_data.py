import process_utils
from collections import defaultdict
import numpy as np
import pandas as pd
import os
from os import path 
import sys
import json
import datetime
from keras.utils import np_utils
from keras.preprocessing import sequence
import configparser

# get the data directory
home_dir=path.dirname(os.getcwd())
data_dir=home_dir+'/data'
dataset_dir=home_dir+'/prepared_dataset'
stusort_dir=dataset_dir+'/filtered_stu_ordered_logfile'
event_list_file=home_dir+'/RNN_event_list.csv'
keras_file=dataset_dir+'/training_data'
config_file=home_dir+'/config.ini'
if not path.exists(keras_file):
    os.mkdir(keras_file)

# readn the json file:
# get the basic directories and read the cleaned_student_sorted event data
cleaned_student_sorted=[]
course_name=[]
course_time=[]
for (root, dirs, files) in os.walk(stusort_dir):
    for file in files:
        name=file.split('-')
        if len(name)==4:
            c_name=name[0]+'-'+name[1]
            c_time=name[2]
        else:
            c_name=name[0]+'-'+name[1]+'-'+name[2]
            c_time=name[3]
        course_name.append(c_name)
        course_time.append(c_time)
        with open(path.join(root,file)) as f:
            stu_sorted=json.load(f)
        cleaned_student_sorted.append(stu_sorted)

        
# Generate dictionary of course event to integer encoding
ce_types = process_utils.get_ce_types(event_list_file)


# generate the file for x_train and y_stopout dict
max_week_count=0
train_sets=[]
stopout_sets=[]



# Generate dataframe of user and their corresponding chronological event sequence
for (stu_sorted,c_name,c_time) in zip(cleaned_student_sorted,course_name,course_time):
    # find the right policy file and the start time and end time in the data directory
    policy_file = path.join(data_dir,c_name+'-'+c_time +'/policies/course/policy.json')
    try:
        with open(policy_file,'r') as f:
            data=json.load(f)
        start_time=data['course/course']['start']
        end_time=data['course/course']['end']
    except:
        policy_file = path.join(data_dir,c_name+'-'+c_time +'/policies/'+c_time+'/policy.json')
        with open(policy_file,'r') as f:
            data=json.load(f)
        start_time=data['course/'+c_time]['start']
        end_time=data['course/'+c_time]['end']   

    tt = start_time.split('+')[0]
    start_time = datetime.datetime.strptime(tt, '%Y-%m-%dT%H:%M:%S.%fZ' if '.' in tt else '%Y-%m-%dT%H:%M:%SZ')
    tt = end_time.split('+')[0]
    end_time = datetime.datetime.strptime(tt, '%Y-%m-%dT%H:%M:%S.%fZ' if '.' in tt else '%Y-%m-%dT%H:%M:%SZ')
    event_stream_per_student = defaultdict(list)
    stopout_label=defaultdict(list)
    
    for u_name, actions in stu_sorted.items():
        for line in actions:
            tt =  line['time'].split('+')[0]
            time_element = datetime.datetime.strptime(tt, '%Y-%m-%dT%H:%M:%S.%f' if '.' in tt else '%Y-%m-%dT%H:%M:%S')
            try:
                parsed_event = process_utils.parse_event(line)
            except ValueError:
                print("Unable to parse:", line)
                continue
            if parsed_event in ce_types:
                event_stream_per_student[u_name].append((ce_types[parsed_event], time_element))

        last_action = event_stream_per_student[u_name][-1][1]
        stop_flag=last_action-datetime.timedelta(days=2)  

        week_count = 1
        weekend=start_time
        # add weekends
        while(1):
            if weekend>=end_time:
                break
            event_stream_per_student[u_name].append((77+week_count, weekend))
            weekend = weekend+ datetime.timedelta(days=7)
            week_count=week_count+1
            if(week_count>max_week_count):
                max_week_count=week_count
        event_stream_per_student[u_name].append((77+week_count, end_time))
        
        # sort again
        s = sorted(event_stream_per_student[u_name], key=lambda p: p[1])
        event_stream_per_student[u_name] = [item[0] for item in s]

        times = [item[1] for item in s]
        index = len([i for i in times if i < stop_flag])
        stopout_label[u_name]=[0]*index+[1]*(len(event_stream_per_student[u_name])-index)
     
    print('finishing one.....' )           
    train_sets.append(event_stream_per_student)
    stopout_sets.append(stopout_label)

#revise the max_length value into the config file
max_input_dim = 78+max_week_count
config = configparser.ConfigParser()
config.read(config_file)
config.set('Options','max_input_dim', str(max_input_dim))
config.write(open(config_file, "w")) 

#parsing the dictionary to matrix/keras sequence for training
max_input_dim=int(config.get('Options','max_input_dim'))
max_seq_len=int(config.get('Options','max_seq_len'))

for i in range(0,len(train_sets)):
    ori_length=[]
    week_label=[]
    for key in train_sets[i]:
        events=train_sets[i][key]
        ori_length.append(len(events))
        temp=[events.index(i) for i in events if i>77 ]
        week_label.append(temp)
    events_df = pd.DataFrame({'username': list(train_sets[i].keys()), 'seq': list(train_sets[i].values())})
    event_list = events_df['seq'].values
    event_list_binary = [np_utils.to_categorical(x, max_input_dim) for x in event_list]   
    stopout_df = pd.DataFrame({'username': list(stopout_sets[i].keys()), 'seq': list(stopout_sets[i].values())})
    stopout_list = stopout_df['seq'].values
    
    # make the directory for completion file
    course_dir=keras_file+'/'+course_name[i]+'-'+course_time[i]
    course_train_dir=course_dir+'/train'
    stopout_dir= course_train_dir+'/stop'
    if not path.exists(course_dir):
        os.mkdir(course_dir)
    if not path.exists(course_train_dir):
        os.mkdir(course_train_dir)
    if not path.exists(stopout_dir):
        os.mkdir(stopout_dir)
        
    # because of the size, we have to save the file seperately
    count=int(len(event_list)/10000)
    for k in range(0,count+1):
        print(k)
        if k==count:
            t_ori_length=ori_length[10000*k:]
            t_week_label=week_label[10000*k:]  
            t_event=event_list_binary[10000*k:]
            t_stopout=stopout_list[10000*k:]
        else:
            t_ori_length=ori_length[10000*k:10000*(k+1)]
            t_week_label=week_label[10000*k:10000*(k+1)]  
            t_event=event_list_binary[10000*k:10000*(k+1)]
            t_stopout=stopout_list[10000*k:10000*(k+1)]
        
        #padding
        t_x_train = sequence.pad_sequences(t_event, maxlen=max_seq_len, dtype='bool',padding='post', truncating='post')
        t_y_stopout=sequence.pad_sequences(t_stopout,maxlen=max_seq_len, dtype='bool',padding='post', truncating='post')

        np.save(stopout_dir+'/x'+str(k)+'_train',t_x_train)
        np.save(stopout_dir+'/ori_length'+str(k),t_ori_length)
        np.save(stopout_dir+'/week_label'+str(k),t_week_label)
        np.save(stopout_dir+'/y'+str(k)+'_stopout',t_y_stopout)
        
    print('finishing')
