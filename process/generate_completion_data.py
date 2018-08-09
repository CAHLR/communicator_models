import process_utils
from collections import defaultdict
import numpy as np
import pandas as pd
import os
from os import path 
import sys
import json
import datetime
import random
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

# read the json file:
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


import importlib
importlib.reload(process_utils)


#the second filter out
event_stream_per_students=[]
for stu_sorted in cleaned_student_sorted:
    event_stream_per_student = defaultdict(list)
    for u_name, actions in stu_sorted.items():
        pure_action=[line['event_type'] for line in actions]
        if 'problem_check' in pure_action:
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
    event_stream_per_students.append(event_stream_per_student)
    print('finishing')

'''
# testing the whether_completion function
event_stream_per_students=[]
for (stu_sorted,c_name,c_time) in zip(cleaned_student_sorted,course_name,course_time):g
    completion_list=process_utils.whether_completion(stu_sorted,c_name+'-'+c_time)
    passing_list=process_utils.whether_passing(stu_sorted,c_name+'-'+c_time)
    length=2*len(completion_list)
    print(c_name)
    for i in passing_list:
        if i not in completion_list:
            print(i)
    print("completion_length:%d"%(length/2))
    print("passing_length:%d"%(len(passing_list)))
    print("total_length:%d"%(len(new_stu_sorted)))
    
'''

train_sets=[]
comp_sets=[]

if not path.exists(keras_file):
    os.mkdir(keras_file)

# Generate dataframe of user and their corresponding chronological event sequence
for (stu_sorted,event_stream_per_student,c_name,c_time) in zip(cleaned_student_sorted,event_stream_per_students,course_name,course_time):
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
    
    completion_list=process_utils.whether_completion(stu_sorted,c_name+'-'+c_time)
    
    train_data= defaultdict(list)
    comp_label= defaultdict(list)
    print(c_name)
    train_data=event_stream_per_student.copy()
    for u_name, actions in event_stream_per_student.items():
        week_count = 1
        weekend=start_time
        while(1):
            if weekend>=end_time:
                break
            train_data[u_name].append((77+week_count, weekend))
            weekend = weekend+ datetime.timedelta(days=7)
            week_count=week_count+1
        train_data[u_name].append((77+week_count, end_time))
        
        # sort again
        s = sorted(train_data[u_name], key=lambda p: p[1])
        train_data[u_name] = [item[0] for item in s]

        if u_name in completion_list:
            comp_label[u_name]=[1]*(len(train_data[u_name]))
        else:
            comp_label[u_name]=[0]*(len(train_data[u_name]))
     
    print('finishing one.....' )           
    train_sets.append(train_data)
    comp_sets.append(comp_label)

    
#parsing the dictionary to matrix/keras sequence for training
config = configparser.ConfigParser()
config.read(config_file)
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
    comp_df = pd.DataFrame({'username': list(comp_sets[i].keys()), 'seq': list(comp_sets[i].values())})
    comp_list = comp_df['seq'].values
    
    # make the directory for completion file
    course_dir=keras_file+'/'+course_name[i]+'-'+course_time[i]
    course_train_dir=course_dir+'/train'
    comp_dir=course_train_dir+'/comp'
    if not path.exists(course_dir):
        os.mkdir(course_dir)
    if not path.exists(course_dir):
        os.mkdir(course_dir)
    if not path.exists(comp_dir):
        os.mkdir(comp_dir)
        
    # because of the size, we have to save the file apart
    
    count=int(len(event_list)/10000)
    for k in range(0,count+1):
        print(k)
        if k==count:
            t_ori_length=ori_length[10000*k:]
            t_week_label=week_label[10000*k:]  
            t_event=event_list_binary[10000*k:]
            t_comp=comp_list[10000*k:]
        else:
            t_ori_length=ori_length[10000*k:10000*(k+1)]
            t_week_label=week_label[10000*k:10000*(k+1)]  
            t_event=event_list_binary[10000*k:10000*(k+1)]
            t_comp=comp_list[10000*k:10000*(k+1)]
        
        #padding
        t_x_train = sequence.pad_sequences(t_event, maxlen=max_seq_len, dtype='bool',padding='post', truncating='post')
        t_y_comp=sequence.pad_sequences(t_comp,maxlen=max_seq_len, dtype='bool',padding='post', truncating='post')

        np.save(comp_dir+'/x'+str(k)+'_train',t_x_train)
        np.save(comp_dir+'/ori_length'+str(k),t_ori_length)
        np.save(comp_dir+'/week_label'+str(k),t_week_label)
        np.save(comp_dir+'/y'+str(k)+'_comp',t_y_comp)
        
    print('finishing')

