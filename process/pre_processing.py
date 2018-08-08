# find a good way to generate a course_list
# use function to get dict
# then use IQR and action>3 filter to save the file to json 


import process_utils
import numpy as np
import pandas as pd
import os
from os import path 
import sys
import json
import configparser

# get the data directory
home_dir=path.dirname(os.getcwd())
data_dir=home_dir+'/data'
dataset_dir=home_dir+'/prepared_dataset'
stusort_dir=dataset_dir+'/stu_ordered_logfile'
filtered_stusort_dir=dataset_dir+'/filtered_stu_ordered_logfile'
config_file=home_dir+'/config.ini'

# create one if not exist
if not path.exists(dataset_dir):
    os.mkdir(dataset_dir)
if not path.exists(stusort_dir):
    os.mkdir(stusort_dir)
if not path.exists(filtered_stusort_dir):
    os.mkdir(filtered_stusort_dir)

# the variable which will export to other files
max_length=0

# generate course list
course_list = []
for (root, dirs, files) in os.walk(data_dir):
    for dirc in dirs:
        course_name = dirc.split('/')[-1]
        module_file = os.path.join(data_dir, course_name + '-courseware_studentmodule-prod-analytics.sql')
        cer_file = os.path.join(data_dir, course_name + '-certificates_generatedcertificate-prod-analytics.sql')
        log_file = os.path.join(data_dir, course_name.replace('-', '_') + '-event.log')
        if path.exists(module_file) and path.exists(cer_file) and path.exists(log_file):
            course_list.append(course_name)
        else:
            continue

# only for this dataset
course_list=course_list[:3]+course_list[4:11]
course_list

student_sorted=[]
for course in course_list:
    log_file = path.join(data_dir, course.replace('-', '_') + '-event.log')
    student_sorted.append(process_utils.generate_stusort_event_copy(log_file))

#save the ordered student dictionary to json in to the file
for (stu_sorted,course) in zip(student_sorted,course_list):
    js_stu_sorted = json.dumps(stu_sorted)
    with open(stusort_dir+'/'+course+ '-stu_log.json', 'w') as f:
        f.write(js_stu_sorted)

# clean the data with the first filter out rule for all three labels
cleaned_student_sorted=[]
for (stu_sorted,course) in zip(student_sorted,course_list):
    new_stu_sorted={}
    event_length=[]
    useless_length=[]
    cer_label=defaultdict(list)
    cer_file = os.path.join(data_dir, course + '-certificates_generatedcertificate-prod-analytics.sql')
    dt=pd.read_table(cer_file)
    print(course)
    print('before:%d'%(len(stu_sorted)))
    for key in stu_sorted:
        status=str(dt[dt.user_id==key]['status'])
        if status.find('audit_passing')!= -1 or status.find('downloadable')!= -1:
            event_length.append(len(stu_sorted[key])) 
        if len(stu_sorted[key])<=3:
            useless_length.append(key)
    #print("passing length:%d"%(len(event_length)))
    #print("useless length:%d"%(len(useless_length)))
    #print("ratio%f"%(len(useless_length)/len(stu_sorted)))
    IQR=np.percentile(event_length,75)-np.percentile(event_length,25)
    upper=np.percentile(event_length,75)+1.5*IQR
    #print('upper:%d'%(upper))
    
    # filter out all the students who have 3 or less actions
    for key in stu_sorted:
        if len(stu_sorted[key]) <= upper and len(stu_sorted[key])>3:
            new_stu_sorted[key]=stu_sorted[key]
    print('after:%d'%(len(new_stu_sorted)))    
    cleaned_student_sorted.append(new_stu_sorted)
    
#calculate the max_length
for stu_sorted in cleaned_student_sorted:
    for key in stu_sorted:
        temp=len(stu_sorted[key])
        if temp>max_length:
            max_length=temp

#revise the max_length value into the config file
config = configparser.ConfigParser()
config.read(config_file)
config.set('Options','max_seq_len', str(max_length))
config.write(open(config_file, "w")) 

#save the ordered student dictionary to json in to the file
for (stu_sorted,course) in zip(cleaned_student_sorted,course_list):
    js_stu_sorted = json.dumps(stu_sorted)
    with open(filtered_stusort_dir+'/'+course+ '-stu_log.json', 'w') as f:
        f.write(js_stu_sorted)
