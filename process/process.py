import utils
from collections import defaultdict
import numpy as np
import pandas as pd
import sys
import json
import argparse
import configparser

def pre_process(course_name,start_date,end_date):
    """
    INPUT: the course name, start and end date of the course needed making prediction 
    OUTPUT: dataframe with two columns: 'userID' and its corresponding 'user_seqs'
    """
    
  # get the data directory
  home_dir=path.dirname(os.getcwd())
  data_dir=home_dir+'/data'
  log_file=data_dir+ '/'+course+'-event.log'
  predictions_file=data_dir+'/predictions.csv'
  cut_off_file=data_dir+ 'cut_off.json'
  config_file=home_dir+'/config.ini'

  config = ConfigParser.ConfigParser()
  config.read(config_file)
  
  # Generate dictionary of student to list of his/her actions sorted chronologically
  student_sorted=utils.generate_ordered_event_copy(log_file)
  # Generate dictionary of course event to integer encoding
  ce_types = utils.get_ce_types()
  # get the pre_action list for events
  pre_action = config.get('Options', 'preaction')
  
  print('data processing')

  #make sure the log is processing the "real time" log
  now=datetime.datetime.now()
  if now<end_time:
      end_time=now
  #get the cutoffs
  with open(cut_off_file, 'r') as f:
      time_interval=json.load(f)  
  #initiate the user and user_sequence list
  users_ID=[]
  users_seq=[]
  #get the sequence
  for user,actions in student_sorted.items():
      u_seq=[]
      temp=[]
      for line in actions:
          tt =  line['time'].split('+')[0]
          time_element = datetime.datetime.strptime(tt, '%Y-%m-%dT%H:%M:%S.%f' if '.' in tt else '%Y-%m-%dT%H:%M:%S')
          try:
              parsed_event = process_utils.parse_event(line)
          except ValueError:
              print("Unable to parse:", line)
              continue
          if parsed_event in ce_types and time_element<end_time:
              temp.append((ce_types[parsed_event]+2, time_element))
      #change the category by time intervals    
      if len(temp)==1:
          u_seq=temp
      else:
          for i in range(0,len(temp)):
              time=temp[i][1]
              event=temp[i][0]
              if (event-2) in pre_action:
                  if i==0:
                      delta=datetime.timedelta(seconds=0)
                  else:
                      delta=time-temp[i-1][1]
              else:
                  if event==4:
                      delta=datetime.timedelta(seconds=0)
                  else:
                      if i==(len(temp)-1):
                          delta=end_time-time
                      else:
                          delta=temp[i+1][1]-time

              if str(event-2) not in cutoff.keys() or delta<=datetime.timedelta(seconds=cutoff[str(event-2)][0]):
                  u_seq.append((event,time))
              elif delta<=datetime.timedelta(seconds=cutoff[str(event-2)][1]):
                  u_seq.append((event+77,time))
              elif delta<=datetime.timedelta(minutes=30):
                  u_seq.append((event+2*77,time))
              else:
                  u_seq.append((event+3*77,time))
                  
      #add the time markers
      weekend=start_time
      # add weekends
      while(1):
          if weekend>end_time:
              break
          u_seq.append((0, weekend))
          weekend = weekend+ datetime.timedelta(days=7)
      # add days
      dayend=start_time
      while(1):
          dayend = dayend+ datetime.timedelta(days=1)
          if dayend>end_time:
              break
          u_seq.append((1, dayend))
          
      # sort again
      s = sorted(u_seq, key=lambda p: p[1])
      seq= [item[0] for item in s]
      #generate the list
      users_seq.append(seq)
      users_ID.append(user)
      
  #generate the event_df    
  event_df = pd.DataFrame({'userID': users_ID, 'seq':users_seq})
  return event_df
