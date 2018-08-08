from collections import defaultdict
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import json
import datetime
import re
import pandas as pd
import numpy as np
import pickle
import glob
import os

def generate_stusort_event_copy(log_file):
    """
    INPUT: path to event log file (one action per row)
    OUTPUT: group time-sorted data by student {'stuX':[dict_time0, dict_time3, dict_time4], 'stuY':[dict_time1, dict_time2], ...}
    """
    
    # first sorting the data by time
    all_data_paired_with_time = []
    with open(log_file) as data_file:
        for line in data_file.readlines():
            try:
                data = json.loads(line)
            except:
                #print(line)
                continue
            time_element = data['time']
            if '.' in time_element:
                date_object = datetime.datetime.strptime(time_element[:-6], '%Y-%m-%dT%H:%M:%S.%f')
            else:
                date_object = datetime.datetime.strptime(time_element[:-6], '%Y-%m-%dT%H:%M:%S')
            all_data_paired_with_time.append((line, date_object))

    print('sorting by time')
    s = sorted(all_data_paired_with_time, key=lambda p: p[1])
    ordered_event_list = [pair[0] for pair in s]
    
    #next sorting all the data by students based on the time_ordered file
    student_sorted = {}
    for i in ordered_event_list:
        i = json.loads(i)
        tmp_id=i['username']
        if tmp_id=='':
            try:
                tmp_id = i['event']['user_id']      
            except:
                tmp_id=0
                continue
        else:
            tmp_id=int(tmp_id.split('_')[1])
        if tmp_id !=0:
            if tmp_id in student_sorted :
                student_sorted[tmp_id].append(i)
            else:
                student_sorted[tmp_id] = [i]
        
    print('finishing sorting by stu')
    return student_sorted



def get_ce_types(event_list_file):
    """
    INPUT: event list file (RNN_event_list.csv)
    OUTPUT: dictionary with integer encodings of event types
    """
    if not os.path.exists(event_list_file):
        raise Exception("No list of RNN Events")

    with open(event_list_file) as f:
        event_list = f.read().splitlines()
    return {etype: i for i, etype in enumerate(event_list)}

def parse_event(data):
    """
    INPUT: row of individual action event
    OUTPUT: the event type it corresponds to
    """
    try:
        event_type = data['event_type']
        if re.match(r"/courses/.+/courseware/", event_type):
            parsed_event = 'courseware_load'
        elif re.match(r"/courses/.+/jump_to_id/[^/]+/?$", event_type):
            parsed_event = "jump_to_id"
        elif re.match(r"/courses/.+/$", event_type):
            parsed_event = 'homepage'
        elif re.match(r"/courses/.+/discussion/users$", event_type):
            parsed_event = 'view users'
        elif re.match(r"/courses/.+/about$", event_type):
            parsed_event = 'about page'
        elif re.match(r"/courses/.+/course_wiki$", event_type):
            parsed_event = 'wiki homepage'
        elif re.match(r"/courses/.+/discussion/forum/?$", event_type):
            parsed_event = "forum homepage"
        elif re.match(r"/courses/.+/info/$", event_type):
            parsed_event = "course info"
        elif re.match(r"/courses/.+/discussion/[^/]+/threads/create$", event_type):
            parsed_event = 'create discussion'
        elif re.match(r"/courses/.+/discussion/comments/[^/]+/reply$", event_type):
            parsed_event = 'reply discussion comment'
        elif re.match(r"/courses/.+/discussion/comments/[^/]+/flagAbuse$", event_type):
            parsed_event = 'flag abuse discussion comment'
        elif re.match(r"/courses/.+/discussion/comments/[^/]+/unFlagAbuse$", event_type):
            parsed_event = 'unflag abuse discussion comment'
        elif re.match(r"/courses/.+/discussion/comments/[^/]+/delete$", event_type):
            parsed_event = 'delete discussion comment'
        elif re.match(r"/courses/.+/discussion/comments/[^/]+/upvote$", event_type):
            parsed_event = 'upvote discussion comment'
        elif re.match(r"/courses/.+/discussion/comments/[^/]+/update$", event_type):
            parsed_event = 'update discussion comment'
        elif re.match(r"/courses/.+/discussion/comments/[^/]+/unvote$", event_type):
            parsed_event = 'unvote discussion comment'
        elif re.match(r"/courses/.+/discussion/comments/[^/]+/endorse$", event_type):
            parsed_event = 'endorse discussion comment'
        elif re.match(r"/courses/.+/discussion/comments/[^/]+$", event_type):
            parsed_event = 'load discussion comment'
        elif re.match(r"/courses/.+/discussion/forum/[^/]+/inline$", event_type):
            parsed_event = 'inline discussion'
        elif re.match(r"/courses/.+/discussion/forum/[^/]+/threads/[^/]+", event_type):
            parsed_event = 'thread discussion'
        elif re.match(r"/courses/.+/discussion/[^/]+/threads/create$", event_type):
            parsed_event = 'create thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/threads/follow$", event_type):
            parsed_event = 'follow thread2 discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/threads/unfollow$", event_type):
            parsed_event = 'unfollow thread2 discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/threads/reply$", event_type):
            parsed_event = 'reply thread2 discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/threads/upvote$", event_type):
            parsed_event = 'upvote thread2 discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/threads/unvote$", event_type):
            parsed_event = 'unvote thread2 discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/threads/delete$", event_type):
            parsed_event = 'delete thread2 discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/follow$", event_type):
            parsed_event = 'follow thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/unfollow$", event_type):
            parsed_event = 'unfollow thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/reply$", event_type):
            parsed_event = 'reply thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/upvote$", event_type):
            parsed_event = 'upvote thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/unvote$", event_type):
            parsed_event = 'unvote thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/delete$", event_type):
            parsed_event = 'delete thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/update$", event_type):
            parsed_event = 'update thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/pin$", event_type):
            parsed_event = 'pin thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/unpin$", event_type):
            parsed_event = 'unpin thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/flagAbuse$", event_type):
            parsed_event = 'flag abuse thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/unFlagAbuse$", event_type):
            parsed_event = 'delete thread discussion'
        elif re.match(r"/courses/.+/discussion/threads/[^/]+/close$", event_type):
            parsed_event = 'close thread discussion'
        elif re.match(r"/courses/.+/discussion/upload", event_type):
            parsed_event = 'upload to discussion'
        elif re.match(r'/courses/.+/info', event_type):
            parsed_event = 'info page'
        elif re.match(r'/courses/.+/pdfbook/', event_type):
            parsed_event = 'pdf book'
        elif re.match(r'/courses/.+/progress', event_type):
            parsed_event = 'progress page'
        elif re.match(r"/courses/.+/wiki/.*", event_type):
            parsed_event = 'wiki page'
        else:
            parsed_event = event_type
        # check for correct or incorrect problem_check
        if parsed_event == "problem_check":
            try:
                if data["event"]["success"] == "correct":
                    parsed_event = 'problem_check_correct'
                else:
                     parsed_event = 'problem_check_incorrect'
            except:
                parsed_event = 'problem_check_incorrect'
        return parsed_event
    except:
        print ("Found this event for the first time, skipping it for now")
        pass
    
def whether_passing(stu_sorted,course):
    passing_list=[]
    home_dir=os.path.dirname(os.getcwd())
    data_dir=home_dir+'/data'
    cer_file = os.path.join(data_dir, course + '-certificates_generatedcertificate-prod-analytics.sql')
    dt=pd.read_table(cer_file)
    #print(dt.user_id)
    for key in stu_sorted:
        status=str(dt[dt.user_id==int(key)]['status'])
        if status.find('audit_passing')!= -1 or status.find('downloadable')!= -1:
            passing_list.append(key)
    return passing_list


def whether_completion(stu_sorted,course):
    
    home_dir=os.path.dirname(os.getcwd())
    data_path=home_dir+'/data/'

    # # file for all the course xml files

    # STEP 1:
    # ------------------------------------------------------------------------------
    # correlate each problem with a grade fraction:

    # [STEP 1A]:
    # ..............................................................................
    # first, extract relevant sequentials:
    seq_lst = glob.glob(data_path + course + "/sequential/*.xml")
    seq_dict = {}
    # KEY: sequential xml name
    # VALUE: {"type": assignment type (matches "type" in grading policy),
    #         "deadline": deadline,
    #         "verticals": list of vertical xml names}


    # ****
    # # NOTE: In exploring the data.... Use this code to find which sequentials
    # # correspond to which assignments!
    # # when in sequential file of a course...
    # xml_namelst = glob.glob("*.xml")
    # for x in xml_namelst:
    #     tree = ET.parse(x)
    #     root = tree.getroot()
    #     try:
    #         print(x + ":")
    #         print(root.attrib['format'])
    #         print("\n")
    #     except:
    #         continue
    # ****

    for seq in seq_lst:
        tmp_tree = ET.parse(seq)
        tmp_root = tmp_tree.getroot()
        # tmp_f = open(data_path + course + "/sequential/" + seq + ".xml", "r")
        # NOTE: in testing, glob kept full path name?
        tmp_f = open(seq, "r")
        soup = BeautifulSoup(''.join(tmp_f.readlines()), 'lxml')
        tmp_f.close()
        try:
            type_format = tmp_root.attrib['format']
            if 'due' in soup.sequential:
                str_deadline = soup.sequential["due"].split("+")[0].strip("Z").strip("\"").strip("\'")
                # convert deadline into datetime object:
                deadline = datetime.datetime.strptime(str_deadline, '%Y-%m-%dT%H:%M:%S.%f' if '.' in str_deadline else '%Y-%m-%dT%H:%M:%S')
            else:
                deadline = datetime.datetime.strptime('3000-12-31T23:59:59.999999', '%Y-%m-%dT%H:%M:%S.%f')
            vert_lst = []
            for i in soup.findAll("vertical"):
                vert_lst.append(i['url_name'])
            seq_dict[seq] = {"type": type_format,
                             "deadline": deadline,
                             "verticals": vert_lst}
        except:
            continue


    # [STEP 1B]:
    # ..............................................................................
    # next, update this dictionary with problem IDs (when available)

    # NOTE: just group together ALL corresponding problem IDs for a single sequential
    # (i.e., across multiple verticals)
    # SO UPDATING seq_dict TO:
    # KEY: sequential xml name
    # VALUE: {"type": assignment type (matches "type" in grading policy),
    #         "deadline": deadline,
    #         "verticals": list of vertical xml names,
    #      -> "problems": list of ALL problem IDs}

    for seq in seq_dict:
        lil_dict = seq_dict[seq]

        problem_lst = []
        for vert in lil_dict["verticals"]:
            tmp_f = open(data_path + course + "/vertical/" + vert + ".xml", "r")
            soup = BeautifulSoup(''.join(tmp_f.readlines()), 'lxml')
            tmp_f.close()

            try:
                for i in soup.findAll("problem"):
                    problem_lst.append(i["url_name"])
            except KeyError:
                # if no problems assigned, just move on for now...
                # TODO: figure out how to handle assignments with no problem IDs (like essays)
                continue

        # and attach to overall dictionary
        seq_dict[seq]["problems"] = problem_lst

    # [STEP 1C]:
    # ..........................................................................
    # finally, match each sequential entry with a grade weight from grading_policy.json
    grpolicy_f = open(glob.glob(data_path + course + "/policies/*/grading_policy.json")[0], "r")
    grpolicy = json.load(grpolicy_f)
    grpolicy_f.close()

    weight_dict = {}
    weight_flag = {} # to see if a type has been used
    # KEYS: assignment types
    # VALUES: weight of INDIVIDUAL assignment types (i.e., normalized by min count)
    for i in grpolicy['GRADER']:
        # extract total weight, and min count (i.e., Quiz min_count -> 5 means 5 quizzes)

        # try with a drop count (subtract from min_count)
        try:
            # TODO: this is not entirely accurate... because lowest x are dropped no matter what
            # but I'm still allowing people to get credit for full number of assignments (including dropped)
            # may inflate the completion rate
            weight_dict[i['type']] = float(i['weight']) / (int(i['min_count']) - int(i['drop_count']))
            weight_flag[i['type']] = False
        # or, if no drop count...
        except KeyError:
            weight_dict[i['type']] = float(i['weight']) / int(i['min_count'])
            weight_flag[i['type']] = False
    print('single weight in different types: %s' % weight_dict)
    implement_weight = min(weight_dict.values())
    print('implement weight: %s' % implement_weight)
    # [STEP 1D]:
    # ..........................................................................
    # also extract graceperiod from policy.json (add to all problem deadlines,
    # with no grade penalty (TODO - should there be a penalty?))
    # NOTE: same deadline for EVERY problem
    policy_f = open(glob.glob(data_path + course + "/policies/*/policy.json")[0], "r")
    policy = json.load(policy_f)
    policy_f.close()

    # extract the simple string (either empty, or "num1 type1 num2 type2...")
    raw_graceperiod = policy[list(policy.keys())[0]]['graceperiod']
    splittime = raw_graceperiod.split(" ")

    # start out with empty additional time
    add_time = datetime.timedelta()

    # and add to it if there is any graceperiod (otherwise, index goes out of range):
    if raw_graceperiod != "":
        for i in range(0, len(splittime), 2):
            time_type = splittime[i + 1]
            # must check each time type for keyword argument:
            if time_type == 'weeks':
                add_time += datetime.timedelta(weeks=float(splittime[i]))
            elif time_type == 'days':
                add_time += datetime.timedelta(days=float(splittime[i]))
            elif time_type == 'hours':
                add_time += datetime.timedelta(hours=float(splittime[i]))
            elif time_type == 'minutes':
                add_time += datetime.timedelta(minutes=float(splittime[i]))
            elif time_type == 'seconds':
                add_time += datetime.timedelta(seconds=float(splittime[i]))

    # and now make a new dict of all problem IDs and corresponding weights:
    # TODO: recover assignments that are NOT problems (e.g., essays)

    prob_weight_dict = {}
    # KEY: problem ID
    # VALUE: {"weight": grade weight for that SINGLE problem (NOTE: must normalize each quiz weight by number of problems in that quiz),
    #         "deadline": deadline}

    for seq in seq_dict:
        lil_dict = seq_dict[seq]
        if 'problems' in lil_dict:
            num_probs = len(lil_dict['problems'])
            # check there are actually problems associated with that assignment:
            if num_probs > 0:
                weightperprob = weight_dict[lil_dict['type']] / num_probs
                try:
                    weight_flag.pop(lil_dict['type'])
                except:
                    pass
                # and add to problem dictionary:
                for prob in lil_dict['problems']:
                    prob_weight_dict[prob] = {'weight': weightperprob, 'deadline': lil_dict['deadline'], 'type': lil_dict['type']}
    if len(weight_flag.keys())>1:
        print('oppenasseement more than 2!')
    # STEP 2:
    # ------------------------------------------------------------------------------
    # find the minimum percentage to pass for each course:

    # NOTE: finding pass rate in grading_policy instead of in course.xml (below)
    try:
        # just a single passing threshhold:
        min_grade = float(grpolicy['GRADE_CUTOFFS']['Pass'])
    except KeyError:
        # then take the smallest grade value there
        # e.g., in DelftX-RI101x-1T2016, "GRADE_CUTOFFS": {"A": 0.75, "B": 0.59} -> take 0.59
        min_grade = min(grpolicy['GRADE_CUTOFFS'].values())

    # # naming varies, but always only one xml file in this folder
    # # TODO: replace with grading_policy.json later
    # xml_name = glob.glob(data_path + course + "/course/*.xml")[0]
    # tree = ET.parse(xml_name)
    # root = tree.getroot()
    #
    # # get minimum percentage:
    # min_grade = float(root.attrib['minimum_grade_credit'])

    # # also get start/end dates to check problems are submitted in proper time frame:
    # tmp_start = root.attrib['start'].split("+")[0].strip("Z").strip("\"").strip("\'")
    # tmp_end = root.attrib['end'].split("+")[0].strip("Z").strip("\"").strip("\'")
    #
    # # and now convert times into datetime objects:
    # new_start = datetime.datetime.strptime(tmp_start, '%Y-%m-%dT%H:%M:%S.%f' if '.' in tmp_start else '%Y-%m-%dT%H:%M:%S')
    # new_end = datetime.datetime.strptime(tmp_end, '%Y-%m-%dT%H:%M:%S.%f' if '.' in tmp_end else '%Y-%m-%dT%H:%M:%S')



    # STEP 3:
    # ------------------------------------------------------------------------------
    # make a dict of submitted problems and date of submission

    # (check which problems a student has submitted, and WHEN: before the deadline
    # (or course end date if no deadline), after the start date of the course):

    # NOTE: taking FIRST submission (giving them the benefit of the doubt)

    # [STEP 3A]:
    # ..............................................................................

    total = 0
    type_weight_dict = {}
    for prob in prob_weight_dict:
        total+=prob_weight_dict[prob]['weight']
        if prob_weight_dict[prob]['type'] not in type_weight_dict:
            type_weight_dict[prob_weight_dict[prob]['type']] = prob_weight_dict[prob]['weight']
        else:
            type_weight_dict[prob_weight_dict[prob]['type']] += prob_weight_dict[prob]['weight']
    print('total grade: %s' % total)
    print('sum grade in different types: %s' % type_weight_dict)
    if total < 1:
        min_grade = total * min_grade
    print('min_grade: %s' % min_grade)

    # [STEP 3B]:
    # ..............................................................................
    # # simplify full dictionary above to just (qualifying) submitted problems and weights per student:
    # allstu_weights = {}
    # # KEY: student_id
    # # VALUE: total (qualifying) attempted grade in the course

    # and dictionary with labels
    allstu_complete = []
    # KEY: student_id
    # VALUE: 0 (attempted below passing threshhold) or 1 (completed)

    for stu in stu_sorted:
        # dictionary of problems attempted and when (first attempt) for single student
        stu_probs = {}
        # KEY: problem_id
        # VALUE: problem weight

        for dic in stu_sorted[stu]:
            if dic['event_type'] == 'problem_check':
                problem_id = dic['event']['problem_id']
                for char in problem_id:
                    if not char.isalnum():
                        # assuming always the last part of the string,
                        # divided by symbols:
                        problem_id = problem_id.split(char)[-1]

                # check if it's already in stu_probs (i.e., already submitted before)
                if problem_id not in stu_probs:
                    # get submitted date (first submission)
                    messysubmtime = dic['time'].split('+')[0]
                    submtime = datetime.datetime.strptime(messysubmtime,'%Y-%m-%dT%H:%M:%S.%f' 
                                                          if '.' in messysubmtime else '%Y-%m-%dT%H:%M:%S')
                    # check that first submission is BEFORE problem due date:
                    # TODO: check why some problems aren't in prob_weight_dict..............
                    # possibly just not graded assignments? practice problems?
                    try:
                        if prob_weight_dict[problem_id]['deadline'] + add_time > submtime:
                            # and add to dict:
                            stu_probs[problem_id] = prob_weight_dict[problem_id]['weight']
                    except KeyError:
                        stu_probs[problem_id] = implement_weight
            if dic['event_type'] == 'openassessmentblock.create_submission' and len(weight_flag.keys()) == 1:
                stu_probs['openassessment'] = weight_dict[list(weight_flag.keys())[0]]
        # STEP 4:
        # ------------------------------------------------------------------------------
        # compute attempted grade fraction for each student, and label using min_grade
        # as threshhold:

        # now stu_probs is complete... sum up all the attempted weights
        # allstu_weights[stu] = sum(stu_probs.values())

        if sum(stu_probs.values()) >= min_grade:
            allstu_complete.append(stu)
        else:
            continue
    return allstu_complete

   