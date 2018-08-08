import os
import shutil
import argparse
import json
try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser


def main(args):
    config = ConfigParser.ConfigParser()
    config.read(args.config_file)
    padded_data_path = config.get('Paths', 'padded_data_path')
    train_path = config.get('Paths', 'train_path')
    test_path = config.get('Paths', 'test_path')
    if os.path.exists(train_path):
        shutil.rmtree(train_path)
    if os.path.exists(test_path):
        shutil.rmtree(test_path)
    os.mkdir(train_path)
    os.mkdir(test_path)
    #
    if args.random == True:
        # split the dataset randomly among all courses
        pass
    else:
        # split the dataset on designated courses
        des_train_courses = config.get('Options', 'designated_train_courses')
        des_test_courses = config.get('Options', 'designated_test_courses')
        for course in json.loads(des_train_courses):
            shutil.copytree(os.path.join(padded_data_path, course), os.path.join(train_path, course))
            print('course %s copied into train set' %course)
        for course in json.loads(des_test_courses):
            shutil.copytree(os.path.join(padded_data_path, course), os.path.join(test_path, course))
            print('course %s copied into test set' % course)

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--random', type = str2bool, default=False, help = 'Whether to generate training and testing sets randomly')
    parser.add_argument('--config_file', type = str, default= './config.ini', help = 'Dir for config file')
    args = parser.parse_args()
    main(args)