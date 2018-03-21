import os
import csv

import const

import sys

from filelock import Timeout, FileLock

DATASET_NAME = 'echo-msk'

curr_dir_path = os.path.dirname(os.path.realpath(__file__))
dataset_folder_path = os.path.join(curr_dir_path, DATASET_NAME+"-dataset/")

def change_paths(find, replace):   

    for item in os.listdir(dataset_folder_path):
        csv_path = os.path.join(dataset_folder_path, item)

        
        if csv_path.split(".")[-1] != "csv":
            continue        
        

        f = open(csv_path, "r")
        parts = list(csv.reader(f))
        f.close()

        f = open(csv_path, "w")
        writer = csv.writer(f)
        for row in parts:
            row[0] = row[0].replace(find, replace)
            writer.writerow(row)
            
        f.close()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('USAGE: python change_paths.py <find_path> <replacement>')
    else:    
        change_paths(sys.argv[1], sys.argv[2])