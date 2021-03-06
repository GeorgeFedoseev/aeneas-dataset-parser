import csv
from filelock import Timeout, FileLock





def clear_csv(csv_path):
    with FileLock(csv_path + ".lock"):
        f = open(csv_path, "w+")
        csv_writer = csv.writer(f)
        f.close()

def read_all(csv_path):
    data = []
    with FileLock(csv_path + ".lock"):
        f = open(csv_path, "r+")
        csv_reader = csv.reader(f)
        data = list(csv_reader)

    return data


def get_column_csv(csv_path, column_index):
    column = []
    with FileLock(csv_path + ".lock"):
        csv_reader = csv.reader(open(csv_path, "r+"))
        data = list(csv_reader)

        for row in data:
            if len(row) > column_index:
                column.append(row[column_index])
    return column

def append_rows_to_csv(csv_path, rows):
    
    with FileLock(csv_path + ".lock"):
        f = open(csv_path, "a+")
        csv_writer = csv.writer(f)
        for row in rows:
            csv_writer.writerow(row)

        f.close()

def write_rows_to_csv(csv_path, rows):
    clear_csv(csv_path)
    append_rows_to_csv(csv_path, rows)

def append_column_to_csv(csv_path, column):
    
    with FileLock(csv_path + ".lock"):
        f = open(csv_path, "a+")
        csv_writer = csv.writer(f)
        for val in column:
            csv_writer.writerow([val])
        f.close()


def write_column_to_csv(csv_path, column):
    clear_csv(csv_path)
    append_column_to_csv(csv_path, column)

def get_item_in_csv(csv_path, row_first_cell_val):
    with FileLock(csv_path + ".lock"):
        csv_reader = csv.reader(open(csv_path, "r+"))
        data = list(csv_reader)

        for row in data:
            if len(row) > 0 and row[0] == row_first_cell_val:
                return row
    return None

def is_item_in_csv(csv_path, item):
    get_item_in_csv(csv_path, item) != None


def put_item_to_csv(csv_path, row_list):
    if not is_item_in_csv(csv_path, row_list[0]):
        with FileLock(csv_path + ".lock"):
            f = open(csv_path, "a+")
            csv_writer = csv.writer(f)
            csv_writer.writerow(row_list)
            f.close()


def pop_first_row_in_csv(csv_path):
    with FileLock(csv_path + ".lock"):
        first_row = None

        csv_reader = csv.reader(open(csv_path, "r+"))

        data = list(csv_reader)
        if len(data) > 0:
            first_row = data.pop(0)

        f = open(csv_path, "wb")
        csv_writer = csv.writer(f)

        # write without first row
        for row in data:
            csv_writer.writerow(row)

        f.close()

        #print 'wrote back '+str(len(data))+' to '+csv_path

    return first_row

def remove_row_by_first_val(csv_path, val):
    with FileLock(csv_path + ".lock"):
        csv_reader = csv.reader(open(csv_path, "r+"))
        data = list(csv_reader)   

        f = open(csv_path, "w")
        csv_writer = csv.writer(f)
        # write without this row
        for row in data:
            if not (len(row) > 0 and row[0] == val):
                csv_writer.writerow(row)

        f.close()



