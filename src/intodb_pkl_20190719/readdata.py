#读取pkl文件,并建表
import os
import pickle
import re
from operator import itemgetter
import psycopg2
import datetime
import random
import time

# md_S510050
def handle_file(filename:str,date:str):
    # if filename == r"\\168.36.1.170\share\archives\20190321_S510050.pkl":
    #     return
    print(filename)
    if os.path.getsize(filename) == 0:
        print("empty file",filename)
        return
    objects = []
    with (open(filename, "rb")) as openfile:
        while True:
            try:
                d = pickle.load(openfile)
                if filename == r"\\168.36.1.170\share\archives\20190321_S510050.pkl" and d == {'KZ:S510050:EXCHTIME': '142314761', 'KZ:S510050:LATEST': 27960, 'KZ:S510050:BA1': 1377500, 'KZ:S510050:BP1': 27960, 'KZ:S510050:SA1': 53900, 'KZ:S510050:SP1': 27970, 'ts': '14:23:14'}:
                    print("遇到坏文件了")
                    d['Date'] = date
                    objects.append(d)
                    return objects
                d['Date'] = date
                objects.append(d)
            except EOFError:
                break
    return objects

path = r"\\168.36.1.170\share\archives"
dirs = os.listdir(path)
file_type = ".pkl"

file_list = list()
for i in dirs:
    if re.match(r"20*",i) and os.path.splitext(i)[1] == '.pkl':
        # print(os.path.splitext(i)[0],os.path.splitext(i)[1])
        date = os.path.splitext(i)[0][:8]
        d = dict()
        d["date"] = date
        d["file"] = path + "\\" + os.path.splitext(i)[0] + os.path.splitext(i)[1]
        file_list.append(d)

conn = psycopg2.connect(database="postgres",user="postgres",password="...", host="192.168.40.129", port="5432")
cur = conn.cursor()
file_list.sort(key=itemgetter('date'),reverse = False)
for i in range(len(file_list)):
    o = handle_file(file_list[i]["file"],file_list[i]["date"])
    # print(o)
    if o == None:
        continue
    for j in range(len(o)):
        data = str(o[j]).replace("\'","\"")
        # print(data)
        cur.execute("insert into md_S510050 values(\'" + data + "\');")
# print(file_list)
conn.commit()
conn.close()