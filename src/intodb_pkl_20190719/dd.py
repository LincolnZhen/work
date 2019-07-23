import os
import pickle
import re
from operator import itemgetter
import psycopg2
from sklearn.externals import joblib

filename = r"\\168.36.1.170\share\archives\20190321_S510050.pkl"
file  = open(filename,"rb")
l = list()
while True:
    data = pickle.load(file)
    l.append(data)

    if data  == {'KZ:S510050:EXCHTIME': '142314761', 'KZ:S510050:LATEST': 27960, 'KZ:S510050:BA1': 1377500,
          'KZ:S510050:BP1': 27960, 'KZ:S510050:SA1': 53900, 'KZ:S510050:SP1': 27970, 'ts': '14:23:14'}:
        break;
conn = psycopg2.connect(database="postgres",user="postgres",password="...", host="192.168.40.129", port="5432")
cur = conn.cursor()
for j in range(len(l)):
    data = str(l[j]).replace("\'", "\"")
    # print(data)
    cur.execute("insert into md_S510050 values(\'" + data + "\');")
print(l)
file.close()