#-*- coding:utf-8 -*-

import os
import re
import psycopg2
import chardet

path = r"\\168.36.1.170\share\archives"
conn = psycopg2.connect(database="postgres",user="postgres",password="...", host="192.168.40.129", port="5432")
cur = conn.cursor()

dirs = os.listdir(path)
file_type = ".rd"

file_list = list()
for i in dirs:
    if re.match(r'MD_*', os.path.splitext(i)[0]) and os.path.splitext(i)[1] == '.rd':
        file_list.append(os.path.splitext(i)[0])

s = set()
for fileName in file_list:
    print(fileName)
    file = open(path+r"/"+ fileName+file_type,encoding='gb18030')
    date = fileName[3:11]
    time = fileName[-6:]
    if not time.startswith("15"):
        continue
    # print(date)
    # print(time)
    for line in file.readlines():
        if re.match(r"OPLST:01*",line) :
            inf = eval(line[22:])

            exchangeInstID = inf['ExchangeInstID']
            inf_str = str(inf).replace("\'","\"")

            # inf_str = inf_str.encode("utf-8")
            # print(chardet.detect(inf_str.encode()))
            # print(exchangeInstID,date)
            if inf['InstrumentCode'] not in s:
                c = inf['InstrumentCode']
                od = inf['OpenDate']
                ed = inf['ExpireDate']
                id = inf["InstrumentID"]
                cur.execute("insert into archive_oplst values(\'"+ c +"\',\'"+od+"\',\'"+ed+"\',\'"+id+"\',\'"+inf_str+"\');")
                s.add(inf['InstrumentCode'])
                print(c,od,ed,id)
cur.execute("select * from archive_oplst where InstrumentCode = \'510050P1909M02450\';")
rows = cur.fetchall()
print(rows)
conn.commit()
conn.close()
