import os
import re
import psycopg2

path = r"\\168.36.1.170\share\archives"
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
        if re.match(r"OPLST:01*",line):
            inf = eval(line[22:])
            exchangeInstID = inf['ExchangeInstID']
            d = inf['InstrumentCode']
            print(d,fileName,date)
            if d in s:
                print("InsturmentCode 不是唯一的")
                print(s)
            else:
                s.add(d)
print("InstrumentCode是唯一的")

