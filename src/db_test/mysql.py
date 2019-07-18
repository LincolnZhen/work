#mysql 常规测试
import pymysql
import random
import time
db = pymysql.connect(host = "192.168.40.132", user="zx",passwd = "password",db = "mysql")
cur = db.cursor()
studentIDs = set()
cur.execute("select studentID from student;")
rows = cur.fetchall()
for i in range(len(rows)):
    studentIDs.add(rows[i][0])

# for i in range(270000):
#     name = "".join(random.sample('zyxwvutsrqponmlkjihgfedcba',20))
#     school = "".join(random.sample('zyxwvutsrqponmlkjihgfedcba',20))
#     studentID = random.randint(20000000, 99999999)
#     while studentID in studentIDs:
#         studentID = random.randint(200000000,999999999)
#     studentIDs.add(studentID)
#     cur.execute("insert into student(name,school,studentID) values(\""+name+"\","+"\""+school+"\","+str(studentID)+");")
#     print(i)

test_list = list()
for i in range(1000):
    test_list.append(list(studentIDs)[random.randint(0,len(studentIDs))])
    # test_list.append("".join(random.sample('zyxwvutsrqponmlkjihgfedcba',20)))
print("开始测试")
time_start = time.time()
for i in range(10000):
    # name = test_list[i%1000]
    # cur.execute("select * from student where name" + "=\'" + name +"\';")
    studentID = test_list[i%1000]
    cur.execute("select * from student where studentID" + "=\'" + str(studentID) + "\';")
time_end = time.time()
print(time_end-time_start)
print(cur)
db.commit()
db.close()