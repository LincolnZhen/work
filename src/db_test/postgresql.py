#postgresql 常规测试
import psycopg2
import random
import  time
conn = psycopg2.connect(database="postgres",user="postgres",password="...", host="192.168.40.129", port="5432")
cur = conn.cursor()
studentIDs = set()
cur.execute("select studentID from student;")
rows = cur.fetchall()
for i in range(0, len(rows)):
    studentIDs.add(rows[i][0])

# print(list(studentIDs)[0])
# for i in range(270000):
#     name = "".join(random.sample('zyxwvutsrqponmlkjihgfedcba',20))
#     school = "".join(random.sample('zyxwvutsrqponmlkjihgfedcba',20))
#     studentID = random.randint(20000000, 99999999)
#     while studentID in studentIDs:
#         studentID = random.randint(200000000,999999999)
#     studentIDs.add(studentID)
#     cur.execute("insert into student (name, school, studentID) values (\'"+name+"\',"+"\'"+school+"\',"+str(studentID)+");")
#     print(i)
test_list = list()
for i in range(1000):
    # test_list.append(list(studentIDs)[random.randint(0,len(studentIDs))])
    # name
    test_list.append("".join(random.sample('zyxwvutsrqponmlkjihgfedcba',20)))

print("开始测试")
time_start = time.time()
for i in range(10000):
    name = test_list[i%1000]
    cur.execute("select * from student where name" + "=\'" + name + "\';")
    # studentID = test_list[i%1000]
    # cur.execute("select * from student where studentID="+ str(studentID)+";")
time_end = time.time()
print(time_end-time_start)
conn.commit()
conn.close()