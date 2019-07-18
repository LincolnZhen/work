# mariadb json测试
import pymysql
import random
import time
db = pymysql.connect(host = "192.168.40.134", user="zx",passwd = "password",db = "mysql")
cur = db.cursor()
# cur.execute("drop table api")
# cur.execute("create table api(attr varchar(1024),CHECK (JSON_VALID(attr)));")
# # cursor.execute("insert into api values('{\"name\": \"zx\", \"school\":\"nwpu\"}')")
# cur.execute("alter table api add name varchar(26) as (JSON_VALUE(attr, \'$.name\'));")
cur.execute("select JSON_VALUE(attr,\'$.name\') from api")
# cur.execute("create index namei on api(name);")
rows = cur.fetchall()
names = set()
for i in range(0, len(rows)):
    names.add(rows[i][0])

# print(len(names))
# for i in range(270000):
#     name = "".join(random.sample('zyxwvutsrqponmlkjihgfedcba',26))
#     school = "".join(random.sample('zyxwvutsrqponmlkjihgfedcba',26))
#     # print(name,school)
#     print(i)
#     if not name in names:
#         names.add(name)
#         cur.execute("insert into api(attr) values('{\"name\": \""+name+"\", \"school\":\"" + school + "\"}')")

# # print(len(names))
test_list = list()
for i in range(1000):
    test_list.append(list(names)[random.randint(1,len(names))])
print("开始测试")
time_start = time.time()
for i in range(10000):
    name = test_list[i%1000]
    #print(name)
    cur.execute("select * from api where name = \"" +name+"\";")
time_end = time.time()
print(time_end - time_start)
# print(tol_time/1000)
# print(rows)
db.commit()
db.close()

