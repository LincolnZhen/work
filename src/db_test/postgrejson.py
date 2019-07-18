#postgresql json测试
import psycopg2
import random
import time

conn = psycopg2.connect(database="postgres",user="postgres",password="...", host="192.168.40.129", port="5432")
cur = conn.cursor()
# cur.execute("explain analyze select * from api where jdoc #> '{student}' @> '{\"name\":\"slohqiudvyktmcrxabezjwnpgf\"}'")
# rows = cur.fetchall()
# print(rows)
cur.execute("select jdoc ->'student'->'name' from api")
rows = cur.fetchall()
print(rows[0])
names = set()
for i in range(0, len(rows)):
    names.add(rows[i][0])
# print(list(names)[0])
# cur.execute("drop table api")
# cur.execute("create table api(jdoc jsonb);")
# cur.execute("drop index hhh;")
# cur.execute("create index hhh on api using gin ((jdoc #> '{student}'));")

print(len(names))
# for i in range(270000):
#     name = "".join(random.sample('zyxwvutsrqponmlkjihgfedcba',26))
#     school = "".join(random.sample('zyxwvutsrqponmlkjihgfedcba',26))
#     # print(name,school)
#     while name in names:
#         name = "".join(random.sample('zyxwvutsrqponmlkjihgfedcba', 26))
#     names.add(name)
#     # cur.execute("insert into api(jdoc) values('{\"student\": {\"name\": \"" +name + "\",\"school\":\"" + school + "\"}}');")
#     cur.execute("insert into api2(jdoc) values ('{\"name\":\""+name+"\",\"school\":\""+school+"\"}');")
#     print(i)
test_list = list()
for i in range(1000):
    test_list.append(list(names)[random.randint(0, len(names))])
#
print('开始测试')
time_start = time.time()
total_time = 0
for i in range(10000):
    name = test_list[i%1000]
    cur.execute("explain analyze select * from api where jdoc @> '{\"student\":{\"name\":\"" + name + "\"}}';")
    # rows = cur.fetchall()
    # print(rows)
    # total_time = total_time + float(rows[-1][0][16:-3])
    # print(float(rows[-1][0][16:-3]))
time_end = time.time()
print(time_end-time_start)
conn.commit()
conn.close()
print(cur)