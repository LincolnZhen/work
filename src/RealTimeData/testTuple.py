import time
from collections import namedtuple
import random


class TestClass:
    def __init__(self, LATEST:str,BP1:str,SP1:str):
        self.LATEST = LATEST
        self.BP1 = BP1
        self.SP1 = SP1
class TestClass2:
    __slots__ = ["LATEST", "BP1",'SP1']

def main():
    test_num = 10000000

    t1 = list()
    t2 = list()
    t3 = list()
    t4 = list()
    latest = random.sample('zyxwvutsrqponmlkjihgfedcba', 5)
    sp1 = random.sample('zyxwvutsrqponmlkjihgfedcba', 5)
    bp1 = random.sample('zyxwvutsrqponmlkjihgfedcba', 5)
    # 写数据
    start_time = time.perf_counter()
    for i in range(test_num):
        t1.append((latest,sp1,bp1))
    end_time = time.perf_counter()
    print("元组的写时间:", end_time - start_time)

    N = namedtuple('Tuple', 'LATEST BP1 SP1')
    start_time = time.perf_counter()
    for i in range(test_num):
        t = TestClass2()
        t.LATEST = latest
        t.SP1 = sp1
        t.BP1 =bp1
        t2.append(t)
    end_time = time.perf_counter()
    print("TestClass2的写时间:", end_time - start_time)

    start_time = time.perf_counter()
    for i in range(test_num):
        t3.append(TestClass(latest,bp1,sp1))
    end_time = time.perf_counter()
    print("TestClass的写时间:", end_time - start_time)

    start_time = time.perf_counter()
    for i in range(test_num):
        t4.append({"LATEST":latest,"SP1":sp1, "BP1":bp1})
    end_time = time.perf_counter()
    print("字典的写时间:", end_time - start_time)

    #读数据
    start_time = time.perf_counter()
    for i in range(len(t1)):
        a = t1[i][0]
        b = t1[i][1]
        c = t1[i][2]
    end_time = time.perf_counter()
    print("元组的读时间",end_time - start_time)

    start_time = time.perf_counter()
    for i in range(len(t2)):
        a = t2[i].LATEST
        b = t2[i].BP1
        c = t2[i].SP1
    end_time = time.perf_counter()
    print("TestClass2的读时间", end_time - start_time)

    start_time = time.perf_counter()
    for i in range(len(t3)):
        a = t3[i].LATEST
        b = t3[i].BP1
        c = t3[i].SP1
    end_time = time.perf_counter()
    print("TestClass的读时间", end_time - start_time)

    start_time = time.perf_counter()
    for i in range(len(t4)):
        a = t4[i]['LATEST']
        b = t4[i]['BP1']
        c = t4[i]['SP1']
    end_time = time.perf_counter()
    print("字典的读时间",end_time-start_time)

if __name__ == '__main__':
    main()