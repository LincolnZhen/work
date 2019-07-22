#-*- coding: UTF-8 -*-
import threading
import time


def second(key:str):
    print("It is " + str(key),time.time(),time.localtime())


class PeriodThread(threading.Thread):
    def __init__(self, firstrange:list, secondrange: list):
        threading.Thread.__init__(self)
        currentDate = str(time.localtime().tm_year) + "-" + str(time.localtime().tm_mon) + "-" + str(time.localtime().tm_mday) #> "2019-07-11"
        print(currentDate)
        self.f_starttime = time.mktime(time.strptime(currentDate + " " + firstrange[0] , "%Y-%m-%d %H:%M:%S"))    # >  1563785375.0012558
        self.f_endtime = time.mktime(time.strptime(currentDate + " " + firstrange[1] , "%Y-%m-%d %H:%M:%S"))      # >  1563785475.0012558
        self.s_starttime = time.mktime(time.strptime(currentDate + " " + secondrange[0] , "%Y-%m-%d %H:%M:%S"))   # >  1563785575.0012558
        self.s_endtime = time.mktime(time.strptime(currentDate + " " + secondrange[1] , "%Y-%m-%d %H:%M:%S"))     # >  1563785675.0012558

    def run(self):
        DemoThread.run(self,int(time.time()))

    def start(self):
        print("上班啦")
        if time.time() < self.f_starttime:
            interval = int(self.f_starttime) - time.time()
            time.sleep(interval)
        if time.time() >= self.f_starttime and time.time() <= self.f_endtime:
            print("上午开盘")
            while time.time() < self.f_endtime:
                self.run()
                time1 = time.time()
                time.sleep(int(time.time())+1 - time.time())
                if int(time.time()) - int(time1) > 1:
                    print("miss one")
            self.run()
            print("上午收盘")
        if time.time() < self.s_starttime:
            interval = int(self.s_starttime) - time.time()
            time.sleep(interval)

        if time.time() >= self.s_starttime and time.time() <= self.s_endtime:
            print("下午开盘")
            while time.time() < self.s_endtime:
                self.run()
                time1 = time.time()
                time.sleep(int(time.time())+1 - time.time())
                if int(time.time()) - int(time1) > 1:
                    print("miss one")
            self.run()
            print("下午收盘")
        print("下班啦")
class DemoThread(PeriodThread):
    def run(self,key):
        second(key)


def main():
    # print(type(time.localtime().tm_mon),time.localtime().tm_year,time.localtime().tm_mday)
    t = PeriodThread(["15:45:50","17:48:59"],["17:49:30","17:54:50"])
    t.start()
    time.sleep(10)
    # t.join()

if __name__=='__main__':
    main()

