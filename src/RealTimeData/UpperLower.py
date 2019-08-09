import redis
import time
import yaml
from collections import deque


class UpperLower:
    def __init__(self, interval_name:str, upp_pro:float, low_pro:float):
        self.interval_dict = {"A30m":1800,'A10s':10}
        self.interval_name = interval_name
        self.interval = self.interval_dict[self.interval_name]
        self.data_queue = deque(maxlen=self.interval)
        self.upper = None
        self.low = None
        self.upp_pro = upp_pro
        self.low_pro = low_pro

    def appendData(self,data:int):
        if data == None:
            self.data_queue = deque(maxlen=self.interval)
            self.upper = None
            self.low = None
        else:
            self.data_queue.append(float(data))

    def computeUpLow(self):
        if len(self.data_queue) == self.interval:
            print("hhh")
            print(self.data_queue)
            t_l = list(self.data_queue)
            t_l = sorted(t_l)
            self.upper = t_l[int(len(t_l) * self.upp_pro)]
            self.low = t_l[int(len(t_l) * self.low_pro)]
        else:
            print("未完成数据累计")

    def ifUppLow(self):
        if self.upper == None or self.low == None:
            return False
        else:
            return True

    def getUppLow(self):
        return self.upper, self.low


class DateType:
    def __init__(self, keyType:str, keyname:str):
        self.keyType = keyType   #> :A5:....
        self.keyname = keyname   #> LATEST, SP1, BP1
        self.data = None


class Flux:
    def __init__(self, dt:DateType,conn_r):
        self.dt = dt
        self.p_r = conn_r.pipeline(transaction=False)

    def getData(self, cur_ts:int):
        prefix = "MDLD:" + str(cur_ts)
        self.p_r.hget(prefix + self.dt.keyType,self.dt.keyname)
        self.dt.data = self.p_r.execute()[0]
        # print(self.dt.data)
        if self.dt.data == None:
            print("Flux查询数据失效")

    def sendData(self):
        return self.dt.data

    def writeDate(self,cur_ts:int,data:dict):
        prefix = "MDLD:" + str(cur_ts)
        self.p_r.hmset(prefix + self.dt.keyType,data)
        self.p_r.execute()

class UpperLowControl:
    def __init__(self, flux:Flux, work_period, interval,upp_pro:float, low_pro:float):
        self.flux = flux
        self.upplow = UpperLower(interval,upp_pro,low_pro)
        self.work_period = work_period
        self.interval = interval
        self.currentDate = str(time.localtime().tm_year) + "-" + str(time.localtime().tm_mon) + "-" + str(time.localtime().tm_mday) #> "2019-07-11"


    def start(self):
        print("上班啦")
        print("上午：")
        self.operate(self.work_period[0])
        print("下午：")
        self.operate(self.work_period[1])
        print("下班了")

    def operate(self,interval):
        print(self.currentDate + " " + interval[0])
        start_time = time.mktime(time.strptime(self.currentDate + " " + interval[0] , "%Y-%m-%d %H:%M:%S"))
        end_time = time.mktime(time.strptime(self.currentDate + " " + interval[1], "%Y-%m-%d %H:%M:%S"))
        if time.time() > end_time:
            return
        ctime = time.time()
        if ctime < start_time:
            print("等待开盘")
            interval = int(start_time) - time.time()
            if interval > 0:
                time.sleep(interval)
            self.run()

        ctime = time.time()
        if ctime >= start_time and ctime <= end_time:
            time.sleep(int(time.time()) + 1 - time.time())
            while time.time() < end_time:
                self.run()
                time1 = time.time()
                time.sleep(int(time.time()) + 1 - time.time())
                if int(time.time()) - int(time1) != 1:
                    print("miss one")
            self.run()

    def run(self):
        cur_ts = int(time.time())-int(time.mktime(time.strptime(self.currentDate + " " + "00:00:00" , "%Y-%m-%d %H:%M:%S"))) + 3600 - 1 # > 1563785675
        print("当前时间：" + str(cur_ts))
        self.flux.getData(cur_ts)
        self.upplow.appendData(self.flux.sendData())
        self.upplow.computeUpLow()
        if self.upplow.ifUppLow():
            d = dict()
            d[self.upplow.interval_name + "Upper"], d[self.upplow.interval_name + "Low"] = self.upplow.getUppLow()
            print(d)
            self.flux.writeDate(cur_ts,d)
        else:
            print("上限下限未计算")



def main():
    keyType = ":A5:1908M02800"
    dt = DateType(keyType, "LATEST")
    conn_r = redis.Redis(host="192.168.40.134", port = 6379, password="", charset='gb18030',errors="replace",
                             decode_responses=True)

    f = Flux(dt,conn_r)
    # ul = UpperLowControl(f,[["08:30:00","11:30:00"],["16:22:00","17:31:00"]],10,0.9,0.1)
    # ul.start()

    ul = UpperLowControl(f,[["09:30:00","11:30:00"],["13:00:00","15:00:00"]],'A30m',0.9,0.1)
    ul.start()


if __name__ == '__main__':
    main()

