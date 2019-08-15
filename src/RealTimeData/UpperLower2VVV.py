import redis
import time
import yaml
from collections import deque

class Config:
    def __init__(self, fn_config:str):
        self.config_flist = self.init_flist()
        self.config_optlist = self.init_optlist()
        self.config_slist = self.init_slist(fn_config)
        print(self.config_flist)
        print(self.config_optlist)
        print(self.config_slist)

    def init_flist(self):
        conn = redis.Redis(host="168.36.1.115", port=6380, password="", charset='gb18030', errors='replace',
                           decode_responses=True)
        pre = "qdb:securityex:derivatives:"
        code = "CODE"
        t = ['IC', 'IF', 'IH']
        d = dict()
        for i in t:
            for j in range(1, 5):
                d[i + "0" + str(j)] = conn.get(pre + i + "0" + str(j) + ":CODE")
        return d

    def init_optlist(self):
        conn = redis.Redis(host="168.36.1.170", port=6379, password="", charset='gb18030', errors='replace',
                           decode_responses=True)
        keys = conn.keys()

        re = dict()
        num = 0
        for key in keys:
            if key.startswith("OPLST:01"):
                num = num + 1
                # print(key)
                code = conn.hget(key, 'InstrumentCode')
                re_key = code[7:]
                if not re_key in re.keys():
                    re[re_key] = ['', '']
                if code[6] == 'P':
                    re[re_key][1] = conn.hget(key, 'InstrumentID')
                if code[6] == 'C':
                    re[re_key][0] = conn.hget(key, 'InstrumentID')
        # print(num)
        return re

    def init_slist(self,filename: str):
        with open(filename, 'r') as file:
            cont = file.read()
            res = yaml.load(cont, Loader=yaml.FullLoader)
            return res['config']['slist']

class UpperLower:
    def __init__(self, interval_name:str, upp_pro:float, low_pro:float):
        self.interval_dict = {"A30m":1800,'A10s':10,'A5m':300}
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
        elif len(self.data_queue) > 0:
            t_l = list(self.data_queue)
            t_l = sorted(t_l)
            self.upper = t_l[int(len(t_l)* self.upp_pro) ]
            self.low = t_l[int(len(t_l)* self.low_pro) ]
            print("未完成数据累计，显示当前数据")
        else:
            print("Deque中没有数据")

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
    def __init__(self, conn_r):
        self.p_r = conn_r.pipeline(transaction=False)

    def getBatchData(self,dt_list:list,cur_ts:int):
        prefix = "MDLD:" + str(cur_ts)
        for dt in dt_list:
            # print(prefix+dt.keyType)
            self.p_r.hmget(prefix+dt.keyType,dt.keyname)
        return self.p_r.execute()

    def writeBatchData(self,dt_list:list,cur_ts:int):
        prefix = "MDLD:" + str(cur_ts)
        for dt in dt_list:
            self.p_r.hmset(prefix + dt.keyType, dt.data)
        self.p_r.execute()

    def getData(self, cur_ts:int,dt:DateType):
        prefix = "MDLD:" + str(cur_ts)
        self.dt = dt
        self.p_r.hget(prefix + self.dt.keyType,self.dt.keyname)
        self.dt.data = self.p_r.execute()[0]
        print(self.dt.data)
        if self.dt.data == None:
            print("Flux查询数据失效")
        return dt.data

    def writeData(self,cur_ts:int,data:dict,dt:DateType):
        prefix = "MDLD:" + str(cur_ts)
        self.p_r.hmset(prefix + self.dt.keyType,data)
        self.p_r.execute()

    def sendWriteOrder(self,cur_ts:int, dt:DateType,data:dict):
        prefix ="MDLD:" + str(cur_ts)
        self.p_r.hmset(prefix + dt.keyType,data)

    def fluxExecute(self):
        self.p_r.execute()

class UpperLowControl:
    def __init__(self, flux:Flux, work_period, interval,upp_pro:float, low_pro:float, computeKind:str):
        '''
        f,[["09:30:00","11:30:01"],["13:00:00","15:00:01"]],'A30m',0.9,0.1
        :param flux: f
        :param work_period: [["09:30:00","11:30:01"],["13:00:00","15:00:01"]]
        :param interval: 'A30m'
        :param upp_pro: 0.9
        :param low_pro: 0.1
        :param computeKind: A5 0|1|2|3, A13 IC01-IH04
        '''
        self.flux = flux
        self.upplow = UpperLower(interval,upp_pro,low_pro)
        self.work_period = work_period
        self.interval = interval
        self.currentDate = str(time.localtime().tm_year) + "-" + str(time.localtime().tm_mon) + "-" + str(time.localtime().tm_mday) #> "2019-07-11"
        self.config = Config("redis_mdld.yaml")
        self.computeArg1 = computeKind.split(" ")[0]
        self.computeArg2 = computeKind.split(" ")[1]
        if self.computeArg1 == "A5":
            self.getReadyForA5(int(self.computeArg2))
        elif self.computeArg1 == "A13":
            self.getReadyForA13()


    def getReadyForA5(self, mon_index:int):
        '''

        :param mon_index:
        :return:
        '''
        date_list = set()
        for pxname, (icode_c, icode_p) in self.config.config_optlist.items():
            date_list.add(pxname[:4])  #> “1909”
        date_list = list(date_list)
        date_list.sort()  #> ['1908', '1909', '1912', '2003']
        self.oplist = list()

        for pxname, (icode_c, icode_p) in self.config.config_optlist.items():
            if pxname.startswith(date_list[mon_index]):
                self.oplist.append(pxname) #> "02750")

        # PM:当月 NM:下月 NS:下季 SS:隔季
        symbol = ['PM','NM','NS','SS']

        self.writeFlux_key = ":A5:" + symbol[mon_index] + ":PJ"
        self.oplist = sorted(self.oplist,key = lambda x: int(x[-5:] * 10))
        print(self.oplist)

    def getReadyForA13(self):
        self.writeFlux_key = ":A13:" + self.computeArg2


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
        cur_ts = int(time.time())-int(time.mktime(time.strptime(self.currentDate + " " + "00:00:00" , "%Y-%m-%d %H:%M:%S"))) + 3 * 3600 - 1 # > 1563785675
        print("当前时间：" + str(cur_ts))
        dt = self.getData(cur_ts)
        # print("run",data)
        self.upplow.appendData(dt.data)
        self.upplow.computeUpLow()
        if self.upplow.ifUppLow():
            d = dict()
            d[self.upplow.interval_name + "Upper"], d[self.upplow.interval_name + "Low"] = self.upplow.getUppLow()
            print(d)
            dt.keyType = self.writeFlux_key #> 平价
            self.flux.writeData(cur_ts,d,dt)
        else:
            print("上限下限未计算")

    def getData(self,cur_ts:int):
        if self.computeArg1 == "A5":
            return self.getPingJiaContract(cur_ts)
        elif self.computeArg1 == "A13":
            return self.getDataForA13(cur_ts)

        pass

    def getDataForA13(self,cur_ts:int):
        dt = DateType(":A13:"+self.computeArg2,'L')
        data = self.flux.getData(cur_ts,dt)
        return dt


    def getPe(self,cur_ts:int):
        pe = self.flux.getData(cur_ts,DateType(":A5:" + self.oplist[0],"Pe"))
        # print(pe)
        return  pe

    def getPingJiaContract(self,cur_ts:int):
        i = 0
        pe = self.getPe(cur_ts)
        if pe == None:
            return DateType("","LATEST")
        else:
            pe = float(pe)
        while i < len(self.oplist):
            if pe < (int(self.oplist[i][-5:]) * 10):
                break
            else:
                i = i + 1
        if abs(pe - int(self.oplist[i-1][-5:])*10) >= abs(pe - int(self.oplist[i][-5:])*10):
            dt = DateType(":A5:" + self.oplist[i],'LATEST')
            data = self.flux.getData(cur_ts, dt)
            print("当前平价合约为", self.oplist[i])
            return dt
        else:
            dt = DateType(":A5:" + self.oplist[i - 1], 'LATEST')
            data= self.flux.getData(cur_ts, dt)
            print("当前平价合约为", self.oplist[i-1])
            return dt








def main():
    conn_r = redis.Redis(host="168.36.1.116", port = 6379, password="", charset='gb18030',errors="replace",
                             decode_responses=True)

    f = Flux(conn_r)
    # ul = UpperLowControl(f,[["08:30:00","11:30:00"],["16:22:00","17:31:00"]],10,0.9,0.1)
    # ul.start()

    ul = UpperLowControl(f,[["09:30:00","11:30:01"],["13:00:00","15:00:01"]],'A5m',0.9,0.1,"A13 IH01")
    ul.start()


if __name__ == '__main__':
    main()

