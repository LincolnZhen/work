import numpy
import redis
import yaml
import time
from multiprocessing import Process


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

class CalcAvg:
    def __init__(self, interval:int):
        self.interval = interval    #> 12, 26
        self.sum = 0
        self.data_cnt = 0
        self.currentAverage = None
        self.currentdata = None

    def on_feed(self, data:float):
        self.currentdata = data

    def computeAerage(self):
        if self.currentAverage != None and self.currentdata != None:
            # print("compute")
            self.currentAverage = self.currentAverage * (self.interval - 1) + self.currentdata * 2
            self.currentAverage = self.currentAverage / (self.interval + 1)
        else:
            self.sum = self.sum + self.currentdata
            self.data_cnt = self.data_cnt + 1
            if self.data_cnt == self.interval:
                self.currentAverage = self.sum / self.interval

    def setCurrentAverage(self, data:float):
        if data != None:
            self.currentAverage = float(data)
        else:
            self.currentAverage = None

    def reborn(self):
        self.sum = 0
        self.data_cnt = 0
        self.currentAverage = None
        self.currentdata = None

class MACD:
    def __init__(self):
        """

        :param interval: 1s, 5s, 10s
        """
        self.avg12 = CalcAvg(12)
        self.avg26 = CalcAvg(26)
        self.av9_dif = CalcAvg(9)
        self.DIF = None
        self.DEA = None
        self.DIF_lim = 9
        self.MACD = None

    def run(self, data:float):
        if data == None:
            # print("MACD接收数据无效")
            self.avg12.reborn()
            self.avg26.reborn()
            self.DIF = None
            self.MACD = None
            self.DEA = None
            self.av9_dif.reborn()
            return
        else:
            data = float(data)
        self.avg12.on_feed(data)
        self.avg26.on_feed(data)
        self.avg12.computeAerage()
        self.avg26.computeAerage()
        if self.avg12.currentAverage != None and self.avg26.currentAverage != None:
            self.DIF = self.avg12.currentAverage - self.avg26.currentAverage
            self.av9_dif.on_feed(self.DIF)
            self.av9_dif.computeAerage()
            if self.av9_dif.currentAverage != None:
                self.DEA = self.av9_dif.currentAverage
        self._computeMACD()

    def _computeMACD(self):
        if self.DIF != None and self.DEA != None:
            self.MACD = (self.DIF - self.DEA) * 2
        else:
            self.MACD = None

    def ifDIF(self):
        if self.DIF != None:
            return True
        else:
            return False
    def ifDEA(self):
        if self.DEA != None:
            return True
        else:
            return False

    def ifMACD(self):
        if self.MACD != None:
            return True
        else:
            return False

    def getEMA12(self):
        return self.avg12.currentAverage

    def getEMA26(self):
        return self.avg26.currentAverage

    def setMACD(self,data:float):
        if data != None:
            self.MACD = float(data)

        else:
            self.MACD = None

    def setDIF(self,data:float):
        if data != None:
            self.DIF = float(data)
        else:
            self.DIF = None

    def setDEA(self,data:float):
        if data != None:
            self.DEA = float(data)
        else:
            self.DEA = None

    def setAvg12(self,data:float):
        self.avg12.setCurrentAverage(data)

    def setAvg26(self,data:float):
        self.avg26.setCurrentAverage(data)

    def setDIFAvg9(self,data:float):
        self.av9_dif.setCurrentAverage(data)

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

    def getBatchData2(self,dt_list:list,cur_ts:int,prefix:str):
        prefix = prefix + str(cur_ts)
        for dt in dt_list:
            # print(prefix+dt.keyType)
            print(prefix+dt.keyType)
            self.p_r.hget(prefix+dt.keyType,dt.keyname)
        return self.p_r.execute()

    def writeBatchData(self,dt_list:list,cur_ts:int):
        prefix = "MDLD:" + str(cur_ts)
        for dt in dt_list:
            self.p_r.hmset(prefix + dt.keyType, dt.data)
        self.p_r.execute()

    def getData(self, cur_ts:int):
        prefix = "MDLD:" + str(cur_ts)
        self.p_r.hget(prefix + self.dt.keyType,self.dt.keyname)
        self.dt.data = self.p_r.execute()[0]
        print(self.dt.data)
        if self.dt.data == None:
            print("Flux查询数据失效")

    def sendWriteOrder(self,cur_ts:int, dt:DateType,data:dict):
        prefix ="MDLD:" + str(cur_ts)
        self.p_r.hmset(prefix + dt.keyType,data)

    def fluxExecute(self):
        return self.p_r.execute()

class MACDControl:
    def __init__(self,work_period, interval:int):
        self.work_period = work_period
        self.interval = interval
        self.currentDate = str(time.localtime().tm_year) + "-" + str(time.localtime().tm_mon) + "-" + str(time.localtime().tm_mday) #> "2019-07-11"
        self.config = Config("redis_mdld.yaml")
        conn_r = redis.Redis(host="168.36.1.116", port=6379, password="", charset='gb18030', errors="replace",
                             decode_responses=True)
        self.flux = Flux(conn_r)
        self.dt_list = self.fillDTList()
        self.macd_list = list()
        for i in range(len(self.dt_list)):
            m = MACD()
            self.macd_list.append(m)
        self.interval_dict = {1:'A1s', 5:'A5s',15:"A15s", 30:"A30s",60:"A1m",180:"A3m",300:"A5m",600:"A10m",900:"A15m"}
        self.interval_name = self.interval_dict[self.interval]


    def start(self):
        print("上班啦")
        print("上午：")
        self.operate(self.work_period[0],64800)
        print("下午：")
        self.operate(self.work_period[1],52200)
        print("下班了")

    def fillLastData(self,last_time:int):
        prefix = "MDLD:" + str(last_time)
        keynames = ["LATEST",'BP1','SP1']
        for i in range(len(self.dt_list)):
            self.flux.p_r.hget(prefix + self.dt_list[i].keyType,self.interval_name + self.dt_list[i].keyname + "MACD")
            self.flux.p_r.hget(prefix + self.dt_list[i].keyType, self.interval_name + self.dt_list[i].keyname + "DEA")
            self.flux.p_r.hget(prefix + self.dt_list[i].keyType, self.interval_name + self.dt_list[i].keyname + "DIF")
            self.flux.p_r.hget(prefix + self.dt_list[i].keyType,self.interval_name + self.dt_list[i].keyname + "EMA12")
            self.flux.p_r.hget(prefix + self.dt_list[i].keyType, self.interval_name + self.dt_list[i].keyname + "EMA26")
        res = self.flux.fluxExecute()

        num = 5
        # 5代表有五个数据：MACD,DEA,DIF,EMA12,EMA26
        if len(res) != len(self.macd_list) * num:
            print("res与listmacd长度不等",len(res),len(self.macd_list))
            return

        for i in range(len(self.macd_list)):
            self.macd_list[i].setMACD(res[i*num])
            self.macd_list[i].setDEA(res[i*num + 1])
            self.macd_list[i].setDIF(res[i*num + 2])
            self.macd_list[i].setAvg12(res[i*num + 3])
            self.macd_list[i].setAvg26(res[i*num + 4])
            self.macd_list[i].setDIFAvg9(res[i*num + 1])
        print("hhh")

    def operate(self,interval,last_time:int):
        print(self.currentDate + " " + interval[0])
        start_time = time.mktime(time.strptime(self.currentDate + " " + interval[0] , "%Y-%m-%d %H:%M:%S"))
        end_time = time.mktime(time.strptime(self.currentDate + " " + interval[1], "%Y-%m-%d %H:%M:%S"))
        if time.time() > end_time:
            return
        ctime = time.time()
        if ctime < start_time:
            print("等待开盘")
            self.fillLastData(last_time)
            interval = int(start_time) - time.time()
            if interval > 0:
                time.sleep(interval)
            ctime = time.time()
            self.run()
            time.sleep(int(ctime) + self.interval - time.time())

        ctime = time.time()
        # self.fillDTList(last_time - 1)
        if ctime >= start_time and ctime <= end_time:
            # time.sleep(int(time.time()) + 1 - time.time())
            # print("hhh")
            while time.time() < end_time:
                tt = int(time.time())
                print(tt)
                if (tt - 1) % self.interval == 0:
                    self.run()
                # time1 = time.time()
                time.sleep(int(time.time()) + 1 - time.time())
                # if int(time.time()) - int(time1) != 1:
                #     print("miss one")
            self.run()

    def run(self):
        cur_ts = int(time.time())-int(time.mktime(time.strptime(self.currentDate + " " + "00:00:00" , "%Y-%m-%d %H:%M:%S"))) + 3600*3 - 1 # > 1563785675
        res = self.flux.getBatchData(self.dt_list,cur_ts)
        # print(len(res))
        self.dispatch(res,self.dt_list)
        print("当前时间：" + str(cur_ts))
        prefix ="MDLD:" + str(cur_ts)
        # print(len(self.macd_list))
        for i in range(len(self.macd_list)):
            # print(i)
            self.macd_list[i].run(self.dt_list[i].data[self.dt_list[i].keyname])
            # if self.macd_list[i].ifDIF():
            #     print(self.dt_list[i].keyType + " "+ self.dt_list[i].keyname + " " + "DIF:", self.macd_list[i].DIF)
            # else:
            #     # print(self.dt_list[i].keyType + " "+ self.dt_list[i].keyname + " " + "DIF未计算")
            # if self.macd_list[i].ifDEA():
            #     print(self.dt_list[i].keyType + " " + self.dt_list[i].keyname + " " + "DEA:", self.macd_list[i].DEA)
            # else:
            #     print(self.dt_list[i].keyType + " "+ self.dt_list[i].keyname + " " +"DEA未计算")
            if self.macd_list[i].ifMACD():
                # print("h")
                # print("yes",self.dt_list[i].keyType,self.dt_list[i].keyname)
                # print(self.macd_list[i].avg12.data_cnt,self.macd_list[i].avg26.data_cnt)
                # print("yes",prefix + self.dt_list[i].keyType, {self.interval_name + list(self.dt_list[i].data.keys())[0] + "MACD": self.macd_list[i].MACD,self.interval_name + list(self.dt_list[i].data.keys())[0] + "DEA": self.macd_list[i].DEA,self.interval_name +list(self.dt_list[i].data.keys())[0] + "DIF": self.macd_list[i].DIF})
                # print(self.dt_list[i].keyType + " " + self.dt_list[i].keyname + " " + "MACD:", self.macd_list[i].MACD)
                self.flux.p_r.hmset(prefix + self.dt_list[i].keyType, {self.interval_name + list(self.dt_list[i].data.keys())[0] + "MACD": self.macd_list[i].MACD,
                                                                       self.interval_name + list(self.dt_list[i].data.keys())[0] + "DEA": self.macd_list[i].DEA,
                                                                       self.interval_name + list(self.dt_list[i].data.keys())[0] + "DIF": self.macd_list[i].DIF,
                                                                       self.interval_name + list(self.dt_list[i].data.keys())[0] + "EMA12": self.macd_list[i].getEMA12(),
                                                                       self.interval_name + list(self.dt_list[i].data.keys())[0] + "EMA26": self.macd_list[i].getEMA26()} )
                print(self.dt_list[i].keyType, self.macd_list[i].avg26.currentAverage,self.macd_list[i].avg12.currentAverage,self.macd_list[i].DIF)
                # self.flux.sendWriteOrder(cur_ts,self.dt_list[i],{self.interval_name + self.dt_list[i].keyname + "MACD": self.macd_list[i].MACD})
                # self.flux.sendWriteOrder(cur_ts,self.dt_list[i],{self.interval_name + self.dt_list[i].keyname + "DEA": self.macd_list[i].DEA})
                # self.flux.sendWriteOrder(cur_ts,self.dt_list[i],{self.interval_name + self.dt_list[i].keyname + "DIF": self.macd_list[i].DIF})
                # self.flux.fluxExecute()
            else:
                continue
                # print(self.dt_list[i].keyType + " "+ self.dt_list[i].keyname + " " + "MACD未计算")
        self.flux.p_r.execute()

    def run2(self,cur_ts):
        cur_ts = cur_ts
        res = self.flux.getBatchData(self.dt_list,cur_ts)
        # print(len(res))
        self.dispatch(res,self.dt_list)
        print("当前时间：" + str(cur_ts))
        prefix ="MDLD:" + str(cur_ts)
        # print(len(self.macd_list))
        for i in range(len(self.macd_list)):
            # print(i)
            self.macd_list[i].run(self.dt_list[i].data[self.dt_list[i].keyname])
            # if self.macd_list[i].ifDIF():
            #     print(self.dt_list[i].keyType + " "+ self.dt_list[i].keyname + " " + "DIF:", self.macd_list[i].DIF)
            # else:
            #     # print(self.dt_list[i].keyType + " "+ self.dt_list[i].keyname + " " + "DIF未计算")
            # if self.macd_list[i].ifDEA():
            #     print(self.dt_list[i].keyType + " " + self.dt_list[i].keyname + " " + "DEA:", self.macd_list[i].DEA)
            # else:
            #     print(self.dt_list[i].keyType + " "+ self.dt_list[i].keyname + " " +"DEA未计算")
            if self.macd_list[i].ifMACD():
                # print("h")
                # print("yes",self.dt_list[i].keyType,self.dt_list[i].keyname)
                # print(self.macd_list[i].avg12.data_cnt,self.macd_list[i].avg26.data_cnt)
                # print("yes",prefix + self.dt_list[i].keyType, {self.interval_name + list(self.dt_list[i].data.keys())[0] + "MACD": self.macd_list[i].MACD,self.interval_name + list(self.dt_list[i].data.keys())[0] + "DEA": self.macd_list[i].DEA,self.interval_name +list(self.dt_list[i].data.keys())[0] + "DIF": self.macd_list[i].DIF})
                # print(self.dt_list[i].keyType + " " + self.dt_list[i].keyname + " " + "MACD:", self.macd_list[i].MACD)
                self.flux.p_r.hmset(prefix + self.dt_list[i].keyType, {self.interval_name + list(self.dt_list[i].data.keys())[0] + "MACD": self.macd_list[i].MACD,
                                                                       self.interval_name + list(self.dt_list[i].data.keys())[0] + "DEA": self.macd_list[i].DEA,
                                                                       self.interval_name + list(self.dt_list[i].data.keys())[0] + "DIF": self.macd_list[i].DIF,
                                                                       self.interval_name + list(self.dt_list[i].data.keys())[0] + "EMA12": self.macd_list[i].getEMA12(),
                                                                       self.interval_name + list(self.dt_list[i].data.keys())[0] + "EMA26": self.macd_list[i].getEMA26()} )
                print(self.dt_list[i].keyType, self.macd_list[i].avg26.currentAverage,self.macd_list[i].avg12.currentAverage,self.macd_list[i].DIF)
                # self.flux.sendWriteOrder(cur_ts,self.dt_list[i],{self.interval_name + self.dt_list[i].keyname + "MACD": self.macd_list[i].MACD})
                # self.flux.sendWriteOrder(cur_ts,self.dt_list[i],{self.interval_name + self.dt_list[i].keyname + "DEA": self.macd_list[i].DEA})
                # self.flux.sendWriteOrder(cur_ts,self.dt_list[i],{self.interval_name + self.dt_list[i].keyname + "DIF": self.macd_list[i].DIF})
                # self.flux.fluxExecute()
            else:
                continue
                # print(self.dt_list[i].keyType + " "+ self.dt_list[i].keyname + " " + "MACD未计算")
        self.flux.p_r.execute()


    def fillDTList(self):
        config = self.config
        dt_list = list()
        latest = "LATEST"
        bp1 = "BP1"
        sp1 = "SP1"

        for keyname, key in config.config_flist.items():
            dt1 = DateType(":F:F" + key,latest)
            dt2 = DateType(":F:F" + key,bp1)
            dt3 = DateType(":F:F" + key,sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)

        for key in config.config_slist:
            dt1 = DateType(":S:" + key, latest)
            dt2 = DateType(":S:" + key, bp1)
            dt3 = DateType(":S:" + key, sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)
            dt1 = DateType(":JZ:" + key, latest)
            dt2 = DateType(":JZ:" + key, bp1)
            dt3 = DateType(":JZ:" + key, sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)

        for pxname, (icode_c, icode_p) in config.config_optlist.items():
            dt1 = DateType(":OP:C" + pxname, latest)
            dt2 = DateType(":OP:C" + pxname, bp1)
            dt3 = DateType(":OP:C" + pxname, sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)
            dt1 = DateType(":OP:P" + pxname, latest)
            dt2 = DateType(":OP:P" + pxname, bp1)
            dt3 = DateType(":OP:P" + pxname, sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)
            dt1 = DateType(":A5:" + pxname, latest)
            dt2 = DateType(":A5:" + pxname, bp1)
            dt3 = DateType(":A5:" + pxname, sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)
            dt1 = DateType(":V:C" + pxname[:4]+"MV" + "0000", latest)
            dt2 = DateType(":V:C" + pxname[:4]+ "MV" + "0000", bp1)
            dt3 = DateType(":V:C" + pxname[:4]+ "MV" + "0000", sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)
            dt1 = DateType(":V:C" + pxname[:4] + "MV" + "0050", latest)
            dt2 = DateType(":V:C" + pxname[:4] + "MV" + "0050", bp1)
            dt3 = DateType(":V:C" + pxname[:4] + "MV" + "0050", sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)
            dt1 = DateType(":V:C" + pxname[:4] + "MV" + "N050", latest)
            dt2 = DateType(":V:C" + pxname[:4] + "MV" + "N050", bp1)
            dt3 = DateType(":V:C" + pxname[:4] + "MV" + "N050", sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)
            dt1 = DateType(":V:P" + pxname[:4]+"MV" + "0000", latest)
            dt2 = DateType(":V:P" + pxname[:4]+ "MV" + "0000", bp1)
            dt3 = DateType(":V:P" + pxname[:4]+ "MV" + "0000", sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)
            dt1 = DateType(":V:P" + pxname[:4] + "MV" + "0050", latest)
            dt2 = DateType(":V:P" + pxname[:4] + "MV" + "0050", bp1)
            dt3 = DateType(":V:P" + pxname[:4] + "MV" + "0050", sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)
            dt1 = DateType(":V:P" + pxname[:4] + "MV" + "N050", latest)
            dt2 = DateType(":V:P" + pxname[:4] + "MV" + "N050", bp1)
            dt3 = DateType(":V:P" + pxname[:4] + "MV" + "N050", sp1)
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)

        for key in config.config_flist.keys():
            dt1 = DateType(":A13:"+ key,"L")
            dt2 = DateType(":A13:" + key, "B")
            dt3 = DateType(":A13:" + key, "S")
            dt_list.append(dt1)
            dt_list.append(dt2)
            dt_list.append(dt3)

        return dt_list

    def dispatch(self,result, dt_list):
        config = self.config
        i = 0
        if len(result) != len(dt_list):
            return
        for keyname, key in config.config_flist.items():
            dt_list[i].data = {dt_list[i].keyname:result[i][0]}
            dt_list[i+1].data = {dt_list[i+1].keyname:result[i+1][0]}
            dt_list[i+2].data = {dt_list[i+2].keyname:result[i+2][0]}
            i = i + 3

        for key in config.config_slist:
            dt_list[i].data = {dt_list[i].keyname:result[i][0]}
            dt_list[i+1].data = {dt_list[i+1].keyname:result[i+1][0]}
            dt_list[i+2].data = {dt_list[i+2].keyname:result[i+2][0]}
            i = i + 3
            dt_list[i].data = {dt_list[i].keyname:result[i][0]}
            dt_list[i+1].data = {dt_list[i+1].keyname:result[i+1][0]}
            dt_list[i+2].data = {dt_list[i+2].keyname:result[i+2][0]}
            i = i + 3

        for pxname, (icode_c, icode_p) in config.config_optlist.items():
            dt_list[i].data = {dt_list[i].keyname:result[i][0]}
            dt_list[i+1].data = {dt_list[i+1].keyname:result[i+1][0]}
            dt_list[i+2].data = {dt_list[i+2].keyname:result[i+2][0]}
            i = i + 3
            dt_list[i].data = {dt_list[i].keyname:result[i][0]}
            dt_list[i+1].data = {dt_list[i+1].keyname:result[i+1][0]}
            dt_list[i+2].data = {dt_list[i+2].keyname:result[i+2][0]}
            i = i + 3
            dt_list[i].data = {dt_list[i].keyname: result[i][0]}
            dt_list[i + 1].data = {dt_list[i+1].keyname: result[i + 1][0]}
            dt_list[i + 2].data = {dt_list[i+2].keyname: result[i + 2][0]}
            i = i + 3
            dt_list[i].data = {dt_list[i].keyname: result[i][0]}
            dt_list[i + 1].data = {dt_list[i+1].keyname: result[i + 1][0]}
            dt_list[i + 2].data = {dt_list[i+2].keyname: result[i + 2][0]}
            i = i + 3
            dt_list[i].data = {dt_list[i].keyname: result[i][0]}
            dt_list[i + 1].data = {dt_list[i+1].keyname: result[i + 1][0]}
            dt_list[i + 2].data = {dt_list[i+2].keyname: result[i + 2][0]}
            i = i + 3
            dt_list[i].data = {dt_list[i].keyname: result[i][0]}
            dt_list[i + 1].data = {dt_list[i+1].keyname: result[i + 1][0]}
            dt_list[i + 2].data = {dt_list[i+2].keyname: result[i + 2][0]}
            i = i + 3
            dt_list[i].data = {dt_list[i].keyname: result[i][0]}
            dt_list[i + 1].data = {dt_list[i+1].keyname: result[i + 1][0]}
            dt_list[i + 2].data = {dt_list[i+2].keyname: result[i + 2][0]}
            i = i + 3
            dt_list[i].data = {dt_list[i].keyname: result[i][0]}
            dt_list[i + 1].data = {dt_list[i+1].keyname: result[i + 1][0]}
            dt_list[i + 2].data = {dt_list[i+2].keyname: result[i + 2][0]}
            i = i + 3
            dt_list[i].data = {dt_list[i].keyname: result[i][0]}
            dt_list[i + 1].data = {dt_list[i+1].keyname: result[i + 1][0]}
            dt_list[i + 2].data = {dt_list[i+2].keyname: result[i + 2][0]}
            i = i + 3


        for key in config.config_flist.keys():
            dt_list[i].data = {dt_list[i].keyname: result[i][0]}
            dt_list[i + 1].data = {dt_list[i+1].keyname: result[i + 1][0]}
            dt_list[i + 2].data = {dt_list[i+2].keyname: result[i + 2][0]}
            i = i + 3

        # print("dispatch", i)

    def restart(self, ctime, start_time):
        if ctime - start_time < 1:
            return
        end_time = int(ctime) - int(
            time.mktime(time.strptime(self.currentDate + " " + "00:00:00", "%Y-%m-%d %H:%M:%S"))) + 3600 * 3
        start_time = start_time - int(
            time.mktime(time.strptime(self.currentDate + " " + "00:00:00", "%Y-%m-%d %H:%M:%S"))) + 3600 * 3
        for cur_ts in range(start_time,end_time,self.interval):
            self.run2(cur_ts)

    def AfterCloseStart(self,last_sec,start_sec,end_sec):
        self.fillLastData(last_sec)
        for cur_ts in range(start_sec,end_sec+1,self.interval):
            self.run2(cur_ts)

class MyProcess(Process):
    def __init__(self, macd):
        super().__init__()
        self.macd = macd

    def run(self):
        # self.macd.start()
        print("hh")


if __name__ == '__main__':

    # conn_r = redis.Redis(host="192.168.40.134", port = 6379, password="", charset='gb18030',errors="replace",
    #                          decode_responses=True)

    macdC = MACDControl([["11:03:00","11:30:01"],["13:44:50","16:50:00"]],5)
    # macdC.start()
    macdC.start()
    # macdC2 = MACDControl([["09:30:00","11:30:00"],["13:00:00","16:50:00"]],60)
    # macdC2.start()
    # p1 = MyProcess(macdC)
    # p2 = MyProcess(macdC2)
    # p1.start()
    # p1.join()
    # p2.start()
    # p2.join()
    # print("over")


