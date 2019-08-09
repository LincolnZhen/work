import redis
import time
from collections import deque
import yaml

class Config:
    def __init__(self, fn_config:str):
        self.config_flist = self.init_flist()
        self.config_optlist = self.init_optlist()
        self.config_slist = self.init_slist(fn_config)
        # print(self.config_flist)
        # print(self.config_optlist)
        # print(self.config_slist)

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

class AverageLine:
    def __init__(self,interval:int):
        self.interval = interval
        self.averageNames = ['A5s','A10s','A15s','A30s','A1m','A3m','A5m','A10m','A15m','A30m','A1h','A2h','A4h','A1d']
        # 1d 和 4h 是相同的
        self.interval_list = [5,10,15,30,60,180,300,600,900,1800,3600,7200,14400,14400]
        self.config = Config('redis_mdld.yaml')
        self.fqueue = {} # type: dict[str, deque]
        self.squeue = {}
        self.opcqueue = {}
        self.oppqueue = {}
        self.vc00queue = {}
        self.vc05queue = {}
        self.vcn5queue = {}
        self.vp00queue = {}
        self.vp05queue = {}
        self.vpn5queue = {}
        self.a5queue = {}
        self.yester_fqueue = {} # type: dict[str, deque]
        self.yester_squeue = {}
        self.yester_opcqueue = {}
        self.yester_oppqueue = {}
        self.yester_vc00queue = {}
        self.yester_vc05queue = {}
        self.yester_vcn5queue = {}
        self.yester_vp00queue = {}
        self.yester_vp05queue = {}
        self.yester_vpn5queue = {}
        self.yester_a5queue = {}
        for key in self.config.config_flist.values():
            self.fqueue[key] = deque(maxlen=self.interval_list[self.interval])
            self.yester_fqueue[key] = {}
        for key in self.config.config_slist:
            self.squeue[key] = deque(maxlen=self.interval_list[self.interval])
            self.yester_squeue[key] = {}
        for key in self.config.config_optlist.items():
            self.opcqueue[key[0]] = deque(maxlen=self.interval_list[self.interval])
            self.yester_opcqueue[key[0]] = {}
        for key in self.config.config_optlist.items():
            self.oppqueue[key[0]] = deque(maxlen=self.interval_list[self.interval])
            self.yester_oppqueue[key[0]] = {}
        for key in self.config.config_optlist.keys():
            self.vc00queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vp00queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vcn5queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vpn5queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vc05queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vp05queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.yester_vc00queue[key[:4]] = {}
            self.yester_vp00queue[key[:4]] = {}
            self.yester_vcn5queue[key[:4]] = {}
            self.yester_vpn5queue[key[:4]] = {}
            self.yester_vc05queue[key[:4]] = {}
            self.yester_vp05queue[key[:4]] = {}
        for pxname, (icode_c, icode_p) in self.config.config_optlist.items():
            self.a5queue[pxname] = deque(maxlen=self.interval_list[self.interval])
            self.yester_a5queue[pxname] = {}

        self.directCompute = False
        self.a5initialYest = False
        self.vp05initialYest = False
        self.vc05initialYest = False
        self.vpn5initialYest = False
        self.vcn5initialYest = False
        self.vp00initialYest = False
        self.vc00initialYest = False
        self.oppinitialYest = False
        self.opcinitialYest = False
        self.sinitialYest = False
        self.finitialYest = False
    # def initialqueue(self):
    #     i = 57600 - self.interval_list[self.interval]
    #     while i < 57600:


    def timeit(func):
        def test(self,key:int,conn_w):
            start = time.perf_counter()
            func(self,key,conn_w)
            end = time.perf_counter()
            print("compute average time used: ", end - start)
        return test

    def averageCompute(self,num_list:list, interval: int):
        res = {self.averageNames[self.interval]+'LATEST':None, self.averageNames[self.interval]+'BP1':None, self.averageNames[self.interval]+'SP1':None}
        latest = 0
        bp1 = 0
        sp1 = 0

        for d in num_list:
            # print(d)
            latest = latest + float(d['LATEST'])
            bp1 = bp1 + float(d['BP1'])
            sp1 = sp1 + float(d['SP1'])
        latest = latest / self.interval_list[interval]
        bp1 = bp1 / self.interval_list[interval]
        sp1 = sp1 / self.interval_list[interval]
        res[self.averageNames[self.interval]+'LATEST'] = latest
        res[self.averageNames[self.interval]+'SP1'] = sp1
        res[self.averageNames[self.interval]+'BP1'] = bp1
        return  res

    def appendFQueue(self,cur_ts:int, conn_w, keyword:str, keyname:str):
        prefix = 'MDLD:'
        # print(conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST','SP1','BP1'))
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST','SP1','BP1')
        print(c_d['LATEST'])
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.fqueue[keyname].append(c_d)
    def appendSQueue(self,cur_ts:int, conn_w, keyword:str, keyname:str):
        prefix = 'MDLD:'
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST','SP1','BP1')
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.squeue[keyname].append(c_d)
    def appendOPcQueue(self,cur_ts:int,conn_w,keyword:str, keyname:str):
        prefix = 'MDLD:'
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST','SP1','BP1')
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.opcqueue[keyname].append(c_d)

    def appendOPpQueue(self,cur_ts:int, conn_w, keyword:str, keyname:str):
        prefix = 'MDLD:'
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST', 'SP1', 'BP1')
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.oppqueue[keyname].append(c_d)

    def appendVQueueC00(self,cur_ts:int, conn_w, keyword:str, keyname:str):
        prefix = 'MDLD:'
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST', 'SP1', 'BP1')
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.vc00queue[keyname].append(c_d)

    def appendVQueueC05(self,cur_ts:int, conn_w, keyword:str, keyname:str):
        prefix = 'MDLD:'
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST', 'SP1', 'BP1')
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.vc05queue[keyname].append(c_d)

    def appendVQueueCN5(self,cur_ts:int, conn_w, keyword:str,keyname:str):
        prefix = 'MDLD:'
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST', 'SP1', 'BP1')
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.vcn5queue[keyname].append(c_d)

    def appendVQueueP00(self,cur_ts:int, conn_w, keyword:str, keyname:str):
        prefix = 'MDLD:'
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST', 'SP1', 'BP1')
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.vp00queue[keyname].append(c_d)

    def appendVQueueP05(self,cur_ts:int, conn_w, keyword:str, keyname:str):
        prefix = 'MDLD:'
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST', 'SP1', 'BP1')
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.vp05queue[keyname].append(c_d)

    def appendVQueuePN5(self,cur_ts:int, conn_w, keyword:str, keyname:str):
        prefix = 'MDLD:'
        c_d = {}
        c_d['LATEST'], c_d['SP1'], c_d['BP1'] = conn_w.hmget(prefix + str(cur_ts) + keyword, 'LATEST', 'SP1', 'BP1')
        if c_d['LATEST'] == None or c_d['SP1'] == None or c_d['BP1'] == None:
            return
        self.vpn5queue[keyname].append(c_d)

    def fillQueue(self,cur_ts,keyword:str,interval:int, keyname:str):
        # print(keyword,keyword[-6:])
        if keyword.startswith(":F:F"):
            return self.fqueue[keyname]
        elif keyword.startswith(':JZ:'):
            return self.squeue[keyname]
        elif keyword.startswith(':OP:C'):
            return self.opcqueue[keyname]
        elif keyword.startswith(':OP:P'):
            return self.oppqueue[keyname]
        elif keyword.startswith(":A5:"):
            return  self.a5queue[keyname]
        elif keyword.startswith(':V:C'):
            if keyword[-6:] == 'MV0000':
                # print("yes")
                return self.vc00queue[keyname]
            elif keyword[-6:] == 'MVN050':
                return  self.vcn5queue[keyname]
            elif keyword[-6:] == 'MV0050':
                return self.vc05queue[keyname]
        elif keyword.startswith(':V:P'):
            if keyword[-6:] == 'MV0000':
                return self.vp00queue[keyname]
            elif keyword[-6:] == 'MVN050':
                return  self.vpn5queue[keyname]
            elif keyword[-6:] == 'MV0050':
                return self.vp05queue[keyname]

    def setDeque(self,queue:deque):
        self.queue = queue

    def operate(self, cur_ts:int, keyword:str, interval:int, conn_w, keyname:str):
        prefix = "MDLD:"
        q = self.fillQueue(cur_ts, keyword, self.interval, keyname)
        # print(prefix+str(cur_ts-1)+keyword,self.averageNames[self.interval] + 'LATEST',prefix+ str(cur_ts-self.interval_list[self.interval])+keyword)
        if not (self.averageNames[self.interval]+'LATEST' in q[len(q) - 2].keys()) or not (self.averageNames[self.interval]+'BP1' in q[len(q) - 2].keys()) or not (self.averageNames[self.interval]+'SP1' in q[len(q) - 2].keys())  :
            # print("AerageLine", prefix+str(cur_ts-1)+keyword, conn_w.hmget(prefix+str(cur_ts-1)+keyword, self.averageNames[self.interval] + 'LATEST'), prefix+ str(cur_ts-self.interval_list[self.interval])+keyword, conn_w.hmget(prefix+ str(cur_ts-self.interval_list[self.interval])+keyword, 'LATEST'))
            d = self.averageCompute(q,interval)
            # start = time.perf_counter()
            conn_w.hmset(prefix + str(cur_ts) + keyword, d)
            # end = time.perf_counter()
            # print(end - start,',')
            q[len(q) - 1][self.averageNames[self.interval] + 'LATEST'] = d[self.averageNames[self.interval] + 'LATEST']
            q[len(q) - 1][self.averageNames[self.interval] + 'SP1'] = d[self.averageNames[self.interval] + 'SP1']
            q[len(q) - 1][self.averageNames[self.interval] + 'BP1'] = d[self.averageNames[self.interval] + 'BP1']
        else:
            # print("compute")
            last_average_latest = q[len(q) - 2][self.averageNames[self.interval]+'LATEST']
            last_average_bp1 = q[len(q) - 2][self.averageNames[self.interval] + 'BP1']
            last_average_sp1 = q[len(q) - 2][self.averageNames[self.interval] + 'SP1']
            # last_average_sp1 = conn_w.hget(prefix + str(cur_ts-1) + keyword, self.averageNames[self.interval] + 'SP1')
            # last_average_bp1 = conn_w.hget(prefix + str(cur_ts-1) + keyword, self.averageNames[self.interval] + 'BP1')
            # last_first_latest = l[0]['LATEST']
            # last_first_bp1 = l[0]['BP1']
            # last_first_sp1 = l[0]['SP1']
            # last_first_latest = conn_w.hget("MDLD:" + str(cur_ts - self.interval_list[self.interval]) + keyword,
            #                                 'LATEST')
            # last_first_sp1 = conn_w.hget("MDLD:" + str(cur_ts - self.interval_list[self.interval]) + keyword,
            #                              'SP1')
            # last_first_bp1 = conn_w.hget("MDLD:" + str(cur_ts - self.interval_list[self.interval]) + keyword,
            #                              'BP1')
            curr_latest = q[len(q) - 1]['LATEST']
            curr_bp1 = q[len(q) - 1]['BP1']
            curr_sp1 = q[len(q) - 1]['SP1']
            # curr_latest = conn_w.hget("MDLD:" + str(cur_ts) +  keyword, 'BP1')
            # curr_sp1 = conn_w.hget("MDLD:" + str(cur_ts) + keyword, 'SP1')
            # curr_bp1 = conn_w.hget("MDLD:" + str(cur_ts) + keyword, 'BP1')
            new_average_latest = float(last_average_latest) * (self.interval_list[self.interval] - 1) + float(curr_latest) * 2
            new_average_latest = new_average_latest / (self.interval_list[self.interval] + 1)
            new_average_sp1 = (float(last_average_sp1) * (self.interval_list[self.interval] - 1) + float(curr_sp1) * 2 ) / (self.interval_list[self.interval] + 1)
            new_average_bp1 = (float(last_average_bp1) * (self.interval_list[self.interval] - 1) + float(curr_bp1) * 2) / ( self.interval_list[self.interval] + 1)
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"] = new_average_latest
            d[self.averageNames[self.interval] + "SP1"] = new_average_sp1
            d[self.averageNames[self.interval] + "BP1"] = new_average_bp1
            # c_d = conn_w.hgetall(prefix + str(cur_ts) + keyword)
            # start = time.perf_counter()
            conn_w.hmset(prefix + str(cur_ts) + keyword, d)
            # end = time.perf_counter()
            # print(end - start,',')
            q[len(q) - 1][self.averageNames[self.interval] + 'LATEST'] = new_average_latest
            q[len(q) - 1][self.averageNames[self.interval] + 'SP1'] = new_average_sp1
            q[len(q) - 1][self.averageNames[self.interval] + 'BP1'] = new_average_bp1

    def fillyesterdayqueue(self,cur_ts,keyword, interval, keyname):
        if keyword.startswith(":F:F"):
            return self.yester_fqueue[keyname]
        elif keyword.startswith(':JZ:'):
            return self.yester_squeue[keyname]
        elif keyword.startswith(':OP:C'):
            return self.yester_opcqueue[keyname]
        elif keyword.startswith(':OP:P'):
            return self.yester_oppqueue[keyname]
        elif keyword.startswith(":A5:"):
            return self.yester_a5queue[keyname]
        elif keyword.startswith(':V:C'):
            if keyword[-6:] == 'MV0000':
                # print("yes")
                return self.yester_vc00queue[keyname]
            elif keyword[-6:] == 'MVN050':
                return  self.yester_vcn5queue[keyname]
            elif keyword[-6:] == 'MV0050':
                return self.yester_vc05queue[keyname]
        elif keyword.startswith(':V:P'):
            if keyword[-6:] == 'MV0000':
                return self.yester_vp00queue[keyname]
            elif keyword[-6:] == 'MVN050':
                return  self.yester_vpn5queue[keyname]
            elif keyword[-6:] == 'MV0050':
                return self.yester_vp05queue[keyname]

    # def getUpperLower(self):


    def operate2(self,cur_ts:int, keyword:str, interval:int, conn_w, keyname:str):
        prefix = "MDLD:"
        l = self.fillyesterdayqueue(cur_ts, keyword, self.interval, keyname)
        l2 = self.fillQueue(cur_ts, keyword,self.interval,keyname)
        # print(prefix+str(cur_ts-1)+keyword,self.averageNames[self.interval] + 'LATEST',prefix+ str(cur_ts-self.interval_list[self.interval])+keyword)
        last_average_latest = l[self.averageNames[self.interval] + 'LATEST']
        last_average_bp1 = l[self.averageNames[self.interval] + 'BP1']
        last_average_sp1 = l[self.averageNames[self.interval] + 'SP1']
        # last_average_sp1 = conn_w.hget(prefix + str(cur_ts-1) + keyword, self.averageNames[self.interval] + 'SP1')
        # last_average_bp1 = conn_w.hget(prefix + str(cur_ts-1) + keyword, self.averageNames[self.interval] + 'BP1')
        # last_first_latest = l[0]['LATEST']
        # last_first_bp1 = l[0]['BP1']
        # last_first_sp1 = l[0]['SP1']
        # last_first_latest = conn_w.hget("MDLD:" + str(cur_ts - self.interval_list[self.interval]) + keyword,
        #                                 'LATEST')
        # last_first_sp1 = conn_w.hget("MDLD:" + str(cur_ts - self.interval_list[self.interval]) + keyword,
        #                              'SP1')
        # last_first_bp1 = conn_w.hget("MDLD:" + str(cur_ts - self.interval_list[self.interval]) + keyword,
        #                              'BP1')
        curr_latest = l2[len(l2) - 1]['LATEST']
        curr_bp1 = l2[len(l2) - 1]['BP1']
        curr_sp1 = l2[len(l2) - 1]['SP1']
        # curr_latest = conn_w.hget("MDLD:" + str(cur_ts) +  keyword, 'BP1')
        # curr_sp1 = conn_w.hget("MDLD:" + str(cur_ts) + keyword, 'SP1')
        # curr_bp1 = conn_w.hget("MDLD:" + str(cur_ts) + keyword, 'BP1')
        print(last_average_latest,curr_latest)
        new_average_latest = float(last_average_latest) * (self.interval_list[self.interval] - 1) + float(curr_latest) * 2
        new_average_latest = new_average_latest / (self.interval_list[self.interval] + 1)
        new_average_sp1 = (float(last_average_sp1) * (self.interval_list[self.interval] - 1) + float(curr_sp1) * 2) / (
                    self.interval_list[self.interval] + 1)
        new_average_bp1 = (float(last_average_bp1) * (self.interval_list[self.interval] - 1) + float(curr_bp1) * 2) / (
                    self.interval_list[self.interval] + 1)
        d = dict()
        d[self.averageNames[self.interval] + "LATEST"] = new_average_latest
        d[self.averageNames[self.interval] + "SP1"] = new_average_sp1
        d[self.averageNames[self.interval] + "BP1"] = new_average_bp1
        # c_d = conn_w.hgetall(prefix + str(cur_ts) + keyword)
        # start = time.perf_counter()
        conn_w.hmset(prefix + str(cur_ts) + keyword, d)
        # end = time.perf_counter()
        # print(end - start,',')
        l[self.averageNames[self.interval] + 'LATEST'] = new_average_latest
        l[self.averageNames[self.interval] + 'SP1'] = new_average_sp1
        l[self.averageNames[self.interval] + 'BP1'] = new_average_bp1
        pass

    def initalYesData(self,conn_r,last_second:int):
        prefix = "MDLD:"
        last_sec = last_second
        ifbreak = False
        for key in self.config.config_flist.values():
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec) + ':F:F' + key, self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_fqueue[key] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.finitialYest = False
        else:
            self.finitialYest = True
        ifbreak = False
        for key in self.config.config_slist:
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec) + ":JZ:" + key, self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_squeue[key] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.sinitialYest = False
        else:
            self.sinitialYest = True
        ifbreak = False
        for key in self.config.config_optlist.items():
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec) + ":OP:C" + key[0], self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_opcqueue[key[0]] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.opcinitialYest = False
        else:
            self.opcinitialYest = True
        ifbreak = False
        for key in self.config.config_optlist.items():
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec) + ":OP:P" + key[0], self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_oppqueue[key[0]] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.oppinitialYest = False
        else:
            self.oppinitialYest = True
        ifbreak = False
        for key in self.config.config_optlist.keys():
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec)+":V:C" + key[:4]+'MV0000', self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_vc00queue[key[:4]] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.vc00initialYest = False
        else:
            self.vc00initialYest = True
        ifbreak = False
        for key in self.config.config_optlist.keys():
            # print(len(self.vcn5queue[key[:4]]))
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec)+":V:P" + key[:4] + "MV0000", self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_vp00queue[key[:4]] = d
            # self.appendVQueueCN5(cur_ts,conn_w,':V:C'+key[:4]+'MVN050',key[:4])
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.vp00initialYest = False
        else:
            self.vp00initialYest = True
        ifbreak = False
        for key in self.config.config_optlist.keys():
            # print(len(self.vcn5queue[key[:4]]))
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec)+":V:C" + key[:4] + "MVN050", self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_vcn5queue[key[:4]] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.vcn5initialYest = False
        else:
            self.vcn5initialYest = True
        ifbreak = False
        for key in self.config.config_optlist.keys():
            # print(len(self.vpn5queue[key[:4]]))
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec)+":V:P" + key[:4] + "MVN050", self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_vpn5queue[key[:4]] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.vpn5initialYest = False
        else:
            self.vpn5initialYest = True
        ifbreak = False
        for key in self.config.config_optlist.keys():
            # print(len(self.vc05queue[key[:4]]))
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec)+":V:C" + key[:4] + "MV0050", self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_vc05queue[key[:4]] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.vc05initialYest = False
        else:
            self.vc05initialYest = True
        ifbreak = False
        for key in self.config.config_optlist.keys():
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec) + ":V:P" + key[:4] + "MV0050", self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_vp05queue[key[:4]] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak = True
                break
        if ifbreak:
            self.vp00initialYest = False
        else:
            self.vp00initialYest = True
        ifbreak = False
        for pxname, (icode_c, icode_p) in self.config.config_optlist.items():
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"], d[self.averageNames[self.interval] + "BP1"], d[self.averageNames[self.interval] + "SP1"] = conn_r.hmget(prefix + str(last_sec) + ":A5:" + pxname, self.averageNames[self.interval] + 'LATEST',self.averageNames[self.interval] + "BP1",self.averageNames[self.interval] + "SP1")
            self.yester_a5queue[pxname] = d
            if d[self.averageNames[self.interval] + "LATEST"] == None or d[self.averageNames[self.interval] + "SP1"] == None or d[self.averageNames[self.interval] + "BP1"] == None:
                ifbreak
                break
        if ifbreak:
            self.a5initialYest = False
        else:
            self.a5initialYest = True
        self.initialYest = True

    @timeit
    def run(self,key:int,conn_w):
        cur_ts = key  # 秒编号
        prefix = "MDLD:" + str(cur_ts)
        p = conn_w.pipeline(transaction=False)
        for key in self.config.config_flist.values():
            if self.finitialYest and len(self.fqueue[key]) > 0:
                self.operate2(cur_ts,":F:F"+key,self.interval, p, keyname=key)
            elif len(self.fqueue[key]) == self.interval_list[self.interval]:
                self.operate(cur_ts,":F:F"+key,self.interval, p, keyname=key)
            else:
                continue
        for key in self.config.config_slist:
            if self.sinitialYest and len(self.squeue[key]) > 0:
                self.operate2(cur_ts,':JZ:'+key,self.interval, p, keyname=key)
            elif len(self.squeue[key]) == self.interval_list[self.interval]:
                self.operate(cur_ts,':JZ:'+key,self.interval, p, keyname=key)
            else:
                continue
        for key in self.config.config_optlist.items():
            if self.opcinitialYest and len(self.opcqueue[key[0]]) > 0:
                self.operate2(cur_ts,':OP:C'+key[0],self.interval, p, keyname=key[0])
            elif len(self.opcqueue[key[0]]) == self.interval_list[self.interval]:
                self.operate(cur_ts,':OP:C'+key[0],self.interval, p, keyname=key[0])
            else:
                continue
        for key in self.config.config_optlist.items():
            # print(len(self.oppqueue[key[0]]))
            # self.appendOPpQueue(cur_ts,conn_w,':OP:P'+ key[0],key[0])
            if self.oppinitialYest and len(self.oppqueue[key[0]]) > 0:
                self.operate2(cur_ts,':OP:P'+key[0],self.interval, p,keyname=key[0])
            # self.appendOPpQueue(cur_ts, conn_w, ':OP:P' + key[0], key[0])
            elif len(self.oppqueue[key[0]]) == self.interval_list[self.interval]:
                self.operate(cur_ts,':OP:P'+key[0],self.interval, p,keyname=key[0])
            else:
                continue
        for key in self.config.config_optlist.keys():
            # print(len(self.vc00queue[key[:4]]))
                # self.appendVQueueC00(cur_ts,conn_w,':V:C'+key[:4]+'MV0000', key[:4])
            if self.vc00initialYest and len(self.vc00queue[key[:4]]) > 0:
                    self.operate2(cur_ts,':V:C'+key[:4]+'MV0000',self.interval, p, keyname=key[:4])
            elif len(self.vc00queue[key[:4]]) == self.interval_list[self.interval]:
                self.operate(cur_ts,':V:C'+key[:4]+'MV0000',self.interval, p, keyname=key[:4])
            else:
                continue
        for key in self.config.config_optlist.keys():
            # print(len(self.vp00queue[key[:4]]))
            if self.vp00initialYest and len(self.vp00queue[key[:4]]) > 0:
                self.operate2(cur_ts,':V:P'+key[:4]+'MV0000',self.interval, p, keyname=key[:4])
            # self.appendVQueueP00(cur_ts,conn_w,':V:P'+key[:4]+'MV0000',key[:4])
            elif len(self.vp00queue[key[:4]]) == self.interval_list[self.interval]:
                self.operate(cur_ts,':V:P'+key[:4]+'MV0000',self.interval, p, keyname=key[:4])
            else:
                continue
        for key in self.config.config_optlist.keys():
            if self.vcn5initialYest and len(self.vcn5queue[key[:4]]) > 0:
                    self.operate2(cur_ts,':V:C'+key[:4]+'MVN050',self.interval, p, keyname=key[:4])
            elif len(self.vcn5queue[key[:4]]) == self.interval_list[self.interval]:
                self.operate(cur_ts,':V:C'+key[:4]+'MVN050',self.interval, p, keyname=key[:4])
            else:
                continue

        for key in self.config.config_optlist.keys():
            if self.vpn5initialYest and len(self.vpn5queue[key[:4]]) > 0:
                self.operate2(cur_ts,':V:P'+key[:4]+'MVN050',self.interval, p, keyname=key[:4])
            # self.appendVQueuePN5(cur_ts,conn_w,':V:P'+key[:4]+'MVN050',key[:4])
            elif len(self.vpn5queue[key[:4]]) == self.interval_list[self.interval]:
                self.operate(cur_ts,':V:P'+key[:4]+'MVN050',self.interval, p, keyname=key[:4])
            else:
                continue

        for key in self.config.config_optlist.keys():
            if self.vc05initialYest and len(self.vc05queue[key[:4]]) > 0:
                self.operate2(cur_ts,':V:C'+key[:4]+'MV0050',self.interval, p, keyname=key[:4])
            elif len(self.vc05queue[key[:4]]) == self.interval_list[self.interval]:
                self.operate(cur_ts,':V:C'+key[:4]+'MV0050',self.interval, p, keyname=key[:4])
            else:
                continue

        for key in self.config.config_optlist.keys():
            if self.vp05initialYest and len(self.vp05queue[key[:4]]) > 0:
                self.operate2(cur_ts,':V:P'+key[:4]+'MV0050',self.interval, p, keyname=key[:4])
            elif len(self.vp05queue[key[:4]]) == self.interval_list[self.interval]:
                self.operate(cur_ts,':V:P'+key[:4]+'MV0050',self.interval, p, keyname=key[:4])
            else:
                continue
        for pxname, (icode_c, icode_p) in self.config.config_optlist.items():
            if self.a5initialYest and len(self.a5queue[pxname]) > 0:
                self.operate2(cur_ts,":A5:"+ pxname,self.interval,p,keyname=pxname)
            elif len(self.a5queue[pxname]) == self.interval_list[self.interval]:
                self.operate(cur_ts,":A5:"+ pxname, self.interval, p, keyname=pxname)
            else:
                continue

        p.execute()
