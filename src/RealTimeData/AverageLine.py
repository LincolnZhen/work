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

        for key in self.config.config_flist.values():
            self.fqueue[key] = deque(maxlen=self.interval_list[self.interval])
        for key in self.config.config_slist:
            self.squeue[key] = deque(maxlen=self.interval_list[self.interval])
        for key in self.config.config_optlist.items():
            self.opcqueue[key[0]] = deque(maxlen=self.interval_list[self.interval])
        for key in self.config.config_optlist.items():
            self.oppqueue[key[0]] = deque(maxlen=self.interval_list[self.interval])
        for key in self.config.config_optlist.keys():
            self.vc00queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vp00queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vcn5queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vpn5queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vc05queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
            self.vp05queue[key[:4]] = deque(maxlen=self.interval_list[self.interval])
        self.directCompute = False

    def timeit(func):
        def test(self,num_list,interval):
            start = time.time()
            func(self,num_list,interval)
            end = time.time()
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
        l = self.fillQueue(cur_ts, keyword, self.interval, keyname)
        # print(prefix+str(cur_ts-1)+keyword,self.averageNames[self.interval] + 'LATEST',prefix+ str(cur_ts-self.interval_list[self.interval])+keyword)
        if not (str(self.interval_list[self.interval])+'LATEST' in l[len(l) - 1].keys()) or not (str(self.interval_list[self.interval])+'BP1' in l[len(l) - 1].keys()) or not (str(self.interval_list[self.interval])+'SP1' in l[len(l) - 1].keys())  :
            # print("AerageLine", prefix+str(cur_ts-1)+keyword, conn_w.hmget(prefix+str(cur_ts-1)+keyword, self.averageNames[self.interval] + 'LATEST'), prefix+ str(cur_ts-self.interval_list[self.interval])+keyword, conn_w.hmget(prefix+ str(cur_ts-self.interval_list[self.interval])+keyword, 'LATEST'))
            d = self.averageCompute(l,interval)
            conn_w.hmset(prefix + str(cur_ts) + keyword, d)
        else:
            # print("compute")
            last_average_latest = l[len(l) - 2][self.interval_list[self.interval]+'LATEST']
            last_average_bp1 = l[len(l) - 2][self.interval_list[self.interval] + 'BP1']
            last_average_sp1 = l[len(l) - 2][self.interval_list[self.interval] + 'SP1']
            # last_average_sp1 = conn_w.hget(prefix + str(cur_ts-1) + keyword, self.averageNames[self.interval] + 'SP1')
            # last_average_bp1 = conn_w.hget(prefix + str(cur_ts-1) + keyword, self.averageNames[self.interval] + 'BP1')
            last_first_latest = l[0]['LATEST']
            last_first_bp1 = l[0]['BP1']
            last_first_sp1 = l[0]['SP1']
            # last_first_latest = conn_w.hget("MDLD:" + str(cur_ts - self.interval_list[self.interval]) + keyword,
            #                                 'LATEST')
            # last_first_sp1 = conn_w.hget("MDLD:" + str(cur_ts - self.interval_list[self.interval]) + keyword,
            #                              'SP1')
            # last_first_bp1 = conn_w.hget("MDLD:" + str(cur_ts - self.interval_list[self.interval]) + keyword,
            #                              'BP1')
            curr_latest = l[len(l) - 1]['LATEST']
            curr_bp1 = l[len(l) - 1]['BP1']
            curr_sp1 = l[len(l) - 1]['SP1']
            # curr_latest = conn_w.hget("MDLD:" + str(cur_ts) +  keyword, 'BP1')
            # curr_sp1 = conn_w.hget("MDLD:" + str(cur_ts) + keyword, 'SP1')
            # curr_bp1 = conn_w.hget("MDLD:" + str(cur_ts) + keyword, 'BP1')
            new_average_latest = (float(last_average_latest) * self.interval_list[self.interval] - float(last_first_latest) + float(curr_latest)) / self.interval_list[self.interval]
            new_average_sp1 = (float(last_average_sp1) * self.interval_list[self.interval] - float(last_first_sp1) + float(curr_sp1)) / \
                              self.interval_list[self.interval]
            new_average_bp1 = (float(last_average_bp1) * self.interval_list[self.interval] - float(last_first_bp1) + float(curr_bp1)) / \
                              self.interval_list[self.interval]
            d = dict()
            d[self.averageNames[self.interval] + "LATEST"] = new_average_latest
            d[self.averageNames[self.interval] + "SP1"] = new_average_sp1
            d[self.averageNames[self.interval] + "BP1"] = new_average_bp1
            # c_d = conn_w.hgetall(prefix + str(cur_ts) + keyword)
            conn_w.hmset(prefix + str(cur_ts) + keyword, d)
            l[len(l) - 1][self.averageNames[self.interval] + 'LATEST'] = new_average_latest
            l[len(l) - 1][self.averageNames[self.interval] + 'SP1'] = new_average_sp1
            l[len(1) - 1][self.averageNames[self.interval] + 'BP1'] = new_average_bp1

    def run(self,key:int,conn_w):
        cur_ts = key  # 秒编号
        prefix = "MDLD:" + str(cur_ts)
        for key in self.config.config_flist.values():
            # print(len(self.fqueue[key]))
            if len(self.fqueue[key]) < self.interval_list[self.interval]:
                # print(len(self.fqueue[key]), self.interval_list[self.interval],
                #       (len(self.fqueue[key]) < self.interval_list[self.interval]))
                # self.appendFQueue(cur_ts,conn_w,':F:F'+key,key)
                continue
            # print('hhhh')
            # self.appendFQueue(cur_ts,conn_w,":F:F"+key,key)
            self.operate(cur_ts,":F:F"+key,self.interval, conn_w, keyname=key)
        for key in self.config.config_slist:
            # print(len(self.squeue[key]))
            if len(self.squeue[key]) < self.interval_list[self.interval]:
                # self.appendSQueue(cur_ts,conn_w,':JZ:'+key, key)
                continue
            # self.appendSQueue(cur_ts,conn_w,':JZ:'+key,key)
            self.operate(cur_ts,':JZ:'+key,self.interval, conn_w, keyname=key)
        for key in self.config.config_optlist.items():
            # print(len(self.opcqueue[key[0]]))
            if len(self.opcqueue[key[0]]) < self.interval_list[self.interval]:
                # self.appendOPcQueue(cur_ts, conn_w, ':OP:C' + key[0], key[0])
                continue
            # self.appendOPcQueue(cur_ts,conn_w,':OP:C'+key[0],key[0])
            self.operate(cur_ts,':OP:C'+key[0],self.interval,conn_w, keyname=key[0])
        for key in self.config.config_optlist.items():
            # print(len(self.oppqueue[key[0]]))
            if len(self.opcqueue[key[0]]) < self.interval_list[self.interval]:
                # self.appendOPpQueue(cur_ts,conn_w,':OP:P'+ key[0],key[0])
                continue
            # self.appendOPpQueue(cur_ts, conn_w, ':OP:P' + key[0], key[0])
            self.operate(cur_ts,':OP:P'+key[0],self.interval,conn_w,keyname=key[0])
        for key in self.config.config_optlist.keys():
            # print(len(self.vc00queue[key[:4]]))
            if len(self.vc00queue[key[:4]]) < self.interval_list[self.interval]:
                # self.appendVQueueC00(cur_ts,conn_w,':V:C'+key[:4]+'MV0000', key[:4])
                continue
            # self.appendVQueueC00(cur_ts,conn_w,':V:C'+key[:4]+'MV0000',key[:4])
            self.operate(cur_ts,':V:C'+key[:4]+'MV0000',self.interval, conn_w, keyname=key[:4])
        for key in self.config.config_optlist.keys():
            # print(len(self.vp00queue[key[:4]]))
            if len(self.vp00queue[key[:4]]) < self.interval_list[self.interval]:
                # self.appendVQueueP00(cur_ts, conn_w, ':V:P' + key[:4] + 'MV0000', key[:4])
                continue
            # self.appendVQueueP00(cur_ts,conn_w,':V:P'+key[:4]+'MV0000',key[:4])
            self.operate(cur_ts,':V:P'+key[:4]+'MV0000',self.interval, conn_w, keyname=key[:4])
        for key in self.config.config_optlist.keys():
            # print(len(self.vcn5queue[key[:4]]))
            if len(self.vcn5queue[key[:4]]) < self.interval_list[self.interval]:
                # self.appendVQueueCN5(cur_ts, conn_w, ':V:C' + key[:4] + 'MVN050', key[:4])
                continue
            # self.appendVQueueCN5(cur_ts,conn_w,':V:C'+key[:4]+'MVN050',key[:4])
            self.operate(cur_ts,':V:C'+key[:4]+'MVN050',self.interval, conn_w, keyname=key[:4])

        for key in self.config.config_optlist.keys():
            # print(len(self.vpn5queue[key[:4]]))
            if len(self.vpn5queue[key[:4]]) < self.interval_list[self.interval]:
                # self.appendVQueuePN5(cur_ts, conn_w, ':V:P' + key[:4] + 'MVN050', key[:4])
                continue
            # self.appendVQueuePN5(cur_ts,conn_w,':V:P'+key[:4]+'MVN050',key[:4])
            self.operate(cur_ts,':V:P'+key[:4]+'MVN050',self.interval, conn_w, keyname=key[:4])

        for key in self.config.config_optlist.keys():
            # print(len(self.vc05queue[key[:4]]))
            if len(self.vc05queue[key[:4]]) < self.interval_list[self.interval]:
                # self.appendVQueueC05(cur_ts, conn_w, ':V:C' + key[:4] + 'MV0050', key[:4])
                continue
            # self.appendVQueueC05(cur_ts,conn_w,':V:C'+key[:4]+'MV0050',key[:4])
            self.operate(cur_ts,':V:C'+key[:4]+'MV0050',self.interval, conn_w, keyname=key[:4])

        for key in self.config.config_optlist.keys():
            if len(self.vp05queue[key[:4]]) < self.interval_list[self.interval]:
                # self.appendVQueueP05(cur_ts, conn_w, ':V:P' + key[:4] + 'MV0050', key[:4])
                continue
            # self.appendVQueueP05(cur_ts,conn_w,':V:P'+key[:4]+'MV0050',key[:4])
            self.operate(cur_ts,':V:P'+key[:4]+'MV0050',self.interval, conn_w, keyname=key[:4])