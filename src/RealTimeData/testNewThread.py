#-*- coding: UTF-8 -*-
import threading
import time
import redis
import yaml
from operator import itemgetter
from AverageLine import  AverageLine
import timeit


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


def second(key:str):
    print("It is " + str(key),time.time(),time.localtime())


class PeriodAPP():
    def __init__(self, firstrange:list, secondrange: list):
        self.currentDate = str(time.localtime().tm_year) + "-" + str(time.localtime().tm_mon) + "-" + str(time.localtime().tm_mday) #> "2019-07-11"

        self.f_starttime = time.mktime(time.strptime(self.currentDate + " " + firstrange[0] , "%Y-%m-%d %H:%M:%S"))    # >  1563785375.0012558
        self.f_endtime = time.mktime(time.strptime(self.currentDate + " " + firstrange[1] , "%Y-%m-%d %H:%M:%S"))      # >  1563785475.0012558
        self.s_starttime = time.mktime(time.strptime(self.currentDate + " " + secondrange[0] , "%Y-%m-%d %H:%M:%S"))   # >  1563785575.0012558
        self.s_endtime = time.mktime(time.strptime(self.currentDate + " " + secondrange[1] , "%Y-%m-%d %H:%M:%S"))     # >  1563785675.0012558

        self.config = Config("redis_mdld.yaml")
        # self.a5s = AverageLine(0)
        # self.a10s = AverageLine(1)
        # self.a15s = AverageLine(2)
        # self.a30s = AverageLine(3)
        # self.a1m = AverageLine(4)
        # self.a3m = AverageLine(5)
        # self.a5m = AverageLine(6)
        # self.a10m = AverageLine(7)
        # self.a15m = AverageLine(8)
        # self.a30m = AverageLine(9)
        # self.a1h = AverageLine(10)
        # self.a2h = AverageLine(11)
        # self.a4h = AverageLine(12)
        # self.a1d = AverageLine(13)
        self.conn_r = redis.Redis(host="168.36.1.115", port=6379, password="", charset='gb18030', errors='replace',
                           decode_responses=True)
        self.conn_w = redis.Redis(host="168.36.1.116", port = 6379, password="", charset='gb18030',errors="replace",
                             decode_responses=True)
        self.conn_r2 = redis.Redis(host="192.168.40.134", port=6379, password="", charset='gb18030', errors='replace',
                           decode_responses=True)

    def timeit(func):
        def test(self):
            start = time.perf_counter()
            func(self)
            end = time.perf_counter()
            print("period time used: ", end - start)
        return test

    def vcompute(self, xingquan_list:list, time:str, pe:int, cur_ts:int, j:int, conn_w):
        # print(p_run[j])

        if pe < xingquan_list[0]['XingQuan'] or pe > xingquan_list[len(xingquan_list) - 1]['XingQuan']:
            if j == 0:
                print("PE-500在行权价以外")
                return
            if j == 1:
                print("PE在行权价以外")
                return
            if j == 2:
                print("PE+500在行权价以外")
                return
        cont = {1:"0000", 0:"N050", 2:"0050"}.get(j, "xxxx")

        i = 0
        while i < len(xingquan_list):
            if  pe < xingquan_list[i]['XingQuan']:
                # print(p_runj,xingquan_list[i]['XingQuan'])
                pre_c_p = 1.0 * (float(xingquan_list[i - 1]['C_Latest']) - float(xingquan_list[i]['C_Latest'])) / (
                               float(xingquan_list[i]['XingQuan']) - float(xingquan_list[i-1]['XingQuan'])) * (
                                          float(xingquan_list[i]['XingQuan']) - float(pe)) + float(xingquan_list[i][
                                 'C_Latest'])
                pre_p_p = 1.0 * (float(xingquan_list[i]['P_Latest']) - float(xingquan_list[i-1]['P_Latest'])) / (
                           float(xingquan_list[i]['XingQuan']) - float(xingquan_list[i-1]['XingQuan'])) * (
                                  float(pe) - float(xingquan_list[i - 1]['XingQuan'])) + float(xingquan_list[i - 1][
                             'P_Latest'])
                pre_c_sp1 = 1.0 * (float(xingquan_list[i - 1]['C_SP1']) - float(xingquan_list[i]['C_SP1'])) / (
                           float(xingquan_list[i]['XingQuan']) - float(xingquan_list[i - 1]['XingQuan'])) * (
                                       float(xingquan_list[i]['XingQuan']) - float(pe) ) + \
                            float(xingquan_list[i]['C_SP1'])
                pre_p_sp1 = 1.0 * (float(xingquan_list[i]['P_SP1']) - float(xingquan_list[i-1]['P_SP1'])) / (
                           float(xingquan_list[i]['XingQuan']) - float(xingquan_list[i-1]['XingQuan'])) * (
                                    float(pe) - float(xingquan_list[i - 1]['XingQuan'])) + float(xingquan_list[i - 1][
                               'P_SP1'])
                pre_c_bp1 = 1.0 * (float(xingquan_list[i-1]['C_BP1']) - float(xingquan_list[i]['C_BP1'])) / (
                           float(xingquan_list[i]['XingQuan']) - float(xingquan_list[i - 1]['XingQuan'])) * (
                                       float(xingquan_list[i]['XingQuan']) - float(pe)) + \
                            float(xingquan_list[i]['C_BP1'])
                pre_p_bp1 = 1.0 * (float(xingquan_list[i]['P_BP1']) - float(xingquan_list[i-1]['P_BP1'])) / (
                           float(xingquan_list[i]['XingQuan']) - float(xingquan_list[i-1]['XingQuan'])) * (
                                    float(pe) - float(xingquan_list[i - 1]['XingQuan'])) + float(xingquan_list[i - 1][
                               'P_BP1'])
                # print(xingquan_list[i]['C_Latest'],xingquan_list[i]['P_Latest'])
                # print({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1},{'LATEST': pre_p_p, 'SP1': pre_p_sp1, 'BP1': pre_p_bp1})
                conn_w.hmset("MDLD:" + str(cur_ts) + ":V:C" + time + 'MV' + cont,
                             {'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                # if j == 1:
                #     self.a5s.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a10s.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a15s.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a30s.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a1m.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a3m.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a5m.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a10m.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a15m.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a30m.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a1h.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a2h.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a4h.vc00queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                # if j == 2:
                #     self.a5s.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a10s.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a15s.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a30s.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a1m.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a3m.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a5m.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a10m.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a15m.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a30m.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a1h.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a2h.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a4h.vc05queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                # if j == 0:
                #     self.a5s.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a10s.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a15s.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a30s.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a1m.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a3m.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a5m.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a10m.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a15m.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a30m.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a1h.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a2h.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})
                #     self.a4h.vcn5queue[time].append({'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})

                conn_w.hmset("MDLD:" + str(cur_ts) + ":V:P" + time + 'MV' + cont,
                             {'LATEST': pre_p_p, 'SP1': pre_p_sp1, 'BP1': pre_p_bp1,'pe':pe})
                d = {'LATEST': pre_p_p, 'SP1': pre_p_sp1, 'BP1': pre_p_bp1,'pe':pe}
                # if j == 1:
                #     self.a5s.vp00queue[time].append(d)
                #     self.a10s.vp00queue[time].append(d)
                #     self.a15s.vp00queue[time].append(d)
                #     self.a30s.vp00queue[time].append(d)
                #     self.a1m.vp00queue[time].append(d)
                #     self.a3m.vp00queue[time].append(d)
                #     self.a5m.vp00queue[time].append(d)
                #     self.a10m.vp00queue[time].append(d)
                #     self.a15m.vp00queue[time].append(d)
                #     self.a30m.vp00queue[time].append(d)
                #     self.a1h.vp00queue[time].append(d)
                #     self.a2h.vp00queue[time].append(d)
                #     self.a4h.vp00queue[time].append(d)
                # if j == 2:
                #     self.a5s.vp05queue[time].append(d)
                #     self.a10s.vp05queue[time].append(d)
                #     self.a15s.vp05queue[time].append(d)
                #     self.a30s.vp05queue[time].append(d)
                #     self.a1m.vp05queue[time].append(d)
                #     self.a3m.vp05queue[time].append(d)
                #     self.a5m.vp05queue[time].append(d)
                #     self.a10m.vp05queue[time].append(d)
                #     self.a15m.vp05queue[time].append(d)
                #     self.a30m.vp05queue[time].append(d)
                #     self.a1h.vp05queue[time].append(d)
                #     self.a2h.vp05queue[time].append(d)
                #     self.a4h.vp05queue[time].append(d)
                # if j == 0:
                #     self.a5s.vpn5queue[time].append(d)
                #     self.a10s.vpn5queue[time].append(d)
                #     self.a15s.vpn5queue[time].append(d)
                #     self.a30s.vpn5queue[time].append(d)
                #     self.a1m.vpn5queue[time].append(d)
                #     self.a3m.vpn5queue[time].append(d)
                #     self.a5m.vpn5queue[time].append(d)
                #     self.a10m.vpn5queue[time].append(d)
                #     self.a15m.vpn5queue[time].append(d)
                #     self.a30m.vpn5queue[time].append(d)
                #     self.a1h.vpn5queue[time].append(d)
                #     self.a2h.vpn5queue[time].append(d)
                #     self.a4h.vpn5queue[time].append(d)
                break
            i = i + 1

    def getTime(self,key):
        currentTime = key + int(time.mktime(time.strptime(self.currentDate + " " + "00:00:00" , "%Y-%m-%d %H:%M:%S"))) - 3600
        # print(time.localtime(currentTime).tm_hour,time.localtime(currentTime).tm_min,time.localtime(currentTime).tm_sec)
        return str(time.localtime(currentTime).tm_hour) + ":" + str(time.localtime(currentTime).tm_min) + ":" + str(time.localtime(currentTime).tm_sec)

    @timeit
    def run(self):
        key = int(time.time())-int(time.mktime(time.strptime(self.currentDate + " " + "00:00:00" , "%Y-%m-%d %H:%M:%S"))) + 3600 # > 1563785675
        print("当前时间：" + str(key),self.getTime(key))
        conn_r = self.conn_r.pipeline(transaction=False)
        conn_w = self.conn_w.pipeline(transaction=False)

        cur_ts = key
        conn_w.set("MDLD:cur_ts",cur_ts)
        pre = 'KZ:F'
        f_d_list = dict()
        # print("期货列表")
        for keyname, key in self.config.config_flist.items():
            # print(pre+key+":LATEST",pre+key+":BP1",pre+key+":SP1")
            conn_r.mget([pre+key+":LATEST",pre+key+":BP1",pre+key+":SP1"])

        pre = 'KZ:'
        pre1 = 'KZ:JZ0000KZE'

        for key in self.config.config_slist:
            conn_r.mget([pre + key + ":LATEST", pre+key + ":BP1", pre + key + ":SP1"])
            conn_r.mget([pre1 + key[1:] +":NEW", pre1 + key[1:] + ":B1", pre1 + key[1:] + ":S1"])
        conn_r2 = self.conn_r2.pipeline(transaction=False)
        # print("期权列表")
        op_c_p_price = list()
        date_list = set()
        for pxname, (icode_c, icode_p)in self.config.config_optlist.items():
            #> '1909M02750': ['10001709', '10001710']
            # print(item[0][:4])
            conn_r2.hmget("MD:01" + icode_c,'Latest','SP1','BP1')  #> "MD:0110001709"
            conn_r2.hmget("MD:01" + icode_p,'Latest','SP1','BP1')
        r1_result = conn_r.execute()
        r2_result = conn_r2.execute()
        # print(r1_result, r2_result)
        r1_index = 0
        for keyname, key in self.config.config_flist.items():
            # print(pre+key+":LATEST",pre+key+":BP1",pre+key+":SP1")
            latest, bp1, sp1 = r1_result[r1_index]
            r1_index = r1_index + 1
            d = dict()
            d['LATEST'] = latest
            d['BP1'] = bp1
            d['SP1'] = sp1
            # print(d)
            # self.a5s.fqueue[key].append(d)
            # self.a10s.fqueue[key].append(d)
            # self.a15s.fqueue[key].append(d)
            # self.a30s.fqueue[key].append(d)
            # self.a1m.fqueue[key].append(d)
            # self.a3m.fqueue[key].append(d)
            # self.a5m.fqueue[key].append(d)
            # self.a10m.fqueue[key].append(d)
            # self.a15m.fqueue[key].append(d)
            # self.a30m.fqueue[key].append(d)
            # self.a1h.fqueue[key].append(d)
            # self.a2h.fqueue[key].append(d)
            # self.a4h.fqueue[key].append(d)
            f_d_list[keyname] = d
            conn_w.hmset("MDLD:" + str(cur_ts) + ":F:F" + key, d)
        pre = 'KZ:'
        pre1 = 'KZ:JZ0000KZE'
        # print("现货列表")
        pe = 0
        pe300 = 0
        pe500 = 0
        pe_510050_SP1 = 0
        pe_510050_BP1 = 0
        jz_510050_SP1 = 0
        jz_510050_BP1 =0
        pe_510300_SP1 = 0
        pe_510300_BP1 = 0
        jz_510300_SP1 = 0
        jz_510300_BP1 =0
        pe_510500_SP1 = 0
        pe_510500_BP1 = 0
        jz_510500_SP1 = 0
        jz_510500_BP1 =0
        for key in self.config.config_slist:
            latest, bp1, sp1 = r1_result[r1_index]
            r1_index = r1_index + 1
            d = dict()
            d['LATEST'] = latest
            d['BP1'] = bp1
            d['SP1'] = sp1
            # print(d)
            conn_w.hmset("MDLD:" + str(cur_ts) + ":S:" + key,d)
            new, b1, s1 = r1_result[r1_index]
            r1_index = r1_index + 1
            if new == None:
                new = 0
            if b1 == None:
                b1 = 0
            if s1 == None:
                s1 = 0
            d2 = dict()
            d2['LATEST'] = new
            d2['BP1'] = b1
            d2['SP1'] = s1
            # self.a5s.squeue[key].append(d)
            # self.a10s.squeue[key].append(d)
            # self.a15s.squeue[key].append(d)
            # self.a30s.squeue[key].append(d)
            # self.a1m.squeue[key].append(d)
            # self.a3m.squeue[key].append(d)
            # self.a5m.squeue[key].append(d)
            # self.a10m.squeue[key].append(d)
            # self.a15m.squeue[key].append(d)
            # self.a30m.squeue[key].append(d)
            # self.a1h.squeue[key].append(d)
            # self.a2h.squeue[key].append(d)
            # self.a4h.squeue[key].append(d)
            # print(d)
            conn_w.hmset("MDLD:" + str(cur_ts) + ":JZ:" + key,d2)
            if key == "S510050":
                pe = int(d['LATEST'])
                pe_510050_SP1 = int(d['SP1'])
                pe_510050_BP1 = int(d['BP1'])
                jz_510050_SP1 = float(d2['SP1'])
                jz_510050_BP1 = float(d2['BP1'])
            if key == "S510300":
                pe300 = int(d['LATEST'])
                pe_510300_SP1 = int(d['BP1'])
                pe_510300_BP1 = int(d['SP1'])
                jz_510300_SP1 = float(d2['SP1'])
                jz_510300_BP1 = float(d2['BP1'])
            if key == "S510500":
                pe500 = int(d['LATEST'])
                pe_510500_SP1 = int(d['SP1'])
                pe_510500_BP1 = int(d['BP1'])
                jz_510500_SP1 = float(d2['SP1'])
                jz_510500_BP1 = float(d2['BP1'])
        # conn_r = redis.Redis(host="168.36.1.170", port=6379, password="", charset='gb18030', errors='replace',
        #                    decode_responses=True)
        # print("期权列表")
        op_c_p_price = list()
        date_list = set()
        r2_index = 0
        for pxname, (icode_c, icode_p)in self.config.config_optlist.items():
            #> '1909M02750': ['10001709', '10001710']
            # print(item[0][:4])
            date_list.add(pxname[:4])  #> “1909”
            tem1 = dict()
            tem1['Latest'], tem1['SP1'], tem1['BP1'] = r2_result[r2_index]  #> "MD:0110001709"
            r2_index = r2_index + 1
            tem2 = dict()
            tem2['Latest'], tem2['SP1'], tem2['BP1'] = r2_result[r2_index]
            r2_index = r2_index + 1
            # print(tem1, tem2)
            if tem1['Latest'] == None or tem1['BP1'] == None or tem1['SP1'] == None or tem2['Latest'] == None or tem2['SP1'] == None or tem2['BP1'] == None:
                continue
            # print(tem1)
            price_dict= dict()
            price_dict['Name'] = pxname
            price_dict['XingQuan'] = int(pxname[-5:]) * 10  #> "02750"
            price_dict['C_Latest'] = tem1['Latest']
            price_dict['C_BP1'] = tem1['BP1']
            price_dict['C_SP1'] = tem1['SP1']
            price_dict['P_Latest'] = tem2['Latest']
            price_dict['P_BP1'] = tem2['BP1']
            price_dict['P_SP1'] = tem2['SP1']
            op_c_p_price.append(price_dict)
            # print(tem1)
            # print(tem2)
            if tem1['Latest'] != None and tem1['BP1'] != None and tem1['SP1'] != None:
                td = {"LATEST": tem1['Latest'], 'BP1': tem1['BP1'], 'SP1': tem1['SP1']}
                conn_w.hmset("MDLD:" + str(cur_ts) + ":OP:" + "C" + pxname, td)

                # self.a5s.opcqueue[pxname].append(td)
                # self.a10s.opcqueue[pxname].append(td)
                # self.a15s.opcqueue[pxname].append(td)
                # self.a30s.opcqueue[pxname].append(td)
                # self.a1m.opcqueue[pxname].append(td)
                # self.a3m.opcqueue[pxname].append(td)
                # self.a5m.opcqueue[pxname].append(td)
                # self.a10m.opcqueue[pxname].append(td)
                # self.a15m.opcqueue[pxname].append(td)
                # self.a30m.opcqueue[pxname].append(td)
                # self.a1h.opcqueue[pxname].append(td)
                # self.a2h.opcqueue[pxname].append(td)
                # self.a4h.opcqueue[pxname].append(td)
            if tem2['Latest'] != None and tem2['SP1'] != None and tem2['BP1'] != None:
                td = {"LATEST":tem2['Latest'],'BP1':tem2['BP1'],'SP1':tem2['SP1']}
                conn_w.hmset("MDLD:" + str(cur_ts) + ":OP:" + "P" + pxname, td)
                # self.a5s.oppqueue[pxname].append(td)
                # self.a10s.oppqueue[pxname].append(td)
                # self.a15s.oppqueue[pxname].append(td)
                # self.a30s.oppqueue[pxname].append(td)
                # self.a1m.oppqueue[pxname].append(td)
                # self.a3m.oppqueue[pxname].append(td)
                # self.a5m.oppqueue[pxname].append(td)
                # self.a10m.oppqueue[pxname].append(td)
                # self.a15m.oppqueue[pxname].append(td)
                # self.a30m.oppqueue[pxname].append(td)
                # self.a1h.oppqueue[pxname].append(td)
                # self.a2h.oppqueue[pxname].append(td)
                # self.a4h.oppqueue[pxname].append(td)
            px = int(pxname[-5:])*10
            pc = int(tem1['Latest'])
            pp = int(tem2['Latest'])
            po= px + pc - pp
            a5 = po - pe
            pcbp1 = int(tem1['BP1'])
            ppsp1 = int(tem2['SP1'])
            pcsp1 = int(tem1['SP1'])
            ppbp1 = int(tem2['BP1'])
            a5s = px + pcbp1 - ppsp1 - pe
            a5b = px + pcsp1 - ppbp1 - pe
            d = dict()
            d['LATEST'] = a5
            d['BP1'] = a5b
            d['SP1'] = a5s
            d['Pe'] = pe
            # self.a5s.a5queue[pxname].append(d)
            # self.a10s.a5queue[pxname].append(d)
            # self.a15s.a5queue[pxname].append(d)
            # self.a30s.a5queue[pxname].append(d)
            # self.a1m.a5queue[pxname].append(d)
            # self.a3m.a5queue[pxname].append(d)
            # self.a5m.a5queue[pxname].append(d)
            # self.a10m.a5queue[pxname].append(d)
            # self.a15m.a5queue[pxname].append(d)
            # self.a30m.a5queue[pxname].append(d)
            # self.a1h.a5queue[pxname].append(d)
            # self.a2h.a5queue[pxname].append(d)
            # self.a4h.a5queue[pxname].append(d)
            conn_w.hmset("MDLD:" + str(cur_ts) + ":A5:" + pxname,d)
            conn_w.set("MDLD:" + str(cur_ts) + ":PO:" + pxname,po)
        for key in self.config.config_flist.keys():
            d = dict()
            if key.startswith("IH"):
                d["B"] = int(f_d_list[key]['BP1'])//1000 - pe_510050_SP1
                d["S"] = int(f_d_list[key]['SP1'])//1000 - pe_510050_BP1
                d["L"] = int(f_d_list[key]['LATEST'])//1000 - pe
                d["C"] = int(f_d_list[key]['BP1'])//1000 - jz_510050_SP1 * 10000
                d["R"] = int(f_d_list[key]['SP1'])//1000 - jz_510050_BP1 * 10000
            elif key.startswith("IF"):
                d["B"] = int(f_d_list[key]['BP1'])//1000 - pe_510300_BP1
                d["S"] = int(f_d_list[key]['SP1'])//1000 - pe_510300_BP1
                d["L"] = int(f_d_list[key]['LATEST'])//1000 - pe300
                d["C"] = int(f_d_list[key]['BP1'])//1000 - jz_510300_SP1 * 10000
                d["R"] = int(f_d_list[key]['SP1'])//1000 - jz_510300_BP1 * 10000
            elif key.startswith("IC"):
                d["B"] = int(f_d_list[key]['BP1'])//1000 - pe_510500_BP1
                d["S"] = int(f_d_list[key]['SP1'])//1000 - pe_510500_BP1
                d["L"] = int(f_d_list[key]['LATEST'])//1000 - pe500
                d["C"] = int(f_d_list[key]['BP1'])//1000 - jz_510500_SP1 * 10000
                d["R"] = int(f_d_list[key]['SP1'])//1000 - jz_510500_BP1 * 10000
            conn_w.hmset("MDLD:" + str(cur_ts) + ":A13:" + key, d)

        # for i in range(len(op_c_p_price)-1):
        #     t = 0
        #     while t < len(op_c_p_price) - 1:
        #         if op_c_p_price[t]['XingQuan'] > op_c_p_price[t+1]['XingQuan']:
        #             temp_d = op_c_p_price[t]
        #             op_c_p_price[t] = op_c_p_price[t+1]
        #             op_c_p_price[t+1] = temp_d
        #         t = t+1
        # print(op_c_p_price)
        op_c_p_price = sorted(op_c_p_price,key = lambda x: x['XingQuan'])
        # print(op_c_p_price)
        date_list = list(date_list)
        date_list.sort()  #> ['1908', '1909', '1912', '2003']
        dangyue_c_p_price = list()
        xiayue_c_p_price = list()
        xiaji_c_p_price = list()
        geji_c_p_price = list()
        for i in range(len(op_c_p_price)):
            if op_c_p_price[i]['Name'].startswith(date_list[0]):
                dangyue_c_p_price.append(op_c_p_price[i])
            elif op_c_p_price[i]['Name'].startswith(date_list[1]):
                xiayue_c_p_price.append(op_c_p_price[i])
            elif op_c_p_price[i]['Name'].startswith(date_list[2]):
                xiaji_c_p_price.append(op_c_p_price[i])
            elif op_c_p_price[i]['Name'].startswith(date_list[3]):
                geji_c_p_price.append(op_c_p_price[i])
        # print(dangyue_c_p_price)
        # print(xiayue_c_p_price)
        # print(xiaji_c_p_price)
        # print(geji_c_p_price)
        pe_p = pe + 500
        pe_n = pe - 500
        p_run = [pe_n,pe,pe_p]
        for i, p_runj in enumerate(p_run):
            if len(dangyue_c_p_price) != 0 :
                self.vcompute(dangyue_c_p_price, dangyue_c_p_price[0]['Name'][:4],p_runj,cur_ts,i,conn_w)
            if len(xiayue_c_p_price) != 0:
                self.vcompute(xiayue_c_p_price, xiayue_c_p_price[0]['Name'][:4],p_runj,cur_ts,i,conn_w)
            if len(xiaji_c_p_price) != 0:
                self.vcompute(xiaji_c_p_price,xiaji_c_p_price[0]['Name'][:4],p_runj,cur_ts,i,conn_w)
            if len(geji_c_p_price) != 0:
                self.vcompute(geji_c_p_price,geji_c_p_price[0]['Name'][:4],p_runj,cur_ts,i,conn_w)
        conn_w.execute()
        # for i in range(20):
        #     self.a5s.run(cur_ts,conn_w)
        # self.a5s.run(cur_ts,conn_w)
        # self.a10s.run(cur_ts,conn_w)
        # self.a15s.run(cur_ts,conn_w)
        # self.a30s.run(cur_ts,conn_w)
        # self.a1m.run(cur_ts,conn_w)
        # self.a3m.run(cur_ts,conn_w)
        # self.a5m.run(cur_ts,conn_w)
        # self.a10m.run(cur_ts,conn_w)
        # self.a15m.run(cur_ts,conn_w)
        # self.a30m.run(cur_ts,conn_w)
        # self.a1h.run(cur_ts,conn_w)
        # self.a2h.run(cur_ts,conn_w)
        # self.a4h.run(cur_ts,conn_w)
        # self.a1d.run(cur_ts,conn_w)

    def start(self):
        print("上班啦")
        print("上午：")
        self.operate([self.f_starttime,self.f_endtime],57600)
        print("下午：")
        self.operate([self.s_starttime,self.s_endtime],45000)
        print("下班啦")

    def operate(self, interval:list, last_sec:int):

        start_time = interval[0]
        end_time = interval[1]
        if time.time() > end_time:
            return
        ctime = time.time()
        if ctime < start_time:
            print("等待开盘")
            conn_w = self.conn_w
            # self.a5s.initalYesData(conn_w,last_sec)
            # self.a10s.initalYesData(conn_w,last_sec)
            # self.a15s.initalYesData(conn_w,last_sec)
            # self.a30s.initalYesData(conn_w,last_sec)
            # self.a1m.initalYesData(conn_w,last_sec)
            # self.a3m.initalYesData(conn_w,last_sec)
            # self.a5m.initalYesData(conn_w,last_sec)
            # self.a10m.initalYesData(conn_w,last_sec)
            # self.a15m.initalYesData(conn_w,last_sec)
            # self.a30m.initalYesData(conn_w,last_sec)
            # self.a1h.initalYesData(conn_w,last_sec)
            # self.a2h.initalYesData(conn_w,last_sec)
            # self.a4h.initalYesData(conn_w,last_sec)
            # self.a1d.initalYesData(conn_w,last_sec)
            interval = int(start_time) - time.time()
            if interval > 0:
                time.sleep(interval)
            self.run()

        ctime = time.time()
        if ctime >= start_time and ctime <= end_time:
            time.sleep(int(time.time())+1 - time.time())
            while time.time() < end_time:
                self.run()
                time1 = time.time()
                time.sleep(int(time.time())+1 - time.time())
                if int(time.time()) - int(time1) != 1:
                    print("miss one")
            self.run()



def main():
    # print(type(time.localtime().tm_mon),time.localtime().tm_year,time.localtime().tm_mday)
    t = PeriodAPP(["09:30:00","11:30:00"],["13:02:00","16:00:00"])
    t.start()
    # time.sleep(10)
    # t.join()
    # print(5)

if __name__=='__main__':
    main()

