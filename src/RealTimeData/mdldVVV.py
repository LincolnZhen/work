#-*- coding: UTF-8 -*-
import threading
import time
import redis
import yaml
from operator import itemgetter
from AverageLine import  AverageLine
import timeit
from HMS import Hms,hmb_heartbeat

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


def timeit(func):
    def test(self):
        start = time.perf_counter()
        func(self)
        end = time.perf_counter()
        print("period time used: ", end - start)
    return test


def mysleep(t:float):
    if t > 0:
        time.sleep(t)


class PeriodAPP:
    def __init__(self,sleep_interval:int,mdld_index:str):
        """
        :param firstrange:  ['09:30:00', '11:30:00']
        :param secondrange:  ['13:00:00', '15:00:00']
        """
        self.currentDate = str(time.localtime().tm_year) + "-" + str(time.localtime().tm_mon) + "-" + str(time.localtime().tm_mday)  #> "2019-07-11"
        self.config = Config("redis_mdld.yaml")
        self.sleep_interval = sleep_interval
        self.mdld_index = mdld_index

        self.conn_r = redis.Redis(host="168.36.1.115", port=6379, password="", charset='gb18030', errors='replace',
                                  decode_responses=True)
        self.conn_w = redis.Redis(host="168.36.1.116", port = 6379, password="", charset='gb18030',errors="replace",
                                  decode_responses=True)
        self.conn_r2 = redis.Redis(host="168.36.1.170", port=6379, password="", charset='gb18030', errors='replace',
                                   decode_responses=True)

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
            if pe < xingquan_list[i]['XingQuan']:
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
                conn_w.hmset("MDLD" + self.mdld_index + ":" + self.mdld_index + str(cur_ts) + ":V:C" + time + 'MV' + cont,
                             {'LATEST': pre_c_p, 'SP1': pre_c_sp1, 'BP1': pre_c_bp1,'pe':pe})


                conn_w.hmset("MDLD" + self.mdld_index + ":" + self.mdld_index + str(cur_ts) + ":V:P" + time + 'MV' + cont,
                             {'LATEST': pre_p_p, 'SP1': pre_p_sp1, 'BP1': pre_p_bp1,'pe':pe})
                d = {'LATEST': pre_p_p, 'SP1': pre_p_sp1, 'BP1': pre_p_bp1,'pe':pe}

                break
            i = i + 1

    def getTime(self,key):
        currentTime = key + int(time.mktime(time.strptime(self.currentDate + " " + "00:00:00" , "%Y-%m-%d %H:%M:%S"))) - 3600 * 3
        # print(time.localtime(currentTime).tm_hour,time.localtime(currentTime).tm_min,time.localtime(currentTime).tm_sec)
        return str(time.localtime(currentTime).tm_hour) + ":" + str(time.localtime(currentTime).tm_min) + ":" + str(time.localtime(currentTime).tm_sec)

    @timeit
    def run(self):
        key = int(time.time())-int(time.mktime(time.strptime(self.currentDate + " " + "00:00:00" , "%Y-%m-%d %H:%M:%S"))) + 3600 * 3 # > 1563785675
        print("当前时间：" + str(key),self.getTime(key))
        conn_r = self.conn_r.pipeline(transaction=False)
        conn_w = self.conn_w.pipeline(transaction=False)

        cur_ts = key
        conn_w.set("MDLD"+ self.mdld_index + ":cur_ts",cur_ts)
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
            conn_r.mget([pre1 + key[1:] + ":NEW", pre1 + key[1:] + ":B1", pre1 + key[1:] + ":S1"])
        conn_r2 = self.conn_r2.pipeline(transaction=False)
        # print("期权列表")
        # op_c_p_price = list()
        # date_list = set()
        for pxname, (icode_c, icode_p)in self.config.config_optlist.items():
            #> '1909M02750': ['10001709', '10001710']
            # print(item[0][:4])
            conn_r2.hmget("MD:01" + icode_c,'Latest','SP1','BP1','PreSettle')  #> "MD:0110001709"
            conn_r2.hmget("MD:01" + icode_p,'Latest','SP1','BP1','PreSettle')
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
            f_d_list[keyname] = d
            conn_w.hmset("MDLD" + self.mdld_index + ":" + str(cur_ts) + ":F:F" + key, d)

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
            conn_w.hmset("MDLD" + self.mdld_index + ":" + str(cur_ts) + ":S:" + key,d)
            new, b1, s1 = r1_result[r1_index]
            r1_index = r1_index + 1
            if new is None:
                new = 0
            if b1 is None:
                b1 = 0
            if s1 is None:
                s1 = 0
            d2 = dict()
            d2['LATEST'] = new
            d2['BP1'] = b1
            d2['SP1'] = s1
            conn_w.hmset("MDLD" + self.mdld_index +":" + str(cur_ts) + ":JZ:" + key,d2)
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
        for pxname, _ in self.config.config_optlist.items():
            #> '1909M02750': ['10001709', '10001710']
            # print(item[0][:4])
            date_list.add(pxname[:4])  #> “1909”
            tem1 = dict()
            tem1['Latest'], tem1['SP1'], tem1['BP1'], tem1['PreSettle'] = r2_result[r2_index]  #> "MD:0110001709"
            r2_index = r2_index + 1
            tem2 = dict()
            tem2['Latest'], tem2['SP1'], tem2['BP1'], tem2['PreSettle'] = r2_result[r2_index]
            r2_index = r2_index + 1
            # print(tem1, tem2)
            if tem1['Latest'] is None or tem1['BP1'] is None or tem1['SP1'] is None or tem2['Latest'] is None \
                    or tem2['SP1'] is None or tem2['BP1'] is None or tem1['PreSettle'] is None or tem2['PreSettle'] is None:
                continue
            if tem1['Latest'] == 0:
                tem1['Latest'] = tem1['PreSettle']
            if tem2['Latest'] == 0:
                tem2['Latest'] = tem2['PreSettle']
            # print(tem1)
            price_dict = dict()
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
            if tem1['Latest'] is not None and tem1['BP1'] is not None and tem1['SP1'] is not None:
                td = {"LATEST": tem1['Latest'], 'BP1': tem1['BP1'], 'SP1': tem1['SP1']}
                conn_w.hmset("MDLD" + self.mdld_index + ":" + str(cur_ts) + ":OP:" + "C" + pxname, td)
            if tem2['Latest'] is not None and tem2['SP1'] is not None and tem2['BP1'] is not None:
                td = {"LATEST":tem2['Latest'],'BP1':tem2['BP1'],'SP1':tem2['SP1']}
                conn_w.hmset("MDLD" + self.mdld_index + ":" + str(cur_ts) + ":OP:" + "P" + pxname, td)
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
            conn_w.hmset("MDLD" + self.mdld_index + ":" + str(cur_ts) + ":A5:" + pxname,d)
            conn_w.set("MDLD"+ self.mdld_index + ":" + str(cur_ts) + ":PO:" + pxname,po)
        for key in self.config.config_flist:
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
            conn_w.hmset("MDLD" + self.mdld_index + ":" + str(cur_ts) + ":A13:" + key, d)
        op_c_p_price = sorted(op_c_p_price, key=lambda x: x['XingQuan'])
        # print(op_c_p_price)
        date_list = list(date_list)
        date_list.sort()  #> ['1908', '1909', '1912', '2003']
        dangyue_c_p_price = list()
        xiayue_c_p_price = list()
        xiaji_c_p_price = list()
        geji_c_p_price = list()
        for px in op_c_p_price:
            if px['Name'].startswith(date_list[0]):
                dangyue_c_p_price.append(px)
            elif px['Name'].startswith(date_list[1]):
                xiayue_c_p_price.append(px)
            elif px['Name'].startswith(date_list[2]):
                xiaji_c_p_price.append(px)
            elif px['Name'].startswith(date_list[3]):
                geji_c_p_price.append(px)
        pe_p = pe + 500  # positive
        pe_n = pe - 500  # negative
        p_run = [pe_n,pe,pe_p]
        for i, p_runj in enumerate(p_run):
            if len(dangyue_c_p_price) != 0:
                self.vcompute(dangyue_c_p_price, dangyue_c_p_price[0]['Name'][:4],p_runj,cur_ts,i,conn_w)
            if len(xiayue_c_p_price) != 0:
                self.vcompute(xiayue_c_p_price, xiayue_c_p_price[0]['Name'][:4],p_runj,cur_ts,i,conn_w)
            if len(xiaji_c_p_price) != 0:
                self.vcompute(xiaji_c_p_price,xiaji_c_p_price[0]['Name'][:4],p_runj,cur_ts,i,conn_w)
            if len(geji_c_p_price) != 0:
                self.vcompute(geji_c_p_price,geji_c_p_price[0]['Name'][:4],p_runj,cur_ts,i,conn_w)
        conn_w.execute()

    def start(self):
        self.hms1 = Hms(9, 30)
        self.hms2 = Hms(11, 30)
        self.hms3 = Hms(13)
        self.hms4 = Hms(15)
        print("上班啦")
        print("上午：")
        self.operate(self.hms1,self.hms2)
        print("下午：")
        self.operate(self.hms3,self.hms4)
        print("下班啦")

    def operate(self, hms1, hms2):
        """
        :param start_time: '09:30:00'
        :param end_time: '11:30:00'
        :return: 
        """""
        for i in hmb_heartbeat(hms1, hms2, self.sleep_interval):
            self.run()



def main():
    # print(type(time.localtime().tm_mon),time.localtime().tm_year,time.localtime().tm_mday)
    t = PeriodAPP(["09:30:00", "11:30:00"], ["13:00:00", "15:00:00"])
    t.start()
    # time.sleep(10)
    # t.join()
    # print(5)


if __name__ == '__main__':
    main()

