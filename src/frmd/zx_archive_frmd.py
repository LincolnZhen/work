
# coding: utf-8

import struct
from ut_btl_md import CbtlMD, ctp_unpack
import time
import os
import pickle
import re
from operator import itemgetter
import psycopg2




def MdUnpack1(buf:bytes):
    t = struct.unpack("@9x8s63xd120x8s4xididi", buf[19:19 + 252])  # 取最后一个字段off+len
    op, latest, ts, tsm, BP1, BA1, SP1, SA1 = t
    print("+++", op, latest, ts, tsm, BP1, BA1, SP1, SA1)


def MdUnpackStruct(buf:bytes, i:int, str_ts:str, tsu:int):
    d = ctp_unpack(buf, 19)

    # print("\n[%d] ---" % i, str_ts, tsu)
    # if d["InstrumentID"][0] != '10001165': continue
    re = dict()
    for fldname in d:
        fldv, fldcomment = d[fldname]
        # print("   ", fldname + ":", repr(fldv), "#", fldcomment)
        re[fldname] = d[fldname][0]
    return re

def main_btl_dump_md(fn:str,number:int):
    print("文件：" , fn)
    btl = CbtlMD(fn)
    num = number
    for i, (ts, tsu, buf) in enumerate(btl):
        if i % 1000 == 0:
            str_ts = time.strftime("%H:%M:%S", time.localtime(ts))
            # print(str_ts)
        # print(len(buf))
        # MdUnpack1(buf, "10001287".encode())
        d = MdUnpackStruct(buf, i, str_ts, tsu)
        # print(d)
        # print(d['AskPrice1'])
        pr_key = d['TradingDay'] + "_" + d['ExchangeID'] + "_" + d['InstrumentID'] + "_" + str(num)
        num = num + 1
        # print(num)
        cur.execute("insert into MDOP Values( \'" + pr_key + '\',\'' + str(d).replace('\'','\"') + "\')"
                    )
    print("total:", i + 1)
    return num

if __name__ == '__main__':
    conn = psycopg2.connect(database="postgres", user="postgres", password="...", host="192.168.40.129", port="5432")
    cur = conn.cursor()
    path = r"\\168.36.1.170\share\archives"
    dirs = os.listdir(path)
    file_list = list()

    for filename in dirs:
        if re.match(r"frmd_*",filename) and os.path.splitext(filename)[1] == '.btl':
            filepath = path + "\\" + filename
            time1 = time.ctime(os.path.getctime(filepath))
            print(filepath)
            d = dict()
            d["time"] = time1
            d["second"] = os.path.getctime(filepath)
            d["fileName"] = filepath
            file_list.append(d)
    file_list.sort(key=itemgetter('second'),reverse = False)

    print("开始插入数据")
    # main_btl_dump_md(file_list[0]['fileName'])
    num = 1
    for file in file_list:
        # print(file['time'])
        num = main_btl_dump_md(file['fileName'],num)

    conn.commit()
    conn.close()
    # main_btl_dump_md(r"\\168.36.1.170\share\archives\frmd_0712_2422.btl")






