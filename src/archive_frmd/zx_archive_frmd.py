
# coding: utf-8

import struct
from ut_btl_md import CbtlMD, ctp_unpack
import time


def MdUnpack1(buf:bytes):
    t = struct.unpack("@9x8s63xd120x8s4xididi", buf[19:19 + 252])  # 取最后一个字段off+len
    op, latest, ts, tsm, BP1, BA1, SP1, SA1 = t
    print("+++", op, latest, ts, tsm, BP1, BA1, SP1, SA1)


def MdUnpackStruct(buf:bytes, i:int, str_ts:str, tsu:int):
    d = ctp_unpack(buf, 19)

    print("\n[%d] ---" % i, str_ts, tsu)
    # if d["InstrumentID"][0] != '10001165': continue
    for fldname in d:
        fldv, fldcomment = d[fldname]
        print("   ", fldname + ":", repr(fldv), "#", fldcomment)


def main_btl_dump_md(fn:str):
    btl = CbtlMD(fn)
    for i, (ts, tsu, buf) in enumerate(btl):
        if i % 1000 == 0:
            str_ts = time.strftime("%H:%M:%S", time.localtime(ts))
            print(str_ts)
        # print(len(buf))
        # MdUnpack1(buf, "10001287".encode())
        MdUnpackStruct(buf, i, str_ts, tsu)

    print("total:", i + 1)


if __name__ == '__main__':
    main_btl_dump_md(r"\\168.36.1.170\share\archives\frmd_0712_2422.btl")






