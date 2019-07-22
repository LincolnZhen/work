
import struct
from collections import OrderedDict


CThostFtdcDepthMarketDataField = [ # sizeof=408
   ('TradingDay', '9s', '交易日'),
   ('InstrumentID', '31s', '合约代码'),
   ('ExchangeID', '9s', '交易所代码'),
   ('ExchangeInstID', '31s', '合约在交易所的代码'),
   ('LastPrice', 'd', '最新价'),
   ('PreSettlementPrice', 'd', '上次结算价'),
   ('PreClosePrice', 'd', '昨收盘'),
   ('PreOpenInterest', 'd', '昨持仓量'),
   ('OpenPrice', 'd', '今开盘'),
   ('HighestPrice', 'd', '最高价'),
   ('LowestPrice', 'd', '最低价'),
   ('Volume', 'i', '数量'),
   ('Turnover', 'd', '成交金额'),
   ('OpenInterest', 'd', '持仓量'),
   ('ClosePrice', 'd', '今收盘'),
   ('SettlementPrice', 'd', '本次结算价'),
   ('UpperLimitPrice', 'd', '涨停板价'),
   ('LowerLimitPrice', 'd', '跌停板价'),
   ('PreDelta', 'd', '昨虚实度'),
   ('CurrDelta', 'd', '今虚实度'),
   ('UpdateTime', '9s', '最后修改时间'),
   ('UpdateMillisec', 'i', '最后修改毫秒'),
   ('BidPrice1', 'd', '申买价一'),
   ('BidVolume1', 'i', '申买量一'),
   ('AskPrice1', 'd', '申卖价一'),
   ('AskVolume1', 'i', '申卖量一'),
   ('BidPrice2', 'd', '申买价二'),
   ('BidVolume2', 'i', '申买量二'),
   ('AskPrice2', 'd', '申卖价二'),
   ('AskVolume2', 'i', '申卖量二'),
   ('BidPrice3', 'd', '申买价三'),
   ('BidVolume3', 'i', '申买量三'),
   ('AskPrice3', 'd', '申卖价三'),
   ('AskVolume3', 'i', '申卖量三'),
   ('BidPrice4', 'd', '申买价四'),
   ('BidVolume4', 'i', '申买量四'),
   ('AskPrice4', 'd', '申卖价四'),
   ('AskVolume4', 'i', '申卖量四'),
   ('BidPrice5', 'd', '申买价五'),
   ('BidVolume5', 'i', '申买量五'),
   ('AskPrice5', 'd', '申卖价五'),
   ('AskVolume5', 'i', '申卖量五'),
   ('AveragePrice', 'd', '当日均价'),
   ('ActionDay', '9s', '业务日期')]


class CbtlMD(object):
    def __init__(self, fn):
        try:
            self.f = open(fn, "rb")
            self.eof = False
        except FileNotFoundError:
            self.f = None
            self.eof = True
            raise

    def __iter__(self):
        return self

    def __next__(self):
        if self.eof:
            raise StopIteration()

        buf = self.f.read(4)
        if not buf:
            self.eof = True
            raise StopIteration()

        if len(buf) < 4:
            print("truncated file")
            self.eof = True
            raise StopIteration()

        size, = struct.unpack("I", buf)

        # print("size:", size)
        buf = self.f.read(size)
        if len(buf) < size:
            print("truncated file")
            self.eof = True
            raise StopIteration()

        ts, tsu = struct.unpack("QQ", buf[:16])  # 16字节的timeval

        if buf[16:19] != b'MD\x00':
            print("bad buf")
            self.eof = True
            raise StopIteration()
        return ts, tsu, buf


def MdUnpack1(buf):
    t = struct.unpack("@9x8s63xd120x8s4xididi", buf[19:19 + 252])  # 取最后一个字段off+len
    op, latest, ts, tsm, BP1, BA1, SP1, SA1 = t
    print("+++", op, latest, ts, tsm, BP1, BA1, SP1, SA1)


def ctp_unpack(bufr:bytes, off=0):
    """
    :param struct_name: eg CThostFtdcInstrumentField
    :type struct_name: str
    :param bufr
    :type bytes

    """

    # struct_comment, struct_fields, struct_size = ctp_types.get(struct_name, (None,)*3)
    struct_fields = CThostFtdcDepthMarketDataField

    packfmt = "@"
    for field_name, fieldfmt, comment in struct_fields:
        packfmt += fieldfmt
    # print(packfmt)
    packsz = struct.calcsize(packfmt)  # struct_size may bigger than packsz because of align

    # print(struct_name, "#", struct_comment)
    a = struct.unpack(packfmt, bufr[off:off+packsz])
    d = OrderedDict()
    for i, v in enumerate(a):
        if struct_fields[i][1].endswith("s"):
            z = v.find(b"\x00")
            if z >= 0: v = v[:z]
            v = v.decode("gbk", errors="ignore")
        # print("   ", struct_fields[i][0], repr(v), "#", struct_fields[i][2])
        d[struct_fields[i][0]] = (v, struct_fields[i][2])
    return d

