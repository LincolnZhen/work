from UpperLower2VVV import  UpperLowControl, Flux
import redis

conn_r = redis.Redis(host="192.168.40.134", port=6379, password="", charset='gb18030', errors="replace",
                     decode_responses=True)
f = Flux(conn_r)

upplow = UpperLowControl(f,[["09:30:00","11:30:00"],["13:00:00","16:00:00"]],'A5m',0.95,0.05,"A5 0")
upplow.start()
