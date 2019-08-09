from UpperLower2VVV import Flux, UpperLowControl
import redis

if __name__ == '__main__':
    conn_r = redis.Redis(host="168.36.1.116", port=6379, password="", charset='gb18030', errors="replace",
                         decode_responses=True)

    f = Flux(conn_r)
    # ul = UpperLowControl(f,[["08:30:00","11:30:00"],["16:22:00","17:31:00"]],10,0.9,0.1)
    # ul.start()

    ul = UpperLowControl(f, [["09:30:01", "11:30:01"], ["13:00:01", "15:00:01"]], 'A30m', 0.9, 0.1,"A5 2")
    ul.start()
