from AverageLine2VVV import MACDControl

if __name__ == '__main__':

    # conn_r = redis.Redis(host="192.168.40.134", port = 6379, password="", charset='gb18030',errors="replace",
    #                          decode_responses=True)

    macdC = MACDControl([["09:30:01","11:30:01"],["13:00:01","15:00:01"]],60)
    # macdC.start()
    macdC.AfterCloseStart(64800,45000,45000+60*35)
