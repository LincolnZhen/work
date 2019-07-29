import redis
from redis_flist import Config

def init_optlist():
    conn = redis.Redis(host="168.36.1.170", port=6379, password="", charset='gb18030', errors='replace',
                       decode_responses=True)
    keys = conn.keys()

    re = dict()
    num = 0
    for key in keys:
        if key.startswith("OPLST:01"):
            num = num + 1
            print(key)
            code = conn.hget(key,'InstrumentCode')
            re_key = code[7:]
            if not re_key in re.keys():
                re[re_key] = ['','']
            if code[6] == 'P':
                re[re_key][1] = conn.hget(key,'InstrumentID')
            if code[6] == 'C':
                re[re_key][0] = conn.hget(key,'InstrumentID')
    print(num)
    return re
def main():
    config = Config(config_optlist=init_optlist())
    print(config.config_optlist)

if __name__ == '__main__':
    main()