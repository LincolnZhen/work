import redis

# 普通连接
conn = redis.Redis(host="192.168.40.129", port=6379,password="",charset='gb18030', errors='replace', decode_responses=True)
conn.set("x1","hello",ex=5) # ex代表seconds，px代表ms
val = conn.get("x1")
print(val)
