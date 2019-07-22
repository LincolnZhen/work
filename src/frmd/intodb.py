import os
import pickle
import re
from operator import itemgetter
import psycopg2

path = r"\\168.36.1.170\share\archives"
dirs = os.listdir(path)

conn = psycopg2.connect(database="postgres", user="postgres", password="...", host="192.168.40.129", port="5432")
cur = conn.cursor()

cur.execute("create table MDOP ("  \
            + "Trad_ExID_Ins varchar(50) primary key," \
            + "inf jsonb);"

            )
# cur.execute("create sequence MDOP_TraD " + \
#             "START WITH 1 " + \
#             "increment by 1 " + \
#             "no minvalue " + \
#             "no maxvalue " + \
#             "cache 1;")
# cur.execute("alter table MDOP alter TradingDay set default nextval('MDOP_TraD')")
# cur.execute("create sequence MDOP_ExID " + \
#             "START WITH 1 " + \
#             "increment by 1 " + \
#             "no minvalue " + \
#             "no maxvalue " + \
#             "cache 1;")
# cur.execute("alter table MDOP alter ExchangeID set default nextval('MDOP_ExID')")
# cur.execute("create sequence MDOP_InsID " + \
#             "START WITH 1 " + \
#             "increment by 1 " + \
#             "no minvalue " + \
#             "no maxvalue " + \
#             "cache 1;")
# cur.execute("alter table MDOP alter InstrumentID set default nextval('MDOP_InsID')")
conn.commit()
conn.close()