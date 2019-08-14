from PyQt5 import QtWidgets, QtCore, QtGui
import pyqtgraph as pg
import sys
import traceback
import psutil
import redis
import time
import yaml
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import  re

class Config:
    def __init__(self, fn_config:str):
        self.config_flist = self.init_flist()
        self.config_optlist = self.init_optlist()
        self.config_slist = self.init_slist(fn_config)
        # print(self.config_flist)
        # print(self.config_optlist)
        # print(self.config_slist)

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


class MainUi(QtWidgets.QMainWindow):
    def __init__(self, key:str):
        super().__init__()
        self.setWindowTitle("hhh")
        self.resize(500,600)
        self.serverAddress()
        self.key = key
        self.config = Config("redis_mdld.yaml")
        self.main_widget =QtWidgets.QWidget()
        self.main_layout = QtWidgets.QGridLayout()
        self.main_layout.setSpacing(0)
        self.main_widget.setLayout(self.main_layout)
        self.option_list = self.config.config_optlist
        self.info_list = list()
        self.date_list = set()
        for key in self.option_list:
            self.info_list.append({"name":key[:4],"xingquan":key[-5:]})
            self.date_list.add(key[:4])

        self.info_list = sorted(self.info_list,key=lambda x: x['xingquan'],reverse=True)
        self.date_list = sorted(list(self.date_list))
        self.dangyue_list = list()
        self.xiayue_list = list()
        for item in self.info_list:
            if item['name'].startswith(self.date_list[0]):
                self.dangyue_list.append(item)
            elif item['name'].startswith(self.date_list[1]):
                self.xiayue_list.append(item)
        self.titleLable1 = QtWidgets.QLabel(self.date_list[0])
        self.titleLable1.setAlignment(Qt.AlignCenter)
        self.titleLable2 = QtWidgets.QLabel(self.date_list[1])
        self.titleLable2.setAlignment(Qt.AlignCenter)

        self.c_widget = QtWidgets.QWidget()
        self.c_layout = QtWidgets.QGridLayout()
        self.c_layout.setSpacing(0)
        self.c_widget.setLayout(self.c_layout)


        # self.c_button = QtWidgets.QPushButton("更新")
        # self.c_button.clicked.connect(self.c_on_click)
        # self.c_layout.addWidget(self.c_button,1,1,1,1)

        self.c_edit_list1 = list()
        self.c_edit_list2 = list()
        self.c_label_list = list()
        self.c_dict = dict()
        for i in range(len(self.dangyue_list)):
            e = QtWidgets.QLineEdit()
            self.c_layout.addWidget(e,2+i,0,1,1)
            self.c_edit_list1.append(e)
            l = QtWidgets.QLabel(self.dangyue_list[i]["xingquan"][-4:])
            l.setFrameStyle(QtWidgets.QFrame.Panel | QtWidgets.QFrame.Sunken)
            l.setAlignment(Qt.AlignCenter)
            self.c_label_list.append(l)
            self.c_layout.addWidget(l,2+i,1,1,1)
            e = QtWidgets.QLineEdit()
            self.c_edit_list2.append(e)
            self.c_layout.addWidget(e,2+i,2,1,1)
            self.c_dict[self.dangyue_list[i]["xingquan"][-4:]] = [self.c_edit_list1[i],self.c_edit_list2[i]]


        self.p_widget = QtWidgets.QWidget()
        self.p_layout = QtWidgets.QGridLayout()
        self.p_layout.setSpacing(0)
        self.p_widget.setLayout(self.p_layout)


        # self.p_button = QtWidgets.QPushButton("更新")
        # self.p_button.clicked.connect(self.p_on_click)
        # self.p_layout.addWidget(self.p_button,1,1,1,1)
        self.p_edit_list1 = list()
        self.p_edit_list2 = list()
        self.p_label_list = list()
        self.p_dict = dict()
        for i in range(len(self.xiayue_list)):
            e = QtWidgets.QLineEdit()
            self.p_layout.addWidget(e,2+i,0,1,1)
            self.p_edit_list1.append(e)
            l = QtWidgets.QLabel(self.xiayue_list[i]["xingquan"][-4:])
            l.setFrameStyle(QtWidgets.QFrame.Panel | QtWidgets.QFrame.Sunken)
            l.setAlignment(Qt.AlignCenter)
            l.setLineWidth(3)
            self.p_label_list.append(l)
            self.p_layout.addWidget(l,2+i,1,1,1)
            e = QtWidgets.QLineEdit()
            self.p_edit_list2.append(e)
            self.p_layout.addWidget(e,i+2,2,1,1)
            self.p_dict[self.xiayue_list[i]["xingquan"][-4:]] = [self.p_edit_list1[i],self.p_edit_list2[i]]


        self.c_label1 = QtWidgets.QLabel("购")
        self.c_label1.setAlignment(Qt.AlignCenter)
        self.c_label1.setFrameStyle(QtWidgets.QFrame.Panel)
        self.c_label2 = QtWidgets.QLabel("购<行权价>沽")
        self.c_label2.setFrameStyle(QtWidgets.QFrame.Panel)
        self.c_label2.setAlignment(Qt.AlignCenter)
        self.c_label3 = QtWidgets.QLabel("沽")
        self.c_label3.setFrameStyle(QtWidgets.QFrame.Panel)
        self.c_label3.setAlignment(Qt.AlignCenter)
        self.p_label1 = QtWidgets.QLabel("购")
        self.p_label1.setAlignment(Qt.AlignCenter)
        self.p_label1.setFrameStyle(QtWidgets.QFrame.Panel)
        self.p_label2 = QtWidgets.QLabel("购<行权价>沽")
        self.p_label3 = QtWidgets.QLabel("沽")
        self.p_label3.setAlignment(Qt.AlignCenter)
        self.p_label3.setFrameStyle(QtWidgets.QFrame.Panel)
        self.c_label_widget = QtWidgets.QWidget()
        self.c_label_layout = QtWidgets.QGridLayout()
        self.c_label_widget.setLayout(self.c_label_layout)
        self.p_label_widget = QtWidgets.QWidget()
        self.p_label_layout = QtWidgets.QGridLayout()
        self.p_label_widget.setLayout(self.p_label_layout)
        self.main_layout.addWidget(self.titleLable1, 0, 0, 1, 1)
        self.main_layout.addWidget(self.titleLable2, 0, 1, 1, 1)
        self.c_label_layout.addWidget(self.c_label1, 0, 0, 1, 1)
        # self.c_label_layout.addWidget(self.c_label2, 0, 6, 1, 1)
        self.c_label_layout.addWidget(self.c_label3, 0, 12, 1, 1)
        self.p_label_layout.addWidget(self.p_label1, 0, 0, 1, 1)
        # self.p_label_layout.addWidget(self.p_label2, 0, 6, 1, 1)
        self.p_label_layout.addWidget(self.p_label3, 0, 12, 1, 1)
        key_list = self.p_r.keys(pattern="ETC:pos_delta")
        key_list = key_list + self.p_r.keys(pattern="ETC:pos_delta_fxop*")
        key_list = key_list + self.p_r.keys(pattern="ETC:pos_delta_f1op*")
        self.search_button = QtWidgets.QPushButton("查询")
        self.search_button.resize(20,20)
        self.search_button.clicked.connect(self.search)
        self.search_comboBox = QtWidgets.QComboBox()
        self.search_comboBox.addItems(key_list)

        self.update_button = QtWidgets.QPushButton("更新")
        self.update_button.clicked.connect(self.on_click)
        self.update_button.resize(20,20)
        self.c_layout.addWidget(self.update_button,1,0,1,1)
        self.c_layout.addWidget(self.search_button,1,1,1,1)
        self.c_layout.addWidget(self.search_comboBox,1,2,1,1)


        self.main_layout.addWidget(self.c_label_widget, 1, 0, 1, 1)
        self.main_layout.addWidget(self.p_label_widget,1,1,1,1)
        self.main_layout.addWidget(self.c_widget,2,0,1,1)
        self.main_layout.addWidget(self.p_widget,2,1,1,1)
        self.setCentralWidget(self.main_widget)

        # self.getData()

    @pyqtSlot()
    def search(self):
        self.key = self.search_comboBox.currentText()
        self.getData()

    @pyqtSlot()
    def on_click(self):
        inf = list()
        value = re.compile(r'^[-+]?[0-9]+$')

        for key in self.c_dict:
            if value.match(self.c_dict[key][0].text()):
                s = "购" + str(int(self.date_list[0][-2:])) + "月" + key
                v = int(self.c_dict[key][0].text())
                inf.append((s,v))
            if value.match(self.c_dict[key][1].text()):
                s = "沽" + str(int(self.date_list[0][-2:])) + "月" + key
                v = int(self.c_dict[key][1].text())
                inf.append((s, v))
        for key in self.p_dict:
            if value.match(self.p_dict[key][0].text()):
                s = "购" + str(int(self.date_list[1][-2:])) + "月" + key
                v = int(self.p_dict[key][0].text())
                inf.append((s, v))
            if value.match(self.p_dict[key][1].text()):
                s = "沽" + str(int(self.date_list[1][-2:])) + "月" + key
                v = int(self.p_dict[key][1].text())
                inf.append((s, v))


        self.p_r.set(self.key,str(inf))

    def serverAddress(self):
        config = "config.yaml"
        prod = "fr_products.yaml"
        serv = "fr_servers.yaml"
        product = ""
        with open(config, 'rb') as file:
            cont = file.read()
            res = yaml.load(cont, Loader=yaml.FullLoader)
            # print(res)
            product = res['default']['product']

        server_td = ""
        with open(prod,"rb") as file:
            cont = file.read()
            res = yaml.load(cont,Loader=yaml.FullLoader)
            # print(res)
            server_td = res[product]["server_td"]

        tdr = ""
        with open(serv, "rb") as file:
            cont = file.read()
            res = yaml.load(cont, Loader=yaml.FullLoader)
            # print(res)
            tdr = res["TD"][server_td]['tdr']
            # print(tdr)
        host = tdr.split(":")[0]
        port = tdr.split(":")[1]
        db = tdr.split(":")[2]
        # print(host,port,db)
        self.p_r = redis.Redis(host=host, port=port, password="", db=db, charset='gb18030', errors="replace",
                                  decode_responses=True)


    def getData(self):
        p_r = self.p_r
        x = eval(p_r.get(self.key))
        for item in x:
            type = item[0][0]
            month = item[0][1:-5]
            xingquan = item[0][-4:]
            money = str(item[1])
            if int(month) == int(self.date_list[0][-2:]):
                if type == '购':
                    self.c_dict[xingquan][0].setText(money)
                elif type == '沽':
                    self.c_dict[xingquan][1].setText(money)
            elif int(month) == int(self.date_list[1][-2:]):
                if type == '购':
                    self.p_dict[xingquan][0].setText(money)
                elif type == '沽':
                    self.p_dict[xingquan][1].setText(money)

        # print(x)
        return x



def main():
    app = QtWidgets.QApplication(sys.argv)
    gui = MainUi("")
    # gui.serverAddress()
    gui.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()