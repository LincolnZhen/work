from PyQt5 import QtWidgets, QtCore, QtGui
import sys
import redis
import yaml
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import  re
import os


class Config:
    def __init__(self, fn_config:str):
        self.config_flist = self.init_flist()
        self.config_optlist = self.init_optlist()
        self.config_slist = self.init_slist(fn_config)
        print(self.config_flist)
        print(self.config_optlist)
        print(self.config_slist)

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
    def __init__(self, key:list):
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



        self.c_edit_list1 = list()
        self.c_edit_list2 = list()
        self.c_label_list = list()
        self.c_dict = dict()
        for i in range(len(self.dangyue_list)):
            e = QtWidgets.QLineEdit()
            e.setReadOnly(True)
            self.c_layout.addWidget(e,2+i,0,1,1)
            self.c_edit_list1.append(e)
            l = QtWidgets.QLabel(self.dangyue_list[i]["xingquan"][-4:])
            l.setFrameStyle(QtWidgets.QFrame.Panel | QtWidgets.QFrame.Sunken)
            l.setAlignment(Qt.AlignCenter)
            self.c_label_list.append(l)
            self.c_layout.addWidget(l,2+i,1,1,1)
            e = QtWidgets.QLineEdit()
            e.setReadOnly(True)
            self.c_edit_list2.append(e)
            self.c_layout.addWidget(e,2+i,2,1,1)
            self.c_dict[self.dangyue_list[i]["xingquan"][-4:]] = [self.c_edit_list1[i],self.c_edit_list2[i]]


        self.p_widget = QtWidgets.QWidget()
        self.p_layout = QtWidgets.QGridLayout()
        self.p_widget.setLayout(self.p_layout)
        self.p_layout.setSpacing(0)

        self.p_edit_list1 = list()
        self.p_edit_list2 = list()
        self.p_label_list = list()
        self.p_dict = dict()
        for i in range(len(self.xiayue_list)):
            e = QtWidgets.QLineEdit()
            e.setReadOnly(True)
            self.p_layout.addWidget(e,2+i,0,1,1)
            self.p_edit_list1.append(e)
            l = QtWidgets.QLabel(self.xiayue_list[i]["xingquan"][-4:])
            l.setFrameStyle(QtWidgets.QFrame.Panel | QtWidgets.QFrame.Sunken)
            l.setAlignment(Qt.AlignCenter)
            l.setLineWidth(3)
            self.p_label_list.append(l)
            self.p_layout.addWidget(l,2+i,1,1,1)
            e = QtWidgets.QLineEdit()
            e.setReadOnly(True)
            self.p_edit_list2.append(e)
            self.p_layout.addWidget(e,i+2,2,1,1)
            self.p_dict[self.xiayue_list[i]["xingquan"][-4:]] = [self.p_edit_list1[i],self.p_edit_list2[i]]


        self.c_label1 = QtWidgets.QLabel("购")
        # self.c_label1.setFixedSize(50,20)
        self.c_label1.setAlignment(Qt.AlignCenter)
        self.c_label1.setFrameStyle(QtWidgets.QFrame.Panel)
        self.c_label2 = QtWidgets.QLabel("")
        self.c_label2.setFrameStyle(QtWidgets.QFrame.Panel)
        self.c_label2.setAlignment(Qt.AlignCenter)
        # self.c_label2.setFixedSize(140,20)
        self.c_label3 = QtWidgets.QLabel("沽")
        self.c_label3.setFrameStyle(QtWidgets.QFrame.Panel )
        self.c_label3.setAlignment(Qt.AlignCenter)
        # self.c_label3.setFixedSize(50,20)
        self.p_label1 = QtWidgets.QLabel("购")
        self.p_label1.setAlignment(Qt.AlignCenter)
        self.p_label1.setFrameStyle(QtWidgets.QFrame.Panel)
        self.p_label2 = QtWidgets.QLabel("")
        self.p_label2.setAlignment(Qt.AlignCenter)
        self.p_label2.setFrameStyle(QtWidgets.QFrame.Panel)
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
        self.c_label_layout.addWidget(self.c_label1, 0, 0, 1, 3)
        # self.c_label_layout.addWidget(self.c_label2, 0, 6, 1, 3)
        self.c_label_layout.addWidget(self.c_label3, 0, 12, 1, 3)
        self.p_label_layout.addWidget(self.p_label1, 0, 0, 1, 1)
        # self.p_label_layout.addWidget(self.p_label2, 0, 6, 1, 1)
        self.p_label_layout.addWidget(self.p_label3, 0, 18, 1, 1)
        self.main_layout.addWidget(self.c_label_widget, 1, 0, 1, 1)
        self.main_layout.addWidget(self.p_label_widget,1,1,1,1)
        self.main_layout.addWidget(self.c_widget,2,0,1,1)
        self.main_layout.addWidget(self.p_widget,2,1,1,1)
        self.edit_button = QtWidgets.QPushButton("修改")
        self.edit_button.clicked.connect(self.on_edit)
        self.main_layout.addWidget(self.edit_button,3,0,1,1)
        self.setCentralWidget(self.main_widget)
        self.timer_start()

    @pyqtSlot()
    def on_edit(self):
        os.system('python ui.exe')

    def timer_start(self):
        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.getData)
        self.timer.start(1000)

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
        for key in self.c_dict:
            self.c_dict[key][0].setText("")
            self.c_dict[key][1].setText("")

        for key in self.p_dict:
            self.p_dict[key][0].setText("")
            self.p_dict[key][1].setText("")

        for key_pattern in self.key:
            key_list = p_r.keys(pattern=key_pattern)
            print(key_list)
            value = re.compile(r'^[-+]?[0-9]+$')

            for key in key_list:
                # print(key)
                # print("..",p_r.get(key))
                x = eval(p_r.get(key))
                for item in x:
                    type = item[0][0]
                    month = item[0][1:-5]
                    xingquan = item[0][-4:]
                    money = str(item[1])
                    if int(month) == int(self.date_list[0][-2:]):
                        if type == '购':
                            if value.match(self.c_dict[xingquan][0].text()) and self.c_dict[xingquan][0].text():
                                money = int(money) + int(self.c_dict[xingquan][0].text())
                                money = str(money)
                            self.c_dict[xingquan][0].setText(money)
                        elif type == '沽':
                            if value.match(self.c_dict[xingquan][1].text()) and self.c_dict[xingquan][1].text():
                                money = int(money) + int(self.c_dict[xingquan][1].text())
                                money = str(money)
                            self.c_dict[xingquan][1].setText(money)
                    elif int(month) == int(self.date_list[1][-2:]):
                        if type == '购':
                            if value.match(self.p_dict[xingquan][0].text()) and self.p_dict[xingquan][0].text():
                                money = int(money) + int(self.p_dict[xingquan][0].text())
                                money = str(money)
                            self.p_dict[xingquan][0].setText(money)
                        elif type == '沽':
                            if value.match(self.p_dict[xingquan][1].text()) and self.p_dict[xingquan][1].text():
                                money = int(money) + int(self.p_dict[xingquan][1].text())
                                money = str(money)
                            self.p_dict[xingquan][1].setText(money)




def main():
    app = QtWidgets.QApplication(sys.argv)
    gui = MainUi(["ETC:pos_delta","ETC:pos_delta_fxop*","ETC:pos_delta_f1op*"])
    gui.show()
    # gui2 = MainUi("ETC:pos_delta_f1op*")
    # gui2.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()