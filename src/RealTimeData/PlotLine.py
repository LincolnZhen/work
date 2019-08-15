from PyQt5 import QtWidgets, QtCore, QtGui
import pyqtgraph as pg
import sys
import traceback
import psutil
import redis
import time
import yaml

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
    averageNames = ["None","A1s", 'A5s','A10s','A15s','A30s','A1m','A3m','A5m','A10m','A15m','A30m','A1h','A2h','A4h','A1d',""]
    keyNames = ["None","Upper","Low","LATESTMACD","BP1MACD","SP1MACD","LATESTDIF","BP1DIF","SP1DIF","LATESTDEA","BP1DEA","SP1DEA","LATEST"]
    keyTypeName = ["None"]
    colortype = [QtGui.QColor(255,0,0),QtGui.QColor(0,255,0),QtGui.QColor(0,0,255),QtGui.QColor(0,255,255),QtGui.QColor(255,0,255)]  #> colortype的数量要和nbox一致
    def __init__(self):
        super().__init__()
        self.config = Config("redis_mdld.yaml")
        self.initiateKeyTypeName()
        self.setWindowTitle("均线")
        self.main_widget = QtWidgets.QWidget()  # 创建一个主部件
        self.main_layout = QtWidgets.QGridLayout()  # 创建一个网格布局
        self.main_widget.setLayout(self.main_layout)  # 设置主部件的布局为网格
        # self.setCentralWidget(self.main_widget)  # 设置窗口默认部件

        self.selection_widget = QtWidgets.QWidget()
        self.selection_layout = QtWidgets.QGridLayout()
        self.averagetype = QtWidgets.QLabel('均线类型')
        self.keynametype = QtWidgets.QLabel('均值类型')
        self.keytype = QtWidgets.QLabel('行情类型')
        self.scaletype = QtWidgets.QLabel('缩放比例10^')
        self.movingType = QtWidgets.QLabel("平移距离")
        self.nbox = 5
        self.select_box_list = list()   #> (QComboBox,QComboBox1,QComboBox2)
        self.data_list = list()         #> list()
        self.time_list = list()         #> list()
        for i in range(self.nbox):
            self.data_list.append(list())
        for i in range(self.nbox):
            self.time_list.append(list())
        for i in range(self.nbox):
            t = QtWidgets.QComboBox()
            t.addItems(self.averageNames)
            t1 = QtWidgets.QComboBox()
            t1.addItems(self.keyTypeName)
            t2 = QtWidgets.QComboBox()
            t2.addItems(self.keyNames)
            color = self.colortype[i]
            label = QtWidgets.QLabel()
            label.setAutoFillBackground(True)
            palette = QtGui.QPalette()
            palette.setColor(QtGui.QPalette.Window, color)
            label.setPalette(palette)
            label2 = QtWidgets.QLabel()
            label2.setText("None")
            # t3 = QtWidgets.QComboBox()
            # t3.addItems(self.scale)
            s = QtWidgets.QSpinBox()
            s.setRange(0,100)
            s.setSingleStep(1)
            s1 = QtWidgets.QDoubleSpinBox()
            s1.setRange(-10.0,10.0)
            s1.setSingleStep(0.1)
            self.select_box_list.append((t,t1,t2,label,label2,s,s1))

        self.selection_widget.setLayout(self.selection_layout)
        self.selection_layout.addWidget(self.averagetype, 0, 0, 1, 1)
        self.selection_layout.addWidget(self.keytype, 0, 1, 1, 1)
        self.selection_layout.addWidget(self.keynametype, 0, 2, 1, 1)
        self.selection_layout.addWidget(self.scaletype,0,5,1,1)
        self.selection_layout.addWidget(self.movingType,0,6,1,1)

        for i in range(self.nbox):
            self.selection_layout.addWidget(self.select_box_list[i][0],i+1,0,1,1)

            self.selection_layout.addWidget(self.select_box_list[i][1],i+1,1,1,1)

            self.selection_layout.addWidget(self.select_box_list[i][2],i+1,2,1,1)

            self.selection_layout.addWidget(self.select_box_list[i][3],i+1,3,1,1)

            self.selection_layout.addWidget(self.select_box_list[i][4],i+1,4,1,1)

            self.selection_layout.addWidget(self.select_box_list[i][5],i+1,5,1,1)

            self.selection_layout.addWidget(self.select_box_list[i][6],i+1,6,1,1)

        self.main_layout.addWidget(self.selection_widget,0,0,1,1)

        self.plot_widget = QtWidgets.QWidget()  # 实例化一个widget部件作为K线图部件
        self.plot_layout = QtWidgets.QGridLayout()  # 实例化一个网格布局层
        self.plot_widget.setLayout(self.plot_layout)  # 设置K线图部件的布局层
        # self.plot_plt = pg.PlotWidget()  # 实例化一个绘图部件
        self.plot_plt2 = pg.GraphicsWindow(title="画图")
        self.time_axis = {37800:"09:30:00", 45000:"11:30:00/13:00:00", 45000+7200:"15:00:00"}
        self.stringaxis = pg.AxisItem(orientation='bottom')
        self.stringaxis.setTicks([self.time_axis.items()])
        self.plot_plt = self.plot_plt2.addPlot(axisItems={'bottom':self.stringaxis})
        self.plot_plt.showGrid(x=True, y=True)  # 显示图形网格
        self.plot_layout.addWidget(self.plot_plt2)  # 添加绘图部件到K线图部件的网格布局层
        # 将上述部件添加到布局层中
        self.main_layout.addWidget(self.plot_widget, 1, 0, 1, 1)
        # self.main_layout.addWidget(self.combobox_1,4,3,3,4)
        # self.main_layout.addWidget(self.combobox_2,7,4,4,4)
        self.setCentralWidget(self.main_widget)

        self.plot_plt.setYRange(max=10, min=0)
        self.plot_plt.setXRange(0,60*60*24,padding=0)
        self.plot_plt.setLabel(axis='left',text = '数据')
        self.plot_plt.setLabel(axis='bottom',text='时间')
        # self.plot_plt.lockXRange()
        self.conn_r = redis.Redis(host="192.168.40.134", port=6379, password="", charset='gb18030', errors="replace",
                                  decode_responses=True)
        self.currentDate = str(time.localtime().tm_year) + "-" + str(time.localtime().tm_mon) + "-" + str(time.localtime().tm_mday) #> "2019-07-11"
        self.plt_list = list()
        for i in range(self.nbox):
            self.plt_list.append(self.plot_plt.plot())
        self.last_name = list()
        for i in range(self.nbox):
            self.last_name.append([None,None,None,None,None])
        self.timer_start()

    def initiateKeyTypeName(self):
        for key in self.config.config_flist.values():
            self.keyTypeName.append(":F:F"+key)
        for key in self.config.config_slist:
            self.keyTypeName.append(":JZ:" + key)
        for key in self.config.config_optlist.items():
            self.keyTypeName.append(":OP:C"+key[0])
        for key in self.config.config_optlist.items():
            self.keyTypeName.append(":OP:P"+key[0])
        for key in self.config.config_optlist.keys():
            self.keyTypeName.append(":V:C"+key[:4]+"MV0000")
        for key in self.config.config_optlist.keys():
            self.keyTypeName.append(":V:P"+key[:4]+'MV0000')
        for key in self.config.config_optlist.keys():
            self.keyTypeName.append(":V:C"+key[:4]+'MVN050')
        for key in self.config.config_optlist.keys():
            self.keyTypeName.append(":V:P"+key[:4]+'MVN050')
        for key in self.config.config_optlist.keys():
            self.keyTypeName.append(":V:C"+ key[:4]+'MV0050')
        for key in self.config.config_optlist.keys():
            self.keyTypeName.append(":V:P"+key[:4]+'MV0050')
        for pxname, (icode_c, icode_p) in self.config.config_optlist.items():
            self.keyTypeName.append(":A5:" + pxname)
        self.keyTypeName.append(":A5:PJ")
    # 启动定时器 时间间隔秒
    def timer_start(self):
        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.get_cpu_info)
        self.timer.start(1000)

    def fill_data_time_list(self,index:int,key:int,averageType:str,keytypename:str,keyname:str,scale:int,movdis:int):
        conn_r = self.conn_r.pipeline(transaction=False)
        starttime = 37800
        prefix = "MDLD:"
        t = starttime
        while key > t:
            conn_r.hget(prefix+str(t)+keytypename,averageType+keyname)
            t = t + 1
        res = conn_r.execute()
        for i in range(len(res)):
            if res[i] != None:
                data = float(res[i])/ pow(10,scale) + movdis
                # print(data)
                time = i + starttime
                if time >= 50400:
                    time = time - (50400 - 45000)
                self.time_list[index].append(time)
                self.data_list[index].append(data)
    # 获取CPU使用率
    def get_cpu_info(self):
        try:
            prefix = 'MDLD:'
            key = int(time.time()) - int(time.mktime(time.strptime(self.currentDate + " " + "00:00:00", "%Y-%m-%d %H:%M:%S"))) + 3600 - 10# > 1563785675
            # time = key
            for i in range(self.nbox):
                averageType = self.select_box_list[i][0].currentText()
                keytypename = self.select_box_list[i][1].currentText()
                keyname = self.select_box_list[i][2].currentText()
                scale = self.select_box_list[i][5].value()
                movdis = self.select_box_list[i][6].value()
                print(i)
                if averageType == "None" or keytypename == "None" or keyname == 'None':
                    self.plt_list[i].setData(list(), list(), pen=self.colortype[i])
                    self.select_box_list[i][4].setText("None")
                    continue
                if averageType != self.last_name[i][0] or keytypename != self.last_name[i][1] or keyname != self.last_name[i][2] or scale != self.last_name[i][3] or movdis != self.last_name[i][4]:
                    self.data_list[i] = list()
                    self.time_list[i] = list()
                    self.last_name[i] = [averageType,keytypename,keyname,scale,movdis]
                    self.plt_list[i].setData(list(), list(), pen=self.colortype[i])
                if len(self.data_list[i]) == 0 or len(self.time_list[i]) == 0:
                    self.fill_data_time_list(i, key,averageType,keytypename,keyname,scale,movdis)
                data = self.getData(prefix+str(key)+keytypename,averageType+keyname)
                print(data)
                if data != None:
                    data = float(data) / pow(10, scale) + movdis
                    self.data_list[i].append(data)
                    if key >= 50400:
                        self.time_list[i].append(key - (50400 - 45000))
                    else:
                        self.time_list[i].append(key)
                # print("hh",movdis)
                print(self.data_list[i])
                print(self.time_list[i])
                self.select_box_list[i][4].setText(str(self.data_list[i][len(self.data_list[i]) - 1] - movdis) + "*10^" + str(scale))
                self.plt_list[i].setData(self.time_list[i],self.data_list[i], pen = self.colortype[i])


            # print(key)
            # data = self.conn_r.hget(prefix + str(key) + ":F:F" + "IC1909", "A5sLATEST")
            # self.data_list.append(float(data) / 10000000)
            # self.time_list.append(key)
            # print(data)
            # self.plot_plt.plot().setData(self.time_list, self.data_list, pen=self.colortype[i])

        except Exception as e:
            pass
            # print(traceback.print_exc())
    def getData(self,search_str:str,keyname:str):
        print(search_str)
        return self.conn_r.hget(search_str, keyname)

def main():
    app = QtWidgets.QApplication(sys.argv)
    gui = MainUi()
    gui.show()
    sys.exit(app.exec_())

import matplotlib.pyplot as plt

if __name__ == '__main__':

    x = [49.28727013628185, 44.56377483092248, 42.110167114987966, 35.400244635373355, 30.697026229567825,
     25.54553572668135, 20.739717973977328, 20.248097592525184, 18.67491242081672, 16.59136378291249, 16.52486426565796,
     15.486725165069103, 12.741895668417216, 7.574386567994952, 1.5249206167533993, 0.7565365168675781,
     -0.49227195475250485, 2.5235307749435307, 6.453022591374815, 8.979513073287904, 11.016170709282159,
     10.4122281652987, 10.777660893909633, 8.228895528651774, 5.500046361818909, -1.3274000949636102,
     -8.097309426113963, -12.03273913654685, -14.819383620373905, -17.471946864165364, -19.67014374332875,
     -21.327760742753743, -18.39480812699348, -16.52545997361094, -16.627532389342786, -15.082118670739234,
     -14.018536769598722, -15.259108935140073, -15.5785417265594, -15.172643865399063, -15.958078374572098,
     -15.274779732190073, -13.60809527183324, -11.189945392392575, -9.327406935371458, -7.761857922770083]
    y = [49559, 49619, 49679, 49739, 49799, 49859, 49919, 49979, 50039, 50099, 50159, 50219, 50279, 50339, 50399, 45059,
     45119, 45179, 45239, 45299, 45359, 45419, 45479, 45539, 45599, 45659, 45719, 45779, 45839, 45899, 45959, 46019,
     46079, 46139, 46199, 46259, 46319, 46379, 46439, 46499, 46559, 46619, 46679, 46739, 46799, 46800]

    print(x)
    print(y)
    plt.plot(y,x)
    plt.show()