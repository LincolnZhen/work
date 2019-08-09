from pyqtgraph.Qt import QtGui, QtCore
import pyqtgraph as pg

import collections
import random
import time
import math
import numpy as np
import redis

class DynamicPlotter():

    def __init__(self, sampleinterval=1, timewindow=10., size=(600,350)):
        # Data stuff
        self._interval = int(sampleinterval*1000)
        self._bufsize = int(60*60*24)
        self.databuffer = collections.deque([0.0]*self._bufsize, self._bufsize)
        self.x = np.linspace(0, 60*60*24, self._bufsize)
        self.y = np.zeros(self._bufsize, dtype=np.float)
        self.conn_r = redis.Redis(host="192.168.40.134", port=6379, password="", charset='gb18030', errors="replace",
                                  decode_responses=True)
        self.currentDate = str(time.localtime().tm_year) + "-" + str(time.localtime().tm_mon) + "-" + str(time.localtime().tm_mday) #> "2019-07-11"
        # PyQtGraph stuff
        self.app = QtGui.QApplication([])
        self.plt = pg.plot(title='Dynamic Plotting with PyQtGraph')
        # self.plt.resize(*size)
        self.plt.showGrid(x=True, y=True)
        self.plt.setLabel('left', 'amplitude', 'V')
        self.plt.setLabel('bottom', 'time', 's')
        self.plt.setXRange(0, 60*60*24)
        self.curve = self.plt.plot(self.x, self.y, pen=(255,0,0))
        # QTimer
        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.updateplot)
        self.timer.start(1000)

    def getdata(self):
        prefix = "MDLD:"
        key = int(time.time())-int(time.mktime(time.strptime(self.currentDate + " " + "00:00:00" , "%Y-%m-%d %H:%M:%S"))) + 3600 - 1
        new = self.conn_r.hget(prefix + str(key) + ":F:F" + "IC1909",'A30sLATEST')
        return key, new

    def updateplot(self):
        key, new = self.getdata()
        print(key,new)
        self.y[key - 3600] = new
        self.curve.setData(self.x, self.y)
        self.app.processEvents()

    def run(self):
        self.app.exec_()

if __name__ == '__main__':

    m = DynamicPlotter(sampleinterval=0.05, timewindow=10.)
    m.run()
