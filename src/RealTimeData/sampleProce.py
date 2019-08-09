from multiprocessing import Process
import time


class MyProcess(Process):
    def __init__(self, name):
        super(MyProcess, self).__init__()
        self.name = name

    def run(self):
        print("%s is running " % self.name)
        time.sleep(2)
        print('%s is done' % self.name)


if __name__ == '__main__':
    p = MyProcess('monicx')
    p.start()  # 就是调用run()方法。
    print('主')
