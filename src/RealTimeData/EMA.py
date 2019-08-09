import redis
import time
from collections import deque
import yaml
import random
import matplotlib.pyplot as plt

diff = [-1,0,1]

def genRandom(startint:int):
    startint = startint + diff[random.randint(0,2)]
    return startint

def computeAverage(l:list):
    sum = 0
    for i in range(len(l)):
        sum = sum + l[i]
    return sum / len(l)


def main():
    data = list()
    i = 0
    N = 200
    a10 = deque(maxlen=N)
    re10 = [0] * 10000
    ema = [0]*10000
    fema = 0
    ifcount = True
    ema2 = [0] * 10000
    start = 100
    ema3 = [0] * 10000

    for i in range(10000):
        d = genRandom(start)
        start = d
        data.append(d)
        a10.append(d)
        if len(a10) == N:
            re10[i] = computeAverage(a10)
        if len(a10) == N and ifcount:
            fema = computeAverage(a10)
            ema[i] = fema
            ema2[i] = fema
            ema3[i] = fema
            ifcount = False
        else:
            ema[i] = ema[i-1] * (N-1) + d * 2
            ema[i] = ema[i] / (N+1)
            ema2[i] = ema2[i-1] * (N-1) + d
            ema2[i] = ema2[i] / N
            ema3[i] = ema3[i-1] * (N-1) + d * 1.618
            ema3[i] = ema3[i] / (N+0.618)
    plt.plot(data, color ='r')
    plt.plot(re10, color = 'g')
    plt.plot(ema, color = 'b')
    plt.plot(ema2,color = 'm')
    plt.plot(ema3,color = 'k')
    plt.show()

if __name__ == '__main__':
    main()
