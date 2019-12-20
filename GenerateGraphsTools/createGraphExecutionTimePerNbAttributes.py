import os
import matplotlib as mpl
if os.environ.get('DISPLAY','') == '':
    mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import time

def generateGraph(xList, yList, legendsList, graphTitle, xLabel, yLabel):
    for yElement in yList:
        yElement.insert(0, None)
        plt.plot(xList, yElement)
    plt.xticks(np.arange(5, max(xList)+1, 5.0))
    plt.title(graphTitle)
    plt.legend(legendsList)
    plt.grid(True)
    plt.xlabel(xLabel, fontsize=10)
    plt.ylabel(yLabel, fontsize=10)
    plt.savefig(os.getcwd()+"/ProducedGraphs/"+graphTitle+"_"+str(int(time.time()))+".png")
