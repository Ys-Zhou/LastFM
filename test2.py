from multiprocessing import Process, Manager
import time
class AvgProc(Process):
    def __init__ (self, inAvgItemFeature):
        Process.__init__(self)
        # Load server values
        self.avgItemFeature = inAvgItemFeature
    
    def run(self):
        d = {'a':{'b':1}}
        self.avgItemFeature.update(d)
       
if __name__ == '__main__': 
    manager = Manager()      
    avgItemFeature = manager.dict()
    avg = AvgProc(avgItemFeature)
    avg.start()
    while True:
        print avgItemFeature
        time.sleep(1)
