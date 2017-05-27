from multiprocessing import Process, Manager
import time
class AvgProc(Process):
    def __init__ (self, inAvgItemFeature):
        Process.__init__(self)
        # Load server values
        self.avgItemFeature = inAvgItemFeature
    
    def run(self):
        if self.avgItemFeature.value == 1:
            print 't'

if __name__ == '__main__': 
    manager = Manager()      
    avgNowStep = manager.Value('i', 1)
    avg = AvgProc(avgNowStep)
    avg.start()
    while True:
        time.sleep(1)