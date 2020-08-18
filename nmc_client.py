from MonitorControl import Observatory
from MonitorControl.Antenna.DSN import DSN_Antenna
import Pyro5.api
import threading
import asyncio
import queue
import sys
import logging

logger = logging.getLogger(__name__)

class CallbackReceiver(object):
  """
  """
  def __init__(self, queue):
    """
    """
    self.logger = logging.getLogger(logger.name+".CallbackReceiver")
    self.queue = queue
    self.daemon = Pyro5.api.Daemon()
    self.uri = self.daemon.register(self)
    self.lock = threading.Lock()
    with self.lock:
            self._running = True
  
    self.thread = threading.Thread(target=self.daemon.requestLoop, 
                                   args=(self.running,) )
    self.thread.daemon = True
    self.thread.start()

  @Pyro5.api.expose
  def running(self):
    """
    Get running status of server
    """
    with self.lock:
      return self._running
  
  @Pyro5.api.expose
  @Pyro5.api.callback
  def finished(self, *args):
    """
    Note that code is executed by the remote server
    """
    print("got %d items from remote; do 'cb_q.get()' to get them"
          % len(args))
    self.queue.put(args)
    self.logger.debug("finished: put on queue:", args)
    if args[0] == 'LRM':
      self.duration = args[1:]
      return args[1:]

class CallbackQueue(queue.Queue):
  """
  """
  def __init__(self):
    #super(CallbackQueue, self).__init__(self) # causes a problem with 'put'
    queue.Queue.__init__(self)
  
  
obs = Observatory("Canberra")
dss43 = DSN_Antenna(obs, 43, hardware=True)
cb_q = CallbackQueue()
cb_receiver = CallbackReceiver(cb_q)

if __name__ == "__main__":
  logging.basicConfig()
  mainlogger = logging.getLogger()
  duration = 0
  dss43.LRM(6, cb_receiver)
  dss43.LRM(4, cb_receiver)
  dss43.LRM(2, cb_receiver)
  dss43.LRM(0.1, cb_receiver)
  #daemon.requestLoop()
