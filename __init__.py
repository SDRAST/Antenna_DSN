import threading
import datetime
import random
import time
import os
try:
    import queue as queue
except ImportError as err:
    import queue

#from support.pyro import asyncs
import Pyro5.api
import support.hdf5_util as hdf5
import MonitorControl as MC

class CallbackHandler(object):
    """
    """
    @Pyro5.api.expose
    @Pyro5.api.callback
    def finished(self, duration):
        """
        """
        print("finished: %d" % duration)
        
class DSN_Antenna(MC.Antenna.Telescope, MC.DeviceReadThread, hdf5.HDF5Mixin):
    """
    A subclass of Telescope that can connect to a server that is currently
    running.

    Attributes:
        hardware (Proxy object): A Proxy object (Pyro5.api.Proxy) corresponding
                                 to a downstream hardware server
    """
    monitor_items = [
        "AzimuthAngle",
        "ElevationAngle",
        "AzimuthPredictedAngle",
        #"ElevationPredictedAngle",
        "ElevationPositionOffset",
        #"CrossElevationPositionOffset",
        "humidity",
        "pressure",
        "temperature",
        "windspeed",
        "winddirection",
        "precipitation",
        "total_precipitation",
    ]

    max_data_file_size = 1e9
    file_attr_keys = []

    def __init__(self, obs,
                 dss=0,
                 LO=None,
                 active=True,
                 hardware=False):
        """
        Args:
            obs (MonitorControl.Observatory): passed to superclass
            dss (int, optional): passed to superclass
            LO (object, optional): passed to superclass
            active (bool, optional): passed to superclass
            hardware (bool, optional): whether or not to connect to hardware
        """
        MC.Antenna.Telescope.__init__(self, obs, dss=dss, LO=LO, active=active)
        # thread for reading from NMC server
        MC.DeviceReadThread.__init__(
            self, self,
            self.action,
            name=self.name,
            suspend=False
        )
        hdf5.HDF5Mixin.__init__(self)
        if hardware:
            uri = "PYRO:APC@localhost:50001"
            self.hardware = Pyro5.api.Proxy(uri)  # asyncs.AsyncProxy(uri)
        else:
            self.hardware = None

        self.lock = threading.Lock()
        self._interval = 2.0
        self.data_file_obj = None
        self.data_file_name = None
        self.data_file_path = None
        self.header = {}
        self.writerQueue = queue.Queue()
        self.diskwriter = None

    def __getattr__(self, name):
        if self.hardware is not None:
            self.hardware._pyroClaimOwnership()
            return getattr(self.hardware, name)

    @property
    def interval(self):
        with self.lock:
            return self._interval

    @interval.setter
    def interval(self, new_interval):
        with self.lock:
            self._interval = new_interval

    def _action_thread_factory(self):
        return MC.ActionThread(
            self, self.write_to_data_file,
            name="DSS{}-writer".format(self.number)
        )

    def start_recording(self, interval=None):
        """
        """
        if interval is not None:
            self.interval = interval
        if not self.is_alive():
            self.start()
        if self.thread_suspend:
            self.resume_thread()
        if self.data_file_obj is None:
            self.open_data_file()
        self.diskwriter = self._action_thread_factory()
        self.diskwriter.start()

    def stop_recording(self):
        """
        """
        self.diskwriter.terminate()
        self.diskwriter.join()
        self.diskwriter = None
        if hasattr(self.data_file_obj, "close"):
            self.data_file_obj.close()
            self.data_file_obj = None
        self.suspend_thread()

    def write_to_data_file(self):
        if self.writerQueue.empty():
            self.logger.debug("write_to_data_file: writerQueue is empty")
            time.sleep(self.interval)
        try:
            data = self.writerQueue.get(timeout=10*self.interval)
        except Queue.Empty:
            return
        if self.data_file_obj is None:
            self.logger.error("write_to_data_file: no data_file_obj present")
        for monitor_item in data:
            monitor_item_value = data[monitor_item]
            current_size = self.data_file_obj[monitor_item].shape[0]
            self.data_file_obj[monitor_item][current_size-1] = \
                monitor_item_value
            self.data_file_obj[monitor_item].resize(current_size+1, axis=0)
        self.logger.debug("write_to_data_file: flushing data_file_obj")
        self.data_file_obj.flush()
        if os.path.getsize(self.data_file_path) > self.max_data_file_size:
            self.logger.debug("write_to_data_file: opening new few")
            self.open_my_data_file()

    def open_my_data_file(self):
        self.open_data_file() # this is now a superclass Device method
        self.logger.debug("open_my_data_file: base dir: %s", self.base_data_dir)
        self.logger.debug("open_my_data_file: data dir: %s", self.data_dir)
        hdf5.HDF5Mixin.open_data_file(self)
        self.initialize_data_file()
        
    def initialize_data_file(self):
        self.data_file_obj.create_dataset("timestamp",
                                          shape=(1, 1),
                                          maxshape=(None, 1),
                                          dtype="S26")
        for monitor_item in self.monitor_items:
            self.data_file_obj.create_dataset(monitor_item,
                                              shape=(1, 1),
                                              maxshape=(None, 1))
        return self.data_file_obj

    def action(self):
        """
        Action for the reader thread.
        """
        monitor_data = {}
        if self.hardware is not None:
            monitor_data.update(self.hardware.get(*self.monitor_items))
        else:
            monitor_data.update({item: random.random()
                                 for item in self.monitor_items})
        monitor_data = {item: float(monitor_data[item])
                        for item in monitor_data}
        monitor_data["timestamp"] = datetime.datetime.utcnow().strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )
        time.sleep(self.interval)
        if self.writerQueue is not None:
            self.writerQueue.put(monitor_data)
        return monitor_data
