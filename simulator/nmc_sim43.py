"""
Module to provide DSN antenna simulator

In Python3, antenna commands are ASCII strings.  The socket module now requires
byte objects, not unicode strings. So we need to declare the commands to be
byte objects.
"""
import time
import os
import datetime
import logging
import threading
import socketserver
import math
import queue

import ephem

from support.pyro import util # for pausable thread
from support.test import auto_test
from support.weather import get_current_weather
from Astronomy.Ephem import SerializableBody
from MonitorControl.Configurations.antenna import wrap as antenna_wrap
from Astronomy.DSN_coordinates import DSS, DSSLocation
# from MonitorControl.Configurations.antenna import slew


class OffsetRateThread(util.PausableThread):
    """
    Thread to compute accumulated rate offsets
    """
    def __init__(self, parent, axis):
        super(OffsetRateThread, self).__init__()
        self.parent = parent
        self.axis = axis

    def run(self):
        self._running_event.set()
        rate = self.parent.get_offset_rate(self.axis)
        while self._running_event.is_set():
            if self._stop_event.is_set():
                self._running_event.clear()
                break
            current_accumulated_offset = self.parent.get_accum_offset(self.axis)
            self.parent.logger.debug(
                     "OffsetRateThread: accumulated offset:{}, rate: {}".format(
                                              current_accumulated_offset, rate))
            current_accumulated_offset += rate
            self.parent.set_accum_offset(self.axis, current_accumulated_offset)
            time.sleep(1.0)
        self._running_event.clear()


class WeatherThread(util.PausableThread):

    def __init__(self, parent, update_rate=20.0, queue=None):
        super(WeatherThread, self).__init__()
        self.parent = parent
        self.update_rate = update_rate
        self.queue = None
        self.convert = 180./math.pi
        dss_location = DSSLocation(self.parent.number)
        self.lon = dss_location.lon.value
        self.lat = dss_location.lat.value

    def run(self):
        self._running_event.set()
        while self._running_event.is_set():
            if self._stop_event.is_set():
                self._running_event.clear()

            try:
                weather = get_current_weather(self.lat, self.lon).json()
                self.parent.logger.debug("WeatherThread: weather: {}".format(weather))
                with self.parent.lock:
                    self.parent.temperature = weather["main"]["temp"]
                    self.parent.pressure = weather["main"]["pressure"]
                    self.parent.humidity = weather["main"]["humidity"]
                    self.parent.windspeed = weather["wind"]["speed"]
                    self.parent.winddirection = weather["wind"]["deg"]

            except Exception as err:
                self.parent.logger.error("WeatherThread: Couldn't get weather: {}".format(err))
                if self.queue is not None:
                    self.queue.put(err)

            time.sleep(self.update_rate)


class PointThread(util.PausableThread):

    def __init__(self, parent, coord,
                    coord_type="RADEC",
                    slew_rate=0.23,
                    update_rate=1.0,
                    update_display_rate=5.0):
        """
        Must supply coords in degrees, not radians.
        This is the form that APCA accepts.
        """
        super(PointThread, self).__init__()
        self.parent = parent
        self.initial_coord = coord
        self.update_rate = update_rate
        self.update_display_rate = update_display_rate
        self.slew_rate = slew_rate

        observer = parent.copy()
        convert = 180. / math.pi
        calc_commanded_azel = None

        if coord_type not in ["AZEL","RADEC"]:
            raise RuntimeError("Need to specify a coordinate type that is either RADEC or AZEL")
        if coord_type == "AZEL":
            raise NotImplementedError("AZEL not implemented at the moment")
        elif coord_type == "RADEC":
            body = SerializableBody()
            body._ra = float(coord[0]) / convert # covnert from degrees to radians
            body._dec = float(coord[1]) / convert
            body._epoch = ephem.now()
            observer.epoch = ephem.now()
            self.parent.logger.debug("PointThread: body._ra: {}, body._dec: {}".format(
                convert*body._ra, convert*body._dec
            ))

            def calc_commanded_azel():
                """The values calculated by this function are actually slighty off what they should be"""
                observer.date = ephem.now()
                body.compute(observer)
                return body.az*convert, body.alt*convert

        if calc_commanded_azel is not None:
            self.calc_commanded_azel = calc_commanded_azel
        else:
            raise RuntimeError("Need means of calculating current source Az/El")

        self.current_time = time.time()

    def run(self):
        def get_direction(diff):
            direction = 1
            if diff < 0:
                direction = -1
            return direction

        self._running_event.set()

        while self._running_event.is_set():
            if self._stop_event.is_set():
                self.parent.logger.debug("PointThread: stopping thread")
                self._running_event.clear()

            commanded_az, commanded_el = self.calc_commanded_azel()
            with self.parent.lock:
                current_point = self.parent.point

            current_point["AZ"]["predicted"] = commanded_az
            current_point["EL"]["predicted"] = commanded_el

            slew_amount = self.slew_rate * self.update_rate
            for axis in ["AZ","EL"]:
                diff = current_point[axis]["predicted"] - current_point[axis]["actual"]
                if abs(diff) > self.slew_rate:
                    current_point[axis]["actual"] += get_direction(diff)*self.slew_rate
                else:
                    current_point[axis]["actual"] = current_point[axis]["predicted"]

            if (time.time() - self.current_time) > self.update_display_rate:
                self.parent.logger.debug("current_point: {}".format(current_point))
                self.current_time = time.time()
            with self.parent.lock:
                self.parent.point = current_point

            time.sleep(self.update_rate)


class SimulatedAntenna(DSS):

    def __init__(self, number, logger=None):
        super(SimulatedAntenna, self).__init__(number)
        self.onsource_counter = 0
        self.offsets = {
            "EL": {
                "accumulated": 0.0,
                "offset": 0.0,
                "rate": 1.0
            },
            "XEL": {
                "accumulated": 0.0,
                "offset": 0.0,
                "rate": 1.0
            }
        }
        self.point = {
            "AZ": {
                "predicted": antenna_wrap[self.number]['wrap']['center'],
                "actual":    antenna_wrap[self.number]['wrap']['center']
            },
            "EL": {
                "predicted": 88.0,
                "actual":    88.0
            }
        }
        self.wrap = 0
        self.temperature = 0
        self.pressure = 0
        self.humidity = 0
        self.windspeed = 0
        self.winddirection = 0
        self.precipitation = 0
        self.WxHr = 0
        self.WxMin = 0
        self.WxSec = 0

        self.lock = threading.Lock()
        self.offset_rate_thread = None
        self.point_antenna_thread = None
        self.weather_thread = None

        self.commands = {
            b"ANTENNA": self.antenna_cmd,
            b"PARAM": self.get,
            b"GET_WEATHER": self.get_weather,
            b"GET_OFFSETS": self.get_offsets,
            b"GET_AZEL": self.get_azel,
            b"GET_HADEC": self.get_hadec,
            b"TERMINATE": self.terminate,
            b"ONSOURCE": self.onsource,
            b"WAITONSOURCE": self.wait_on_source,
            b"GET_COMMANDS": self.get_commands,
            b"GET_PARAMS": self.get_params,
            b"ATTEN": self.atten
        }
        self.antenna_subcommands = {
            b"HI": self.antenna_hi,
            b"RO": self.antenna_ro,
            b"PO": self.antenna_po,
            b"CLR": self.antenna_clr,
            b"TRK": self.antenna_trk,
            b"MOVE": self.antenna_move,
            b"SEMN": self.antenna_semn,
            b"RADEC": self.antenna_radec,
            b"AZEL": self.antenna_azel
        }

        self.params = {
            b"temperature": lambda: self.temperature,
            b"pressure": lambda: self.pressure,
            b"windspeed": lambda: self.windspeed,
            b"winddirection": lambda: self.winddirection,
            b"precipitation": lambda: self.precipitation,
            b"total_precipitation": lambda: self.precipitation,
            b"humidity": lambda: self.humidity,
            b"WxHr": lambda: self.WxHr,
            b"WxMin": lambda: self.WxMin,
            b"WxSec": lambda: self.WxSec,
            b"AzimuthPredictedAngle": lambda: self.point["AZ"]["predicted"],
            b"AzimuthAngle": lambda: self.point["AZ"]["actual"],
            b"ElevationPredictedAngle": lambda: self.point["EL"]["predicted"],
            b"ElevationAngle": lambda: self.point["EL"]["actual"],
            b"WRAP": lambda: self.wrap,
            b"AzimuthTrackingError": lambda: 0,
            b"ElevationTrackingError": lambda: 0,
            b"Status": lambda: "operational",
            b"AxisAngleTime": lambda: 0,
            b"ElevationManualOffset": lambda:  self.get_accum_offset("EL"),
            b"ElevationPositionOffset": lambda: self.get_offset("EL"),
            b"ElevationAccumulatedRateOffset": lambda: self.get_accum_offset("EL"),
            b"CrossElevationManualOffset": lambda: self.get_accum_offset("XEL"),
            b"CrossElevationPositionOffset": lambda: self.get_offset("XEL"),
            b"CrossElevationAccumulatedRateOffset": lambda: self.get_accum_offset("XEL"),
            b"AzimuthAngleWrap": lambda: self.wrap,
            b"ANGTIME": lambda: "{} {} {}".format(time.time(), 18000000, 0),
            b"CurrentDoyTime": lambda: datetime.datetime.utcnow().strftime("%Y-%j-%H:%M:%S.%f")
        }
        if logger is None:
            logger = logging.getLogger(__name__+".SimulatedAntenna")
        self.logger = logger

    @auto_test(returns="ERROR", args=(["FOO"]))
    def process_request(self, request_str):
        """
        validate the commands
        
        Attribute ``commands`` is a dict which maps a command to a method.
        This splits the command string into command and arguments. It then
        calls that method with the arguments.
        """
        self.logger.debug(
            "process_request: called with request_str: {}".format(request_str))
        self.logger.debug("process_request: 'request_str' is %s", type(request_str))
        try:
          self.logger.debug("process_request: making request list")
          split = [r.strip() for r in request_str.split(b" ")]
        except Exception as details:
          self.logger.error("process_request: failed: %s", details)
          split = ["ERROR"]
        self.logger.debug("process_request: split command: %s", split)
        # This is the command
        first = split[0].upper()
        for cmd in self.commands:
            cb = self.commands[cmd]
            if cmd == first:
                self.logger.debug("process_request: found match for: %s",cmd)
                # invoke the method appropriate to the command.
                return cb(split)
        self.logger.debug("process_request: no match found")
        return "ERROR"

    @auto_test(returns=str, args=(["ANTENNA", "MOVE"],))
    def antenna_move(self, request_list):
        self.logger.debug("antenna_move: called")
        return "COMPLETED"

    @auto_test(returns=str, args=(["ANTENNA", "SEMN"],))
    def antenna_semn(self, request_list):
        self.logger.debug("antenna_semn: called")
        return "COMPLETED"

    @auto_test(returns=str, args=(["ANTENNA", "TRK"],))
    def antenna_trk(self, request_list):
        self.logger.debug("antenna_trk: called")
        return "COMPLETED"

    @auto_test(returns=str, args=(["ANTENNA", "HI"],))
    def antenna_hi(self, request_list):
        self.logger.debug("antenna_hi: called")
        return "COMPLETED"

    @auto_test(returns=str, args=(["ANTENNA", "RO", "EL", "5.0"],))
    def antenna_ro(self, request_list):
        self.logger.debug("antenna_ro: called")
        resp = "REJECTED"
        if len(request_list) == 4:
            axis, rate = request_list[2:]
            self.logger.debug("antenna_ro: Setting offset rate in {} to {}".format(axis, rate))
            self.set_rate(axis, rate)
            resp = "COMPLETED"
        return resp

    @auto_test(returns=str, args=(["ANTENNA", "PO", "EL", "5.0"],))
    def antenna_po(self, request_list):
        self.logger.debug("antenna_po: called")
        resp = "REJECTED"
        if len(request_list) == 4:
            axis, offset = request_list[2:]
            self.logger.debug(
                "antenna_po: Setting offset in {} to {}".format(axis, offset))
            self.set_offset(axis, offset)
            resp = "COMPLETED"
        return resp

    @auto_test(returns=str, args=(["ANTENNA","CLR"],))
    def antenna_clr(self, request_list):
        self.logger.debug("antenna_clr: called")
        resp = "REJECTED"
        if len(request_list) > 2:
            if request_list[2] == "RO":
                self.logger.debug(
                    "antenna_clr: Clearing position rate offsets")
                for direction in self.offsets:
                    self.set_rate(direction, 0.0)
                if self.offset_rate_thread is not None:
                    self.offset_rate_thread._stop_event.set()
                    self.offset_rate_thread.join()
                resp = "COMPLETED"
            elif request_list[2] == "PO":
                self.logger.debug("antenna_clr: Clearing position offsets")
                for direction in self.offsets:
                    self.offsets[direction]["offset"] = 0.0
                    self.offsets[direction]["accumulated"] = 0.0
                resp = "COMPLETED"
        return resp

    # @auto_test(returns=str, args=(["ANTENNA RADEC"]))
    def antenna_radec(self, request_list):
        self.logger.debug("antenna_radec: called")
        resp = "REJECTED"
        if len(request_list) == 4:
            ra, dec = request_list[2], request_list[3]
            self.point_antenna(ra, dec)
            resp = None
        return resp

    def antenna_azel(self, request_list):
        raise NotImplementedError

    @auto_test(returns="REJECTED", args=(["FOO"],))
    def antenna_cmd(self, request_list):
        self.logger.debug("antenna_cmd: called")
        if len(request_list) > 1:
            second = request_list[1].upper()
            for cmd in self.antenna_subcommands:
                cb = self.antenna_subcommands[cmd]
                if cmd == second:
                    return cb(request_list)
        else:
            return "REJECTED"

    @auto_test(returns=str, args=(["PARAM", "AzimuthAngle"],))
    def get(self, request_list):
        self.logger.debug(
            "get: called with request_list: {}".format(request_list))
        requested_params = request_list[1:]
        resp = ", ".join([str(self.params[p]()) for p in requested_params
                          if p in self.params])
        self.logger.debug("get: returning {}".format(resp))
        return resp

    @auto_test(returns=str, args=(["GET_WEATHER"],))
    def get_weather(self, request_list):
        self.logger.debug("get_weather: called")
        return "COMPLETED {} {} {} {} {} {} {}:{}:{}".format(
            self.temperature,
            self.pressure,
            self.humidity,
            self.windspeed,
            self.winddirection,
            self.precipitation,
            self.WxHr,
            self.WxMin,
            self.WxSec
        )

    @auto_test(returns=str, args=(["GET_OFFSETS"],))
    def get_offsets(self, request_list):
        self.logger.debug("get_offsets: called")
        return "COMPLETED {} {} {} {}".format(self.get_offset("EL"), 0, self.get_offset("XEL"), 0)

    @auto_test(returns=float, args=("EL",))
    def get_offset(self, axis):
        self.logger.debug("get_offset: called")
        with self.lock:
            return self.offsets[axis]["offset"]

    @auto_test(returns=float, args=("EL",))
    def get_accum_offset(self, axis):
        self.logger.debug("get_accum_offset: called")
        with self.lock:
            return self.offsets[axis]["accumulated"]

    @auto_test(returns=float, args=("EL",))
    def get_offset_rate(self, axis):
        self.logger.debug("get_offset_rate: called")
        with self.lock:
            rate = self.offsets[axis]["rate"]
            self.logger.debug("get_offset_rate: axis: {}, rate: {}, type(rate): {}".format(axis, rate, type(rate)))
            return rate

    def set_rate(self, axis, rate):
        axis = axis.upper()
        self.logger.debug(
            "set_rate: changing rate from {} to {} on axis {}".format(
                self.offsets[axis]["rate"], rate, axis
            )
        )
        if axis not in list(self.offsets.keys()):
            self.logger.debug("Can't change offset for axis type {}".format(axis))
            return
        with self.lock:
            self.offsets[axis]["rate"] = float(rate)
        if self.offset_rate_thread is not None:
            self.offset_rate_thread._stop_event.set()
            self.offset_rate_thread.join()
        self.offset_rate_thread = self._start_thread(OffsetRateThread, thread_args=(self,axis))

    def set_offset(self, axis, offset):
        offset = float(offset)
        axis = axis.upper()
        self.logger.debug(
            "set_offset: changing offset from {} to {} on axis {}".format(
                self.get_offset(axis), offset, axis
            )
        )
        if axis not in list(self.offsets.keys()):
            self.logger.error("set_offset: can't change offset for axis type {}".format(axis))
            return

        with self.lock:
            self.offsets[axis]["offset"] = offset
            self.offsets[axis]["accumulated"] = offset

    def set_accum_offset(self, axis, offset):
        offset = float(offset)
        axis = axis.upper()
        self.logger.debug(
            "set_offset: changing accumulated offset from {} to {} on axis {}".format(
                self.get_accum_offset(axis), offset, axis
            )
        )
        if axis not in list(self.offsets.keys()):
            self.logger.error("set_offset: can't change offset for axis type {}".format(axis))
            return

        with self.lock:
            self.offsets[axis]["accumulated"] = offset

    @auto_test(returns=str, args=(["GET_AZEL"],))
    def get_azel(self, request_list):
        self.logger.debug("set_azel: called")
        timestamp = time.mktime(datetime.datetime.utcnow().timetuple())
        return "COMPLETED {} {} {} {}".format(self.point["AZ"]["actual"], self.point["EL"]["actual"], self.wrap, timestamp)

    @auto_test(returns=str, args=(["GET_HADEC"],))
    def get_hadec(self, request_list):
        self.logger.debug("get_hadec: called")
        return "COMPLETED {} {} {} {}".format(0,0,0,0)

    @auto_test(returns=str, args=(["TERMINATE"],))
    def terminate(self, request_list):
        self.logger.debug("terminate: called")
        return "COMPLETED"

    @auto_test(returns=str, args=(["ONSOURCE"],))
    def onsource(self, request_list):
        self.logger.debug("onsource: called")
        if self.onsource_counter >=3:
            self.onsource_counter = 0
            return "COMPLETED ONSOURCE"
        else:
            self.onsource_counter += 1
            return "COMPLETED SLEWING"

    @auto_test(returns=str, args=(["WAITONSOURCE"],))
    def wait_on_source(self, request_list):
        self.logger.debug("wait_on_source: called")
        return "COMPLETED"

    @auto_test(returns=str, args=(["GET_COMMANDS"],))
    def get_commands(self, request_list):
        self.logger.debug("get_commands: called")
        return ", ".join(list(self.commands.keys()))

    @auto_test(returns=str, args=(["GET_PARAMS"],))
    def get_params(self, request_list):
        self.logger.debug("get_params: called with: %s", request_list)
        params_list = b", ".join(list(self.params.keys()))
        self.logger.debug("get_params: list: %s", params_list)
        return params_list

    def atten(self, request_list):
        self.logger.debug("atten: called")
        return "COMPLETED"

    def point_antenna(self, ra, dec):
        self.logger.debug("point_antenna: ra: {}, dec: {}".format(ra, dec))

        if self.point_antenna_thread is not None:
            self.point_antenna_thread._stop_event.set()
            self.point_antenna_thread.join()

        self.point_antenna_thread = self._start_thread(PointThread,
                            thread_args=(self, [ra, dec]),
                            thread_kwargs={"coord_type":"RADEC"})


    def start_weather_thread(self):

        if self.weather_thread is not None:
            self.weather_thread._stop_event.set()
            self.weather_thread.join()

        self.weather_thread = self._start_thread(WeatherThread, thread_args=(self,), thread_kwargs={"update_rate":300})

    def _start_thread(self, thread_cls, thread_args=None, thread_kwargs=None):
        if thread_args is None: thread_args = ()
        if thread_kwargs is None: thread_kwargs = {}
        t = thread_cls(*thread_args, **thread_kwargs)
        t.daemon=True
        t.start()
        return t

antenna = SimulatedAntenna(43)

class NMCRequestHandler(socketserver.BaseRequestHandler):

    def __init__(self, request, client_address, server):
        self.logger = logging.getLogger(__name__+".NMCRequestHandler")
        self.break_char = "\n".encode()
        self.recv_val = 8
        socketserver.BaseRequestHandler.__init__(self, request, client_address,
                                                       server)

    def handle(self):
        self.logger.debug("handle: called.")
        while True:
            data = self.request.recv(self.recv_val)
            self.logger.debug("handle: got: %s", data)
            if not data:
                break
            accum = data
            while data.find(self.break_char) == -1:
                data = self.request.recv(self.recv_val)
                accum += data
            self.logger.debug("handle: received {} from client at {}".format(
                               accum, time.strftime("%H:%M:%S", time.gmtime())))
            try:
                # this is the response from the command
                resp = antenna.process_request(accum)
                self.logger.debug("handle: resp is %s", type(resp))
                self.logger.debug("handle: resp=%s", resp)
                if type(resp) == bytes:
                  self.request.send(resp)
                else:
                  self.request.send(bytes(resp, encoding='utf-8'))
                self.logger.debug("handle: sent {} to client at {}".format(
                                resp, time.strftime("%H:%M:%S", time.gmtime())))
            except Exception as err:
                self.logger.error("handle: error processing request: {}".format(err))

class NMCServer(socketserver.ThreadingMixIn, socketserver.TCPServer):

    def __init__(self, server_address, handler_class=NMCRequestHandler):
        self.logger = logging.getLogger(__name__+".NMCServer")
        # SocketServer.ThreadingMixIn.__init__(self)
        socketserver.TCPServer.__init__(self, server_address, handler_class)
        self.logger.info("__init__: waiting on %s", address)

    # def server_activate(self):
    #     self.logger.debug('server_activate')
    #     return super(NMCServer, self).server_activate()

    def serve_forever(self):
        self.logger.debug('waiting for request')
        self.logger.info('serve_forever: handling requests, press <Ctrl-C> to quit')
        while True:
            self.handle_request()

    def handle_request(self):
        self.logger.debug('handle_request: called.')
        return socketserver.TCPServer.handle_request(self)

def create_arg_parser():
    import argparse
    parser = argparse.ArgumentParser(description="Fire up NMC simulator")
    parser.add_argument("--verbose", "-v", dest="verbose", required=False, action="store_true",
                        help="In verbose mode, the log level is DEBUG")
    parser.add_argument("--log_dir", "-ld", dest="log_dir", required=False, action="store",
                        default="/usr/local/logs/dss43", help="Specify the log directory")
    return parser

if __name__ == "__main__":
    from support.logs import setup_logging
    parsed = create_arg_parser().parse_args()
    log_dir = parsed.log_dir
    if not os.path.exists(log_dir):
        log_dir = os.path.dirname(os.path.abspath(__file__))
    level = logging.INFO
    if parsed.verbose:
        level = logging.DEBUG
    antenna.logger.setLevel(level)
    # antenna.point_antenna(194.284850, -5.888186)
    # while True:
    #     time.sleep(0.1)
    # antenna.point_antenna(204.659698178, -13.0484309367)
    # antenna.set_rate("EL",1.0)
    # time.sleep(3.0)
    # antenna.set_rate("EL",-5.0)
    # antenna.antenna_clr(["ANTENNA", "CLR", "PO"])
    # antenna.antenna_clr(["ANTENNA", "CLR", "RO"])
    # while True:
    #     pass
    # antenna.start_weather_thread()
    timestamp = datetime.datetime.utcnow().strftime("%Y-%j-%Hh%Mm%Ss")
    setup_logging(
        logLevel=level,
        logfile=os.path.join(log_dir, "NMC_sim43_{}.log".format(timestamp))
    )
    address = ("localhost",6743)
    server = NMCServer(address, NMCRequestHandler)
    server.serve_forever()
