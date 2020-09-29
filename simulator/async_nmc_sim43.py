import time
import os
import datetime
import logging
import math
import asyncio

import ephem

from MonitorControl.DSS_server_cfg import tams_config
from support.test import auto_test
from support.weather import get_current_weather
from Astronomy.Ephem import SerializableBody
from Astronomy.DSN_coordinates import DSS, DSSLocation
# from MonitorControl.Configurations.antenna import slew

module_logger = logging.getLogger(__name__)


class SimulatedAntenna(DSS):

    def __init__(self, number):
        super(SimulatedAntenna, self).__init__(number)
        self.logger = module_logger.getChild("SimulatedAntenna")
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
                "predicted": 17.0,
                "actual": 17.0
            },
            "EL": {
                "predicted": 88.0,
                "actual": 88.0
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

        self.offset_rate_task = None
        self.point_antenna_task = None
        self.weather_task = None

        self.update_rate = {
            "update_weather": 30.0,
            "point_antenna": 1.0,
            "offset_rate": 1.0,
        }

        self.commands = {
            "ANTENNA": self.antenna_cmd,
            "PARAM": self.get,
            "GET_WEATHER": self.get_weather,
            "GET_OFFSETS": self.get_offsets,
            "GET_AZEL": self.get_azel,
            "GET_HADEC": self.get_hadec,
            "TERMINATE": self.terminate,
            "ONSOURCE": self.onsource,
            "WAITONSOURCE": self.wait_on_source,
            "GET_COMMANDS": self.get_commands,
            "GET_PARAMS": self.get_params,
            "ATTEN": self.atten
        }
        self.antenna_subcommands = {
            "HI": self.antenna_hi,
            "RO": self.antenna_ro,
            "PO": self.antenna_po,
            "CLR": self.antenna_clr,
            "TRK": self.antenna_trk,
            "MOVE": self.antenna_move,
            "SEMN": self.antenna_semn,
            "RADEC": self.antenna_radec,
            "AZEL": self.antenna_azel
        }

        self.params = {
            "temperature": lambda: self.temperature,
            "pressure": lambda: self.pressure,
            "windspeed": lambda: self.windspeed,
            "winddirection": lambda: self.winddirection,
            "precipitation": lambda: self.precipitation,
            "total_precipitation": lambda: self.precipitation,
            "humidity": lambda: self.humidity,
            "WxHr": lambda: self.WxHr,
            "WxMin": lambda: self.WxMin,
            "WxSec": lambda: self.WxSec,
            "AzimuthPredictedAngle": lambda: self.point["AZ"]["predicted"],
            "AzimuthAngle": lambda: self.point["AZ"]["actual"],
            "ElevationPredictedAngle": lambda: self.point["EL"]["predicted"],
            "ElevationAngle": lambda: self.point["EL"]["actual"],
            "WRAP": lambda: self.wrap,
            "AzimuthTrackingError": lambda: 0,
            "ElevationTrackingError": lambda: 0,
            "Status": lambda: "operational",
            "AxisAngleTime": lambda: 0,
            "ElevationManualOffset": lambda:  self.get_accum_offset("EL"),
            "ElevationPositionOffset": lambda: self.get_offset("EL"),
            "ElevationAccumulatedRateOffset": lambda: self.get_accum_offset("EL"),
            "CrossElevationManualOffset": lambda: self.get_accum_offset("XEL"),
            "CrossElevationPositionOffset": lambda: self.get_offset("XEL"),
            "CrossElevationAccumulatedRateOffset": lambda: self.get_accum_offset("XEL"),
            "AzimuthAngleWrap": lambda: self.wrap,
            "ANGTIME": lambda: "{} {} {}".format(time.time(), 18000000, 0),
            "CurrentDoyTime": lambda: datetime.datetime.utcnow().strftime("%Y-%j-%H:%M:%S.%f")
        }

    def start_weather_updates(self):
        self.weather_task = asyncio.ensure_future(
            self.update_weather()
        )

    async def dispatch_command(self, request_str):
        self.logger.debug(
            "dispatch_command: called. request_str: {}".format(request_str))
        split = [r.strip() for r in request_str.split(" ")]
        first = split[0].upper()
        if first in self.commands:
            cmd_processor = self.commands[first]
            return cmd_processor(split)
        else:
            return "ERROR"

    # @auto_test(returns=str, args=(["ANTENNA", "MOVE"],))
    def antenna_move(self, request_list):
        self.logger.debug("antenna_move: called")
        return "COMPLETED"

    # @auto_test(returns=str, args=(["ANTENNA", "SEMN"],))
    def antenna_semn(self, request_list):
        self.logger.debug("antenna_semn: called")
        return "COMPLETED"

    # @auto_test(returns=str, args=(["ANTENNA", "TRK"],))
    def antenna_trk(self, request_list):
        self.logger.debug("antenna_trk: called")
        return "COMPLETED"

    # @auto_test(returns=str, args=(["ANTENNA", "HI"],))
    def antenna_hi(self, request_list):
        self.logger.debug("antenna_hi: called")
        return "COMPLETED"

    # @auto_test(returns=str, args=(["ANTENNA", "RO", "EL", "5.0"],))
    def antenna_ro(self, request_list):
        self.logger.debug("antenna_ro: called")
        resp = "REJECTED"
        if len(request_list) == 4:
            axis, rate = request_list[2:]
            self.logger.debug(
                ("antenna_ro: "
                 "Setting offset rate in {} to {}").format(axis, rate))
            if self.offset_rate_task is not None:
                self.offset_rate_task.cancel()

            self.offset_rate_task = asyncio.ensure_future(
                self.set_rate(axis, rate)
            )

            resp = "COMPLETED"
        return resp

    # @auto_test(returns=str, args=(["ANTENNA", "PO", "EL", "5.0"],))
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

    # @auto_test(returns=str, args=(["ANTENNA", "CLR"],))
    def antenna_clr(self, request_list):
        self.logger.debug("antenna_clr: called")
        resp = "REJECTED"
        if len(request_list) > 2:
            if request_list[2] == "RO":
                self.logger.debug(
                    "antenna_clr: Clearing position rate offsets")
                for direction in self.offsets:
                    self.set_rate(direction, 0.0)
                if self.offset_rate_task is not None:
                    self.offset_rate_task.cancel()
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
            if self.point_antenna_task is not None:
                self.point_antenna_task.cancel()
            self.point_antenna_task = asyncio.ensure_future(
                self.point_antenna(ra, dec)
            )
            resp = "COMPLETED"
        return resp

    def antenna_azel(self, request_list):
        raise NotImplementedError

    # @auto_test(returns="REJECTED", args=(["FOO"],))
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

    # @auto_test(returns=str, args=(["PARAM", "AzimuthAngle"],))
    def get(self, request_list):
        self.logger.debug(
            "get: called with request_list: {}".format(request_list))
        requested_params = request_list[1:]
        resp = ", ".join([str(self.params[p]()) for p in requested_params
                          if p in self.params])
        self.logger.debug("get: returing {}".format(resp))
        return resp

    # @auto_test(returns=str, args=(["GET_WEATHER"],))
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

    # @auto_test(returns=str, args=(["GET_OFFSETS"],))
    def get_offsets(self, request_list):
        self.logger.debug("get_offsets: called")
        return "COMPLETED {} {} {} {}".format(
            self.get_offset("EL"), 0, self.get_offset("XEL"), 0)

    # @auto_test(returns=float, args=("EL",))
    def get_offset(self, axis):
        self.logger.debug("get_offset: called")
        return self.offsets[axis]["offset"]

    # @auto_test(returns=float, args=("EL",))
    def get_accum_offset(self, axis):
        self.logger.debug("get_accum_offset: called")
        return self.offsets[axis]["accumulated"]

    # @auto_test(returns=float, args=("EL",))
    def get_offset_rate(self, axis):
        self.logger.debug("get_offset_rate: called")
        rate = self.offsets[axis]["rate"]
        self.logger.debug(
            ("get_offset_rate: "
             "axis: {}, "
             "rate: {}, "
             "type(rate): {}").format(axis, rate, type(rate)))
        return rate

    async def set_offset(self, axis, offset):
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

    # @auto_test(returns=str, args=(["GET_AZEL"],))
    def get_azel(self, request_list):
        self.logger.debug("set_azel: called")
        timestamp = time.mktime(datetime.datetime.utcnow().timetuple())
        return "COMPLETED {} {} {} {}".format(
            self.point["AZ"]["actual"],
            self.point["EL"]["actual"],
            self.wrap,
            timestamp
        )

    # @auto_test(returns=str, args=(["GET_HADEC"],))
    def get_hadec(self, request_list):
        self.logger.debug("get_hadec: called")
        return "COMPLETED {} {} {} {}".format(0, 0, 0, 0)

    # @auto_test(returns=str, args=(["TERMINATE"],))
    def terminate(self, request_list):
        self.logger.debug("terminate: called")
        return "COMPLETED"

    # @auto_test(returns=str, args=(["ONSOURCE"],))
    def onsource(self, request_list):
        self.logger.debug("onsource: called")
        if self.onsource_counter >= 3:
            self.onsource_counter = 0
            return "COMPLETED ONSOURCE"
        else:
            self.onsource_counter += 1
            return "COMPLETED SLEWING"

    # @auto_test(returns=str, args=(["WAITONSOURCE"],))
    def wait_on_source(self, request_list):
        self.logger.debug("wait_on_source: called")
        return "COMPLETED"

    # @auto_test(returns=str, args=(["GET_COMMANDS"],))
    def get_commands(self, request_list):
        self.logger.debug("get_commands: called")
        return ", ".join(list(self.commands.keys()))

    # @auto_test(returns=str, args=(["GET_PARAMS"],))
    def get_params(self, request_list):
        self.logger.debug("get_params: called")
        self.logger.debug("")
        return ", ".join(list(self.params.keys()))

    def atten(self, request_list):
        self.logger.debug("atten: called")
        return "COMPLETED"

    async def set_rate(self, axis, rate):
        axis = axis.upper()
        self.logger.debug(
            "set_rate: changing rate from {} to {} on axis {}".format(
                self.offsets[axis]["rate"], rate, axis
            )
        )
        if axis not in list(self.offsets.keys()):
            self.logger.debug(
                "Can't change offset for axis type {}".format(axis))
            return
        self.offsets[axis]["rate"] = float(rate)
        while True:
            current_accumulated_offset = self.get_accum_offset(self.axis)
            self.logger.debug(
                ("set_rate: "
                 "accumulated offset: {}, "
                 "rate: {}").format(current_accumulated_offset, rate))
            current_accumulated_offset += rate
            self.set_accum_offset(self.axis, current_accumulated_offset)
            await asyncio.sleep(self.update_rate["offset_rate"])

    async def point_antenna(self, ra, dec, slew_rate=0.23, coord_type="RADEC"):
        """
        Args:
            ra (float): current source RA, in radians
            dec (float): current source DEC in radians
        """
        convert = 180. / math.pi

        def get_direction(diff):
            """
            If diff is less than 0, return -1, otherwise return 1
            Args:
                diff (float/int)
            """
            direction = 1
            if diff < 0:
                direction = -1
            return direction

        def calc_commanded_azel(observer, body):
            """
            The values calculated by this function
            are actually slighty off what they should be.
            """
            observer.date = ephem.now()
            body.compute(observer)
            return body.az*convert, body.alt*convert

        # def exit_cond(current_point):
        #     return all(
        #         [current_point[axis]["actual"] == current_point[axis]["predicted"]
        #          for axis in ["AZ", "EL"]
        #     )

        self.logger.debug("point_antenna: ra: {}, dec: {}".format(ra, dec))
        observer = self.copy()

        if coord_type not in ["AZEL", "RADEC"]:
            raise RuntimeError("Need to specify a coordinate type that is either RADEC or AZEL")
        if coord_type == "AZEL":
            raise NotImplementedError("AZEL not implemented at the moment")
        elif coord_type == "RADEC":
            body = SerializableBody()
            body._ra = float(ra)
            body._dec = float(dec)
            body._epoch = ephem.now()
            observer.epoch = ephem.now()
            self.logger.debug(
                "point_antenna: body._ra: {}, body._dec: {}".format(
                    convert*body._ra, convert*body._dec))

        # try:
        slew_amount = slew_rate*self.update_rate["point_antenna"]
        commanded_az, commanded_el = calc_commanded_azel(observer, body)
        current_point = self.point
        current_point["AZ"]["predicted"] = commanded_az
        current_point["EL"]["predicted"] = commanded_el
            # module_logger.debug("point_antenna: starting to move")
        # except Exception as err:
        #     module_logger.error("point_antenna: {}".format(err))

        while True:
            self.logger.debug("point_antenna: point: {}".format(self.point))
            commanded_az, commanded_el = calc_commanded_azel(observer, body)
            current_point = self.point
            current_point["AZ"]["predicted"] = commanded_az
            current_point["EL"]["predicted"] = commanded_el

            for axis in ["AZ", "EL"]:
                diff = current_point[axis]["predicted"] - current_point[axis]["actual"]
                if abs(diff) > slew_rate:
                    current_point[axis]["actual"] += get_direction(diff)*slew_rate
                else:
                    current_point[axis]["actual"] = current_point[axis]["predicted"]

            self.point = current_point
            await asyncio.sleep(self.update_rate["point_antenna"])

    async def update_weather(self):

        dss_location = DSSLocation(self.number)
        lon = dss_location.lon.value
        lat = dss_location.lat.value

        while True:
            try:
                weather = get_current_weather(
                    float(self.lat), float(self.lon)
                ).json()
                self.logger.debug("update_weather: {}".format(weather))
                self.temperature = weather["main"]["temp"]
                self.pressure = weather["main"]["pressure"]
                self.humidity = weather["main"]["humidity"]
                self.windspeed = weather["wind"]["speed"]
                self.winddirection = weather["wind"]["deg"]

            except Exception as err:
                self.logger.error("WeatherThread: Couldn't get weather: {}".format(err))

            await asyncio.sleep(self.update_rate["update_weather"])


class NMCServer(object):

    break_char = "\n"
    recv_val = 8

    def __init__(self, antenna):
        self.logger = module_logger.getChild("NMCServer")
        self.antenna = antenna
        self.logger.debug("__init__: for %s", self.antenna)

    async def handle_client(self, reader, writer):

        while True:
            self.logger.debug("handle_client: waiting for data")
            data = (await reader.read(self.recv_val)).decode("utf8")
            if not data:
                break
            accum = data
            while data.find(self.break_char) == -1:
                data = (await reader.read(self.recv_val)).decode("utf8")
                accum += data
            self.logger.debug("handle: received {} from client".format(accum))
            try:
                resp = await self.antenna.dispatch_command(accum)
                writer.write(resp.encode("utf8"))
                self.logger.debug(
                    "handle: sent {} to client".format(resp))
            except Exception as err:
                self.logger.error(
                    "handle: error processing request: {}".format(err))


def create_arg_parser():
    import argparse
    parser = argparse.ArgumentParser(description="Fire up NMC simulator")
    parser.add_argument("--verbose", "-v", dest="verbose", required=False,
                        action="store_true",
                        help="In verbose mode, the log level is DEBUG")
    parser.add_argument("--log_dir", "-ld", dest="log_dir", required=False,
                        action="store",
                        default=tams_config.log_dir,
                        help="Specify the log directory")
    return parser


if __name__ == "__main__":
    from support.logs import setup_logging
    
    parsed = create_arg_parser().parse_args()
    log_dir = parsed.log_dir
    
    level = logging.INFO
    if parsed.verbose:
        level = logging.DEBUG
        
    timestamp = datetime.datetime.utcnow().strftime("%Y-%j-%Hh%Mm%Ss")
    setup_logging(
        logger=logging.getLogger(""),
        logLevel=level,
        logfile=os.path.join(log_dir, "NMC_sim43_{}.log".format(timestamp))
    )

    antenna = SimulatedAntenna(43)
    # antenna.start_weather_updates()
    antenna_server = NMCServer(antenna)
    host = "localhost"
    port = 6743
    loop = asyncio.get_event_loop()
    loop.create_task(
        asyncio.start_server(
            antenna_server.handle_client, host, port
        )
    )
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.close()
