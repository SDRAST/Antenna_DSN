"""
nmc_server.py - A Pyro5 server for interacting with NMC control scripts which
send commands to the Antenna, and access Antenna monitor items. This is
designed to work with NMC control script nmc_socket_control.tcl, located
in the `tcl` area of the MonitorControl/Antenna/DSN repository.

This is the way to connect to the server with a Pyro5 client:

.. code-block:: python

    In [1]: from MonitorControl.Antenna.DSN import DSN_Antenna
    In [2]: hardware = {
    ...:         "Antenna":True,
    ...:         "FrontEnd":False,
    ...:         "Receiver":False,
    ...:         "Backend":False
    ...:       }

    In [5]: from MonitorControl import Observatory
    In [6]: observatory = Observatory("Canberra")
    In [7]: antenna = DSN_Antenna(observatory, dss=43, hardware=hardware["Antenna"])
    In [8]: print antenna.help()

    Server methods
    --------------
    connect_to_hardware(wsn, sock_port=None)
      Connect to APC via NMC wsn when not in simulator mode.
    simulate()
      Turn on simulation mode.
    k_band_fwhm(freq=22000)
      70-m beamwidth in millideg
    slew_to_source(source_name)
      Slew to source position
    single_scan(tperscan, eloffset, xeloffset, *args)
      Perform a single scan, or moving from one feed to another.
    tipping_function(status, pm_callback_dict)
      Perform tipping. Steps:
       1) Go to stow position 15 degrees,
       2) Scan to 88 degrees in the source direction,
       3) Record PM data and
      plot vs elevation
    close()

    Antenna methods
    ---------------
    command(command_str, recv_val=128)
      Send an arbitrary command str. Use this method with extreme caution.
      This should be properly formatted to reflect what the antenna server
      wants to see.
    move(position, axis='EL')
      Send MOVE command to antenna
    get_hadec(s)
      Get Hadec data from the antenna.
    get_azel()
      Get azimuthal and elevation information from the antenna.
    el_offset
      Current elevation offset
    xel_offset
      Current cross-elevation offset
    get_offsets()
      Get offset data from the telescope
    set_offset(axis1='EL', axis2='XEL', value1=0, value2=0)
      Set the antenna offset.
    set_offset_one_axis(axis, value)
      This does the same thing as the set_offset method, except that it sets
      offset one axis at a time.
    onsource()
      Determine whether the antenna is onsource.
    clr_rate(reset_param)
      request rate parameter (az, el, dec, xel or xdec) to be reset to zero.
    stop()
      Stop the telescope from moving
    stow()
      Stow the telescope
    clr_offsets()
      Reset offsets to zero.
    ap_trk()
      Put antenna in track mode
    set_rate(axis, rate)
      Set a slew rate for the antenna along one axis.
    feed_change(flag, eloffset, xeloffset)
      Change the feed for two beam nodding.
    point_onsource(source_name, source_ra, source_dec):
      point onsource
    point_source(coord_type, long_coord, lat_coord):
      Point to arbitrary coordinates.
"""
import logging
import socket
import sys
import time
import errno

import Pyro5
import ephem
import h5py
import numpy as np

from MonitorControl.Antenna.DSN.simulator import FakeAntenna
from support.pyro.socket_error import register_socket_error
from support.pyro.pyro5_server import Pyro5Server
from support.test import auto_test

help = """
Server methods
--------------
connect_to_hardware(wsn, sock_port=None)
    Connect to APC via NMC wsn when not in simulator mode.
simulate()
    Turn on simulation mode.
k_band_fwhm(freq=22000)
    70-m beamwidth in millideg
slew_to_source(source_name)
    Slew to source position
single_scan(tperscan, eloffset, xeloffset, *args)
    Perform a single scan, or moving from one feed to another.
tipping_function(status, pm_callback_dict)
    Perform tipping. Steps: 1) Go to stow position 15 degrees, 2) Scan to
    88 degrees in the source direction, 3) Record PM data and
    plot vs elevation
close()

Antenna methods
---------------
command(command_str, recv_val=128)
    Send an arbitrary command str. Use this method with extreme caution.
      This should be properly formatted to reflect what the antenna server
    wants to see.
move(position, axis='EL')
     Send MOVE command to antenna
get_hadec(s)
     Get Hadec data from the antenna.
get_azel()
     Get azimuthal and elevation information from the antenna.
el_offset
    Current elevation offset
xel_offset
    Current cross-elevation offset
get_offsets()
    Get offset data from the telescope
set_offset(axis1='EL', axis2='XEL', value1=0, value2=0)
    Set the antenna offset.
set_offset_one_axis(axis, value)
    This does the same thing as the set_offset method, except that it sets
    offset one axis at a time.
onsource()
    Determine whether the antenna is onsource.
clr_rate(reset_param)
    request rate parameter (az, el, dec, xel or xdec) to be reset to zero.
stop()
    Stop the telescope from moving
stow()
    Stow the telescope
clr_offsets()
    Reset offsets to zero.
ap_trk()
    Put antenna in track mode
set_rate(axis, rate)
    Set a slew rate for the antenna along one axis.
feed_change(flag, eloffset, xeloffset)
    Change the feed for two beam nodding.
point_onsource(source_name, source_ra, source_dec):
    point onsource
point_source(coord_type, long_coord, lat_coord):
    Point to arbitrary coordinates.
"""
register_socket_error()


@Pyro5.api.expose
class NMCServer(Pyro5Server):
    """
    APC antenna socket connection.
    """
    ws_nm = {"CDSCC": {0: 'localhost',
                       1: '137.228.202.190',
                       2: '137.228.202.191',
                       3: '137.228.202.192',
                       4: '137.228.202.193',
                       5: '137.228.202.194',
                       6: '137.228.202.195',
                       7: '137.228.202.209',
                       8: '137.228.202.203',
                       9: '137.228.202.210',
                       91: '137.228.202.68',
                       92: '137.228.202.81'},
             "GDSCC": {0: 'localhost',
                       1: '137.228.201.190',
                       2: '137.228.201.191',
                       3: '137.228.201.192',
                       4: '137.228.201.193',
                       5: '137.228.201.194',
                       6: '137.228.201.195',
                       7: '137.228.201.209',
                       11: '137.228.201.86',
                       12: '137.228.201.87',
                       93: '137.228.201.68',
                       94: '137.228.201.81'}}

    def __init__(self, wsn=0, site="GDSCC", dss="14", simulated=False,
                 logfile=None, logLevel=logging.INFO, logger=None):
        """
        Create APC server, including any socket connections

        Socket connections are provided not in simulation mode.

        @param wsn : the work station number ('0')
        @type  wsn : str

        @param sock_port : The socket port for the APC connection (6743)
        @type  sock_port : int

        @param **kwargs: To be passed to Pyro5Server
        """
        if logger is None:
            logger = logging.getLogger(__name__+".NMCServer")
        self.logger = logger
        self.sock = None
        self._sock_port = 6700+int(dss)
        self.logger.debug("__init__: socket_port = %s", self._sock_port)
        self._simulated = simulated
        self._wsn = wsn
        self._site = site
        self._dss = dss
        self.server_initialized = False
        if not self._simulated:
            success = self.connect_to_hardware(wsn, site)
        elif self._simulated:
            success = self.simulate()
        self.logger.debug("__init__: success = %s", success)
        self.logger.debug(
            (" simulation mode: {}, "
             "socket host: {}, "
             "socket port: {}, "
             "workstation number: {}").format(
                self._simulated,
                self.ws_nm[site][self._wsn],
                self._sock_port,
                wsn)
        )
        self.monitem_mapping = {
            "AzimuthAngle": "az",
            "ElevationAngle": "el"
        }

        self.status = {
            "antenna_status": None,
            "point_status": None,
            "angle": {
                "az": None,
                "el": None,
                "az_tol": 0.085,
                "el_tol": 0.085,
                "az_pred_tol": 0.11,
                "el_pred_tol": 0.11
            },
            "offset": {
                "az": None,
                "xel": None
            },
            "offset_rate": {
                "az": None,
                "xel": None
            }
        }
        super(NMCServer, self).__init__(obj=self,
                                        logger=logger,
                                        logfile=logfile,
                                        name="APC_DSS_{}".format(dss))

    @auto_test(args=("point_status",))
    def get_status(self, *args):
        """
        Access any parameter in status attribute dictionary in a thread safe
        manner.
        """
        self.logger.debug("get_status: args: {}".format(args))
        with self.lock:
            if len(args) == 0:
                return self.status
            else:
                sub = self.status[args[0]]
                for arg in args[1:]:
                    sub = self.status[arg]
                return sub

    @auto_test(args=("point_status","SLEWING"))
    def _update_status(self, *args):
        """
        Update status attribute dictionary in a thread safe manner.
        """
        if len(args) == 0:
            self.logger.debug("_update_status: Need to provide some parameter to update")
        else:
            update_val = args[-1]
            sub = self.status[args[0]]
            for arg in args[1:-2]:
                sub = self.status[arg]
            sub[args[-2]] = update_val

    def help(self):
        return help

    @property
    def simulated(self):
        return self._simulated

    @property
    def wsn(self):
        return self._wsn

    @property
    def site(self):
        return self._site

    @property
    def dss(self):
        return self._dss

    def reconnect_to_hardware(self):
        """Reconnect to socket using _wsn and _site info"""
        if self.sock is not None:
            self.close_socket()
        return self.connect_to_hardware(self._wsn, self._site)

    def connect_to_hardware(self, wsn, site,
                            sock_port=None, socket_timeout=10):
        """
        Connect to APC when not in simulator mode.

        @param wsn : The workstation number to connect to
        @type  wsn : int

        @param sock_port : The socket port to use.
        @type  sock_port : int

        @return: None
        """
        self.logger.debug("connect_to_hardware: on workstation {}".format(wsn))
        if not sock_port:
            sock_port = self._sock_port
        self._site = site
        try:
            self._wsn = int(wsn)
            sock_host = self.ws_nm[site][self._wsn]
        except KeyError:
            self.logger.error("connect_to_hardware: couldn't identify workstation")
            sock_host = 'localhost'
            self._wsn = 0
        except Exception as err:
            self.logger.error("connect_to_hardware:"+
                        "couldn't identify workstation {}: {}".format(wsn, err))
            sock_host = 'localhost'
            self._wsn = 0
        self.logger.debug("connect_to_hardware: host=%s, socket=%s",
                             sock_host, sock_port)
        address = (sock_host, sock_port)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        init_timeout = self.sock.gettimeout()
        self.logger.debug(
            "connect_to_hardware: initial socket timeout: {}".format(init_timeout))
        self.sock.settimeout(socket_timeout)
        try:
            self.sock.connect(address)
            self.logger.debug("connect_to_hardware: assigned socket %s",
                              self.sock.getsockname())
            self.sock.settimeout(init_timeout)
            self._simulated = False
            return {'success': True, 'wsn': self._wsn}
        except socket.timeout:
            self.sock.settimeout(init_timeout)
            self.logger.error(
                     "connect_to_hardware: Couldn't connect to workstation {}."+
                     "Connection timed out".format(wsn))
            self.simulate()
            return {'success': False, 'wsn': 0}
        except socket.error as err:
            num = err.errno
            self.logger.error("connect_to_hardware:"+
                   "Couldn't connect to workstation {}, error number {}".format(
                                                     wsn, errno.errorcode[num]))
            self.simulate()
            return {'success': False, 'wsn': 0, 'errno':errno.errorcode[num]}

    def simulate(self):
        """
        Turn on simulation mode.

        @return: None
        """
        self.logger.debug("simulate: connecting to simulator")
        self.simulator = FakeAntenna()
        self._wsn = self.simulator.wsn
        self._simulated = True
        self._site = self.simulator.site
        self._dss = self.simulator.dss
        try:
            self.sock.close()
            self.logger.debug("simulate: socket closed")
            self.sock = None
        except AttributeError:
            self.sock = None
        return {'success': True, 'wsn': 0}

    def k_band_fwhm(self, freq=22000):
        """
        70-m beamwidth in millideg
        """
        return 13.5*22000/freq

    @auto_test(args=("GET_OFFSETS\n", ), returns=str)
    def command(self, command_str, recv_val=128):
        """
        Send an arbitrary command str. Use this method with extreme caution.

        This should be properly formatted to reflect what the antenna server
        wants to see.

        @param command_str : The string to send to the server.
        @type  command_str : str

        @param recv_val : The amount of bytes to wait for from APC server.
        @type  recv_val : int

        @return: Raw response from the APC server.
        """
        if not self._simulated:
            # self.logger.debug("command: sent '%s'", command_str)
            self.sock.sendall(command_str)
            # self.logger.debug("command: waiting for response")
            resp = self.sock.recv(recv_val)
            # self.logger.debug("command: received '%s'", resp)
            return resp
        else:
            # self.logger.debug("command: simulating '%s'", command_str)
            self.simulator.send(command_str)
            resp = self.simulator.recv(recv_val)
            return 'COMPLETED'


    @auto_test(returns=list)
    def get_commands(self):
        commands = self.command("GET_COMMANDS\n", recv_val=1024)
        return commands.split(", ")

    @auto_test(returns=list)
    def get_params(self):
        self.server_initialized = True
        params = self.command("GET_PARAMS\n", recv_val=1024)
        return params.split(", ")

    @auto_test(args=("AzimuthAngle",), returns=dict)
    def get(self, *params, **kwargs):
        """
        Get any parameter being monitored (`monitem`) in NMC control script.
        """
        if not self.server_initialized:
            self.get_params()
        recv_val = kwargs.get("recv_val", 1024)
        return_vals = {params[i]:None for i in range(len(params))}
        if not self._simulated:
            self.logger.debug("get: params: {}".format(params))
            if self.sock is not None:
                cmd = "PARAM {}\n".format(" ".join(params))
                self.logger.debug("get: sending cmd: {}".format(cmd))
                resp = self.command(cmd, recv_val=recv_val)
                self.logger.debug("get: resp: {}".format(resp))
                resp_list = resp.split(",")
                if len(resp_list) == len(params):
                    return_vals = {params[i]:resp_list[i].strip() for i in range(len(params))}
                else:
                    self.logger.error(
                    ("Discrepency between number of parameters requested and number returned: "
                                       ""))
        else:
          return {"AzimuthAngle": 60, "ElevationAngle": 45}
        return return_vals

    @auto_test(returns=dict)
    def get_weather(self):

        return self.get("temperature",
                        "pressure",
                        "humidity",
                        "winddirection",
                        "precipitation",
                        "WxHr", "WxMin", "WxSec")

    @auto_test()
    def get_hadec(self):
        """
        Get Hadec data from the antenna.

        @return: dict with ha, dec, wrap and time values.
        """
        if not self._simulated:
            cmd = "GET_HADEC\n"
            self.logger.debug("get_hadec: sending command {}".format(cmd))
            hadec = self.command(cmd, recv_val=1024)
            self.logger.debug("get_hadec: response: {}".format(hadec))
            try:
                response_split = hadec.strip().split()
                success, wrap, timing = response_split[0], response_split[-2], response_split[-1]
                success = success.upper()
            except ValueError:
                # this means we're using the APC simulator
                success = hadec.strip()
                success = success.upper()
                wrap, timing = 0, 0

            return {'success': success,
                    'wrap': wrap,
                    'timing': timing}

        elif self._simulated:
            success = "COMPLETED"
            self.ha_dec = np.random.rand(4)
            return {'success': success,
                    'ha':self.ha_dec[0],
                    'dec':self.ha_dec[1],
                    'wrap': self.ha_dec[2],
                    'timing': self.ha_dec[3]}

    @auto_test()
    def get_azel(self):
        """
        Get azimuthal and elevation information from the antenna.

        The response is a dict::
               keys: az, az_rad, el, wrap, timing.
               values: azimuthal, azimuthal (in radians), elevation, wrap, timing

        @return: dict
        """
        params = ["AzimuthAngle","ElevationAngle"]
        self.logger.debug("get_azel: getting params {}".format(params))
        resp = self.get(*params)
        return {key:float(resp[key]) for key in resp}

    @auto_test()
    def get_offsets(self):
        """
        Get offset data from the telescope

        To calculate cross elevation (xel), we need antenna elevation (el), and
        antenna azimuth offset (az).

        xel = az*cos(el) -- from Shinji
        """
        params = ["ElevationPositionOffset", "CrossElevationPositionOffset"]
        self.logger.debug("get_offsets: getting params {}".format(params))
        resp = self.get(*params)
        resp = {1000*float(resp[key]) for key in resp}
        return resp

    @auto_test(args=("EL", "XEL", 0.0, 0.0),
                        returns=['COMPLETED', 'COMPLETED'])
    def set_offset(self, axis1='EL', axis2='XEL', value1=0, value2=0, wait_time=None):
        """
        Set the antenna offset.

        @param axis1 : The first axis to set. ("EL")
        @type  axis1 : str

        @param axis2 : The second axis to set. ("XEL")
        @type  axis2 : str

        @param value1 : value to set for the first axis, in millideg (0)
        @type  value2 : float

        @param value2 : value to set for the second axis, in millideg (0)
        @type  value2 : float
        """
        return_val = {"success":{}}
        for axis in [axis1, axis2]:
            return_val["success"][axis] = "COMPLETED"
        if not self._simulated:
            for axis, value in zip([axis1, axis2], [value1, value2]):
                self.logger.debug("set_offset: axis: {}, value: {}".format(axis, value))
                cmd = "ANTENNA PO {} {:.4f}\n".format(axis, value)
                self.logger.debug("set_offset: sending command {}".format(cmd))
                resp = self.command(cmd, recv_val=128)
                self.logger.debug("set_offset: response: {}".format(resp))
                return_val["success"][axis] = resp
        if wait_time is not None:
            time.sleep(float(wait_time))
        return return_val

    @auto_test(args=('EL', 0.0), returns='COMPLETED')
    def set_offset_one_axis(self, axis, value, wait_time=None):
        """
        This does the same thing as the set_offset method, except that it sets
        offset one axis at a time.

        @param axis : The axis we're working with
        @type  axis : str

        @param value : The value to set for the axis, in millidegrees
        @type  value : float
        """
        return_vals = {"success":"COMPLETED"}
        self.logger.debug("set_offset_one_axis: entered with %s %s, %s %s",
                          type(axis), axis, type(value), value)
        if not self._simulated:
            cmd = "ANTENNA PO {} {:.4f}\n".format(axis, value)
            self.logger.debug("set_offset_one_axis: sending command {}".format(cmd))
            resp = self.command(cmd)
            self.logger.debug("set_offset_one_axis: response: {}".format(resp))
            return_vals["success"] = resp
        return return_vals

    @auto_test(returns=str)
    def onsource(self):
        """
        Determine whether the antenna is onsource.
        """
        operational_status = ["operational", "marginal"]
        non_operational_status = ["critical"]

        if self._simulated:
            return "ONSOURCE"

        def determine_antenna_status(status_mon_item_resp):
            status = status_mon_item_resp.lower()
            for s in operational_status:
                if s in status:
                    return True
            for s in non_operational_status:
                if s in status:
                    return False
            return True

        def xel(el, az):
            return az*np.cos(np.deg2rad(el))

        # current_point_status = self.get_status("point_status")
        current_point_status = self.status["point_status"]
        param_names = ["AzimuthTrackingError", "AzimuthAngle", "AzimuthPredictedAngle",
                  "ElevationTrackingError","ElevationAngle", "ElevationPredictedAngle", "Status"]
        params_before = self.get(*param_names)
        self.logger.debug("onsource: onsource params before: {}".format(params_before))
        # self._update_status("antenna_status",params_before["Status"])
        antenna_status_bool = determine_antenna_status(params_before["Status"])
        time.sleep(2.0)
        params_after = self.get(*param_names)
        self.logger.debug("onsource: onsource params after: {}".format(params_after))

        el_prev = float(params_before["ElevationAngle"])
        el_cur = float(params_after["ElevationAngle"])
        # el_err_prev = float(params_before["ElevationTrackingError"])
        el_err_cur = float(params_after["ElevationTrackingError"])
        el_pred_cur = float(params_after["ElevationPredictedAngle"])

        az_prev = float(params_before["AzimuthAngle"])
        az_cur = float(params_after["AzimuthAngle"])
        # az_err_prev = float(params_before["AzimuthTrackingError"])
        az_err_cur = float(params_after["AzimuthTrackingError"])
        az_pred_cur = float(params_after["AzimuthPredictedAngle"])

        xel_prev = xel(el_prev, az_prev)
        xel_cur = xel(el_cur, az_cur)
        # xel_err_prev = xel(el_err_prev, az_err_prev)
        xel_err_cur = xel(el_err_cur, az_err_cur)
        xel_pred_cur = xel(el_cur, az_pred_cur)

        # el_tol = self.get_status("angle","el_tol")
        # xel_tol = self.get_status("angle","az_tol")
        el_tol = self.status["angle"]["el_tol"]
        xel_tol = self.status["angle"]["az_tol"]
        el_pred_tol = self.status["angle"]["el_pred_tol"]
        xel_pred_tol = self.status["angle"]["az_pred_tol"]

        self.logger.debug(("onsource: current el: {}, previous_el: {} "
                           "current az: {}, previous az: {} "
                           "current xel: {}, previous xel: {} "
                           "current el error: {}, current az error: {}, current xel error {} "
                           "predicted el: {}, predicted az: {}, predicted xel: {}").format(
                                el_cur, el_prev,
                                az_cur, az_prev,
                                xel_cur, xel_prev,
                                el_err_cur, az_err_cur, xel_err_cur,
                                el_pred_cur, az_pred_cur, xel_pred_cur
                           ))
        self.logger.debug("onsource: antenna_status_bool: {}".format(
                                                           antenna_status_bool))
        self.logger.debug("onsource: abs(xel_err_cur) < xel_tol: {}".format(
                                                    abs(xel_err_cur) < xel_tol))
        self.logger.debug("onsource: abs(el_err_cur) < el_tol: {}".format(
                                                      abs(el_err_cur) < el_tol))
        self.logger.debug(
                       "onsource: abs(xel_cur - xel_prev) < xel_tol: {}".format(
                                             abs(xel_cur - xel_prev) < xel_tol))
        self.logger.debug("onsource: abs(el_cur - el_prev) < el_tol: {}".format(
                                                abs(el_cur - el_prev) < el_tol))
        self.logger.debug(
              "onsource: abs(xel_pred_cur - xel_cur) < xel_pred_tol: {}".format(
                                    abs(xel_pred_cur - xel_cur) < xel_pred_tol))
        self.logger.debug(
                 "onsource: abs(el_pred_cur - el_cur) < el_pred_tol: {}".format(
                                       abs(el_pred_cur - el_cur) < el_pred_tol))

        if (abs(xel_err_cur) < xel_tol and
            abs(el_err_cur) < el_tol and
            abs(xel_cur - xel_prev) < xel_tol and
            abs(el_cur - el_prev) < el_tol and
            abs(xel_pred_cur - xel_cur) < xel_pred_tol and
            abs(el_pred_cur - el_cur) < el_pred_tol):
            point_status = "ONSOURCE"
        elif antenna_status_bool:
            point_status = "SLEWING"
        else:
            point_status = "ERROR"
        self.status["point_status"] = point_status
        # self._update_status("point_status", point_status)
        return point_status

    def clr_rates(self):
        """
        Set rates for all axes to 0
        """
        resp = self.command("ANTENNA CLR RO\n")
        return resp

    def clr_rate(self, reset_param):
        """
        request rate parameter to be reset to zero.

        reset_param can either be az, el, dec, xel or xdec
        """
        raise NotImplementedError
        if not self._simulated:
            if reset_param not in ["AZ", "EL", "DEC", "XEL", "XDEC"]:
                self.logger.error("Requested parameter for reset not recognised")
            else:
                self.sock.sendall("ANTENNA CLR RO {}\n".format(reset_param))
                resp = self.sock.recv(128)
                self.logger.info("clr_rate: RO parameters reset")
        elif self._simulated:
            self.logger.info("clr_rate: RO parameters reset")

    def clr_offset(self, axis):
        raise NotImplementedError

    def clr_offsets(self):
        """
        Reset offsets to zero.
        """
        self.logger.debug("clr_offsets: Offset being cleared.")
        resp = self.command("ANTENNA CLR PO\n")
        return resp

    def set_semn(self, model_name):
        """
        Set current pointing model
        """
        raise NotImplementedError
        resp = self.command("ANTENNA SEMN {}\n".format(model_name))
        return resp

    #@auto_test(args=(45.,),kwargs={"axis":"EL"},returns=str)
    def move(self, position, axis='EL'):
        """
        Send MOVE command to antenna

        @param position : coordinate value in degrees
        @type  position : float

        @param axis : direction in which to move
        @type  axis : str

        @return: dict
        """
        return_vals = {"success":"COMPLETED"}
        if not self._simulated:
            cmd = "ANTENNA MOVE {} {}\n".format(axis, position)
            self.logger.debug("move: sending command {}".format(cmd))
            resp = self.command(cmd)
            self.logger.debug("move: Response from server: {}".format(resp))
            resp = resp.strip()
            return_vals["success"] = resp
        return return_vals

    def stop(self):
        """
        Stop the telescope from moving.
        """
        raise NotImplementedError
        if not self._simulated:
            self.sock.sendall("ANTENNA STOP \n")
            response = self.sock.recv(128)
            time.sleep(2)
            if response == 'COMPLETED':
                self.logger.info("stop: Telescope stop request succesful\n")
            elif response == 'REJECTED':
                self.logger.info("stop: Telescope stop request utterly failed\n")
            else:
                self.logger.info("stop: Unexpected response from server:{}\n".format(response))

            return response
        elif self._simulated:
            self.logger.info("stop: Telescope stop request succesful\n")
            return "COMPLETED"

    def stow(self):
        """
        Stow the telescope.
        """
        raise NotImplementedError
        return_val = {"success":"COMPLETED"}
        if not self._simulated:
            cmd = "ANTENNA STOW\n"
            self.logger.debug("stow: sending command {}".format(cmd))
            resp = self.command(cmd)
            self.logger.debug("stow: response: {}".format(res))
            return_val["success"] = resp
        return return_val

    def trk(self):
        """
        Put antenna in track mode, which I think means that once a source is selected,
        it will automatically follow it's position across the sky as the Earth rotates.
        """
        return_val = {"success":"COMPLETED"}
        if not self._simulated:
            cmd = "ANTENNA TRK\n"
            self.logger.debug("trk: sending command {}".format(cmd))
            resp = self.command(cmd)
            self.logger.info("trk: response: {}".format(resp))
            return_val["success"] = resp
        return return_val

    @auto_test(args=('EL', 0.0), returns="COMPLETED")
    def set_rate(self, axis, rate):
        """
        Set a slew rate for the antenna along one axis.
        
        @param axis: The axis for which to set rate
        @type axis: str

        @param rate: The actual rate in millideg per second
        @type rate: float
        """
        axis_rate_commands = {
            'AZ':'AZRTE',
            'EL':'ELRTE',
            'XEL':'AXRTE',
            'HA':'HARTE',
            'DEC':'DERTE',
            'XDEC':'HXRTE'
        } # this may be deprecated

        return_val = {"success":"COMPLETED"}

        self.logger.debug("set_rate: axis: {}, rate: {}".format(axis, rate))
        if not self._simulated:
            accepted_axis_types = ["AZ","EL","XEL","HA","DEC","XDEC"]
            if axis.upper() not in accepted_axis_types:
                msg = "Won't accept axis type: {}".format(axis)
                self.logger.error(msg)
                raise RuntimeError(msg)

            cmd = "ANTENNA RO {} {:.4f}\n".format(axis.upper(), rate)
            self.logger.debug("set_rate: sending command {}".format(cmd))
            resp = self.command(cmd, recv_val=128).strip()
            self.logger.debug("set_rate: received {} from server".format(resp))
            return_val["success"] = resp

        return return_val

    @auto_test(args=[False, 0.0, 0.0])
    def feed_change(self, flag, eloffset, xeloffset):
        """
        Change the feed for two beam nodding.
        
        @param flag : This flag indicates whether to be using feed 1 or feed 2
        @type  flag : bool
        
        @param eloffset: elevation offset calculated using the boresight routine
        @param xeloffset: cross elevation offset calculated using the boresight routine
        """
        if not flag:
            feed = 'Feed 1'
            status = self.set_offset('EL', 'XEL', eloffset, xeloffset)
            self.logger.info(
                ("Moving to feed position {} "
                 "by setting offsets to El: {}, xEl: {}"
                 "Response from set_offset: {}").format(
                    feed, eloffset, xeloffset, status))
            return 1
        else:
            feed = 'Feed 2'
            eloffset, xeloffset = 14.0 + eloffset, 31.0 + xeloffset
            status = self.set_offset('EL', 'XEL', eloffset, xeloffset)
            self.logger.info(
                ("Moving to feed position {} by setting OFF"
                 "source offsets to El: {},  xEl: {}. "
                 "Response from set_offset: {}").format(
                    feed, eloffset, xeloffset, status))
            return 0

    def point_radec(self, ra, dec, epoch="J2000"):
        """
        Point to a source, given coordinates in RA and DEC.
        the epoch defaults to "J2000", but we can also send coordinates
        that we've precessed ourselves.
        Args:
            ra (float): Right Ascension as floating point number
            dec (float): Declination as floating point number
        Keyword Args:
            epoch (str): Are we supplying J2000 coordinates or
                coordinates that are already precessed? ("J2000")
        Returns:
        """
        self.logger.debug("point_radec: ra: {}, dec: {}, epoch: {}".format(ra, dec, epoch))

        if epoch.lower() not in ["j2000", "now"]:
            msg = "provided epoch {} is not either J2000 or now".format(epoch)
            self.logger.error(msg)
            raise RuntimeError(msg)

        if epoch.lower() == "now":
            if isinstance(ra, str):
                ra = ephem.hours(ra)
                ra = float(ra)*180./np.pi
            if isinstance(dec, str):
                dec = ephem.degrees(dec)
                dec = float(dec)*180/np.pi
            ra_precessed = float(ra)
            dec_precessed = float(dec)
        else:
            body = ephem.FixedBody()
            body._ra = ra
            body._dec = dec
            body.compute()
            ra_precessed = float(body.ra)*180./np.pi
            dec_precessed = float(body.dec)*180./np.pi

        self.logger.debug("point_radec: sending ra: {}, dec: {}".format(ra_precessed, dec_precessed))
        cmd = "ANTENNA RADEC {:.6f} {:.6f}\n".format(ra_precessed, dec_precessed)
        self.logger.debug("point_radec: sending command {}".format(cmd))
        resp = self.command(cmd)
        self.logger.debug("point_radec: response: {}".format(resp))

    def point_azel(self, az, el):
        raise NotImplementedError

    def single_scan(self, tperscan, eloffset, xeloffset, *args):
        """
        Perform a single scan, or moving from one feed to another.

        This can be done multiple times to acheive a complete observation.

        @param tperscan : The time to spend before switching
        
        @param eloffset : the el offset. This comes from the boresight.
        
        @param xeloffset : the xel offset
        
        @param *args : args to be passed to point_onsource
        """
        response = self.point_onsource(*args)
        if response:
            self.feed_change(0, eloffset, xeloffset)
            time.sleep(tperscan)
            self.feed_change(1, eloffset, xeloffset)
            time.sleep(tperscan)
            return True
        else:
            return False

    def tipping_function(self, status, pm_callback_dict):
        """
        Perform tipping

            Steps::
                1) Go to stow position 15 degrees
                2) Scan to 88 degrees in the source direction
                3) Record PM data and plot vs elevation
        @param status: The current status (from ant_sts UI element)
        @param pm_callback_dict:
        """
        if status:
            # self.sock.sendall("ANTENNA PO %s %.4f\n"%('XEL', 100))
            # self.logger.info(self.sock.recv(128))
            time.sleep(5)
        else:
            pass

        pm_cb = pm_callback_dict['callback']
        pm_cb_args = pm_callback_dict['callback_args']

        for el, status in zip([15, 88], ['Halfway', 'Completed']):
            self.logger.info("Doing tipping now to {}".format(el))
            # Move antenna to 8 EL

            self.sock.sendall("ANTENNA MOVE EL {}\n".format(el))
            self.logger.info(self.sock.recv(128))
            #        start_time = time.time()
            end_time = time.time() + 320  # should be 320 in real client
            tipping_record_el, tipping_record_tsys = [], []
            while (end_time > time.time()):
                azel_dat = self.get_azel()
                el = azel_dat['el']
                tipping_record_el.append(el)

                pm_dat = pm_cb(*pm_cb_args)
                tipping_record_tsys.append(pm_dat)
                # tipping_record_el.append(self.plot_tipping()[0])
                # tipping_record_tsys.append(self.plot_tipping()[1])
                time.sleep(1)
            else:
                pass

            cur_time = time.strftime("%H:%M:%S")
            filename = './tipping_data' + cur_time + '.hdf5'
            try:
                #            print tipping_record_el, tipping_record_tsys
                f = h5py.File(filename, 'w')
                f.create_dataset('elevation', data=np.array(tipping_record_el))
                f.create_dataset('tsys', data=np.array(tipping_record_tsys))
                f.close()
            except:
                self.logger.info("Could not save tipping data to file, bug 1!!!")
                pass
            yield {'status': status,
                   'tipping_el': np.asarray(tipping_record_el, dtype=float),
                   'tipping_tsys': np.asarray(tipping_record_tsys, dtype=float)}

        self.logger.info("Completed")

    def close_socket(self):
        """
        Close down the socket connection without killing the connection to the Pyro5 Daemon.
        """
        self.logger.debug("close_socket: Closing down socket connection")
        if not self._simulated:
            try:
                self.sock.close()
                time.sleep(0.5)
            except:
                self.logger.error("close_socket: Closing down socket connection Failed")

    def close(self):
        """
        Close socket, and then shut down server.
        """
        self.close_socket()
        super(NMCServer, self).close()

    @Pyro5.api.expose
    @Pyro5.api.oneway
    def LRM(self, duration, callback):
        """
        test of callback with long-running method
        """
        self.logger.debug("LRM: called for %d", duration)
        callback._pyroClaimOwnership()
        time.sleep(duration)
        self.logger.debug("LRM: sleep finished")
        try:
            callback.finished(self.LRM.__name__, duration)
        except Exception:
            self.logger.error("LRM: callback.finished failed")
            self.logger.error("".join(Pyro5.errors.get_pyro_traceback()))
        self.logger.debug("LRM: finished %d" % duration)

        
def create_arg_parser():
    import argparse
    parser = argparse.ArgumentParser(
                                   description="Fire up APC(A) control server.")
    parser.add_argument("--workstation", "-wsn", dest="wsn", default=0,
                        required=False, action="store",type=int,
             help="Select the operator workstation you'll be connecting to (0)")
    parser.add_argument("--site","-st",dest="site",default="CDSCC",
                        required=False, action="store",
                        help="Select the DSN site you'll be accessing (CDSCC)")
    parser.add_argument("--dss","-d",dest="dss",default=43,required=False,
                        action="store",
               help="Select which DSS antenna the server will connect to. (43)")
    parser.add_argument("--simulated","-s",dest="s",default=False,
                        required=False, action="store_true",
                        help="In simulation mode"+
                  " the APC(A) server won't connect to a control server (True)")
    parser.add_argument("--local","-l",dest="local",default=True,
                        required=False, action="store",
                        help="In local mode, the APC(A) server will fire up"+
                       " on localhost, without creating any SSH tunnels (True)")
    parser.add_argument("--verbose","-v",dest="verbose",default=True,
                        required=False, action="store_true",
                        help="In verbose mode, the log level is DEBUG")

    return parser


def main(server_cls):
    """
    starts logging, creates a server_cls object, launches the server object
    
    
    """
    def _main():
        from support.logs import setup_logging
        import datetime
        import os
        
        parsed = create_arg_parser().parse_args()
        
        level = logging.DEBUG
        if not parsed.verbose:
            level = logging.INFO
        # logdir = "/home/ops/roach_data/sao_test_data/logdir/"
        logdir = "/usr/local/Logs/"+socket.gethostname()
        if not os.path.exists(logdir):
            logdir = os.path.dirname(os.path.abspath(__file__))
        timestamp = datetime.datetime.utcnow().strftime("%Y-%j-%Hh%Mm%Ss")
        logfile = os.path.join(
            logdir, "{}_{}.log".format(
                server_cls.__name__, timestamp
            )
        )
        setup_logging(logLevel=level, logfile=logfile)
        server = server_cls(
            wsn=parsed.wsn,
            dss=parsed.dss,
            site=parsed.site,
            logLevel=level,
            simulated=parsed.s
        )
        # print(server.feed_change(0, 0, 0))
        server.launch_server(
            ns=False,
            objectId="APC",
            objectPort=50001,
            local=parsed.local,
            threaded=False
        )

    return _main

if __name__ == '__main__':
    main(NMCServer)()
