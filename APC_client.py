import Pyro4
import threading
import logging

class DSS43K2Client(object):
    """
    Simple DSS43K2Client that registers callbacks. Can only be used locally,
    not over ssh connection.
    """
    def __init__(self, ns_host='localhost', ns_port=9090):
        self.logger = logging.getLogger(__name__)
        ns = Pyro4.locateNS(host=ns_host, port=ns_port)
        dss43_uri = ns.lookup('APC')
        self.proxy = Pyro4.Proxy(dss43_uri)
        self.callback_daemon = Pyro4.Daemon(host=ns_host)
        uri = self.callback_daemon.register(self)
        self.callback_daemon_thread = threading.Thread(target=self.callback_daemon.requestLoop)
        self.callback_daemon_thread.start()

    @Pyro4.expose
    @Pyro4.callback
    def boresight_updates_callback(self, updates):
        if 'status' in updates:
            print((updates['status']))

    @Pyro4.expose
    @Pyro4.callback
    def boresight_callback(self, results):
        if 'prog' in results:
            print((results['prog']))

    def __getitem__(method_name):
        """
        Optional convenience method. You might also extend from the Pyro4.Proxy class.
        """
        return getattr(self.proxy, method_name)
        
logging.basicConfig(level=logging.DEBUG)
mylogger = logging.getLogger()

