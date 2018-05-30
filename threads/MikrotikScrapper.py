import routeros_api
from threading import Thread
from time import sleep
from etc import config
from lib.logger import get_logger
from lib.flow import Flow


class MikrotikScrapper(Thread):

    def __init__(self, output_queue):
        # Setup thread stuff
        Thread.__init__(self)
        self.threadID = 2
        self.daemon = True
        self.LOGGER = get_logger(type(self).__name__)

        # Setup output
        self._out_report_queue = output_queue

        # Setup flags
        self._error_count = 0
        self.running = False
        self.wantRunning = True  # Can be changed from main

        self._apiConnection = None

        self._flows = []
        for flow in config.flows:
            try:
                obj = Flow(self.LOGGER, flow)
                self._flows.append(obj)
            except Exception, e:
                self.LOGGER.error("Could not setup flow [{}]\n{}".format(flow, e))

    def _get_api_client(self):
        try:
            if not self._apiConnection or not self._apiConnection.connected:
                # Init API connection
                self._apiConnection = routeros_api.RouterOsApiPool(config.mikrotik['host'],
                                                                   config.mikrotik['username'],
                                                                   config.mikrotik['password'])
                self.LOGGER.info("Connected to API")

            return self._apiConnection.get_api()
        except Exception, e:
            self.LOGGER.error("Error connecting to API [{}]".format(e))
            self._error_count += 1

    def _disconnect_client(self):
        if self._apiConnection.connected:
            try:
                self._apiConnection.disconnect()
                self.LOGGER.info("Disconnected API")
            except Exception, e:
                self.LOGGER.error("Error disconnecting API [{}]".format(e))

    """
    Main loop
    """
    def run(self):
        self.running = True
        self.LOGGER.info("Starting loop")

        while True:
            try:
                # If connecting failed sleep few seconds before retrying
                if self._error_count > 0:
                    sleep(5)

                # If connecting failed too many times shutdown thread
                if self._error_count > 5:
                    self.LOGGER.info("Could not connect to API, breaking loop")
                    self.wantRunning = False

                # Main wants out, break out while loop
                if not self.wantRunning:
                    # Call disconnect if client is still connected
                    if self._apiConnection.connected:
                        self.LOGGER.info("Disconnecting API client")
                        self._disconnect_client()

                    self.LOGGER.info("Breaking loop")
                    self.running = False
                    break

                # Get API client object
                client = self._get_api_client()

                # ## Check passed

                # Run loop methods for all flows
                # If flow returned anything send result to Database thread
                for flow in self._flows:
                    res = flow.loop_pass(client)
                    if res:
                        self._out_report_queue.put(res)

                # Work done, sleep a bit
                sleep(.2)

            except Exception, e:
                self.LOGGER.error("Unexpected exception in loop\n***{}".format(e))
                self.running = False
                break
        # While loop broken
        self.LOGGER.warning("Thread exiting")
        return
