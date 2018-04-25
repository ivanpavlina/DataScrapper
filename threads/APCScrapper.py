import pexpect
from threading import Thread
from time import sleep
from etc import config
from lib.logger import get_logger
import sys


class APCScrapper(Thread):

    def __init__(self, output_queue):
        # Setup thread stuff
        Thread.__init__(self)
        self.threadID = 3
        self.daemon = True
        self.LOGGER = get_logger(type(self).__name__)

        # Setup output
        self._out_status_queue = output_queue

        # Setup flags
        self._error_count = 0
        self.running = False
        self.wantRunning = True  # Can be changed from main

        # Init API connection
        self._client = None

    def _connect_client(self):
        try:
            self._client = pexpect.spawn('telnet {}'.format(config.apc['host']), timeout=5)

            try:
                self._client.expect('User Name : ', timeout=None)
                self._client.sendline(config.apc['username']+"\r")
            except:
                pass

            self._client.expect('Password  : ')
            self._client.sendline(config.apc['password']+"\r")
            self._client.expect('> ')

            # Enter ups monitoring
            self._client.sendline('1'+"\r")
            self._client.expect('> ')
            self._client.sendline('1'+"\r")
            self._client.expect('> ')

            self.LOGGER.info("Connected to APC")

        except Exception, e:
            self.LOGGER.error("Error connecting client [{}]".format(e))
            self._disconnect_client()
            self._error_count += 1

    def _disconnect_client(self):
        if self._client:
            try:
                self._client.close(force=True)
                self.LOGGER.info("Disconnected telnet")
            except Exception, e:
                self.LOGGER.error("Error disconnecting telnet [{}]".format(e))
        self._client = None

    """
    Main loop
    Check current UPS status
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
                    self.LOGGER.info("Could not connect to telnet, breaking loop")
                    self.wantRunning = False

                # Main wants out, break out while loop
                if not self.wantRunning:
                    # Call disconnect if client is still connected
                    self.LOGGER.info("Disconnecting telnet client")
                    self._disconnect_client()
                    self.LOGGER.info("Breaking loop")
                    self.running = False
                    break

                # Connect api client
                if not self._client:
                    self.LOGGER.info("Connecting telnet client")
                    self._connect_client()
                    continue

                # ## Check passed

                ups_status = None
                self._client.sendline()
                for line in self._client.before.splitlines():
                    if 'Status of UPS' in line:
                        ups_status = line.split(' : ')[1].strip()

                if not ups_status:
                    self.LOGGER.error("Unable to retrieve UPS status")
                else:
                    self.LOGGER.debug("UPS status received:\n{}".format(ups_status))
                    if 'On Line, No Alarms Present' not in ups_status:
                        self.LOGGER.warning("Invalid status detected!!!")
                    else:
                        self.LOGGER.debug("...")

                sleep(config.apc['status_retrieve_interval'])

            except Exception, e:
                self.LOGGER.error("Unexpected exception in loop\n***{}".format(e))
                self.running = False
                break
        # While loop broken
        self.LOGGER.warning("Thread exiting")
        return
