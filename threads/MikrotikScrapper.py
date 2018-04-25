import routeros_api
from threading import Thread
from time import sleep
from etc import config
from lib.logger import get_logger
import time
import re


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
    Check for new messagess and if anything in publishQueue send it
    """
    def run(self):
        self.running = True
        self.LOGGER.info("Starting loop")

        last_lease_run = None
        last_accounting_run = None

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

                # Get host leases data every 10 seconds
                if not last_lease_run or time.time() - last_lease_run > config.mikrotik['lease_retrieve_interval']:
                    self.LOGGER.debug("Retrieving lease data")
                    lease_resource = client.get_resource('/ip/dhcp-server/lease/')
                    lease_list = lease_resource.get()
                    self.LOGGER.debug("Got {} leases".format(len(lease_list)))
                    res = []
                    for lease in lease_list:
                        comment = lease.get('comment') or ''
                        try:
                            tmp = comment.split(';;')
                            name = tmp[0]
                            color = tmp[1]
                            if not color.startswith('#'):
                                color = '#{}'.format(color)
                        except:
                            name = comment
                            color = '#44dddd'

                        res.append(
                            {'mac_address': lease.get('mac-address'),
                             'ip_address': lease.get('address'),
                             'host_name': lease.get('host-name') or 'unknown',
                             'name': name,
                             'chart_color': color,
                             'active': 1 if lease.get('status') == 'bound' else 0}
                        )

                    self._out_report_queue.put({'type': 'mikrotik_lease',
                                                'payload': res})
                    last_lease_run = time.time()
                    self.LOGGER.info("Lease data retrieved")

                # Get traffic accounting data
                # If first time running just take snapshot and discard results. It should prevent bandwidth spikes
                if not last_accounting_run:
                    traffic_resource = client.get_resource('/ip/accounting/snapshot/')
                    traffic_resource.call('take')
                    last_accounting_run = time.time()

                # Get result every second
                if time.time() - last_accounting_run > config.mikrotik['traffic_retrieve_interval']:
                    self.LOGGER.debug("Retrieving accounting data")
                    traffic_resource = client.get_resource('/ip/accounting/snapshot/')
                    traffic_resource.call('take')
                    traffic_list = traffic_resource.get()

                    self.LOGGER.debug("Got {} traffic rows".format(len(traffic_list)))

                    res = []
                    lan_regex = re.compile(config.mikrotik['lan_range_regex'])
                    for traffic in traffic_list:
                        source_ip = str(traffic.get('src-address')).strip()
                        destination_ip = str(traffic.get('dst-address')).strip()
                        bandwidth_count = str(traffic.get('bytes')).strip()
                        packet_count = str(traffic.get('packets')).strip()

                        if lan_regex.match(source_ip) and lan_regex.match(destination_ip):
                            traffic_type = 'local'
                            local_ip = source_ip
                        elif lan_regex.match(source_ip) and not lan_regex.match(destination_ip):
                            traffic_type = 'upload'
                            local_ip = source_ip
                        elif not lan_regex.match(source_ip) and lan_regex.match(destination_ip):
                            traffic_type = 'download'
                            local_ip = destination_ip
                        else:
                            traffic_type = 'wan'
                            local_ip = ''

                        res.append({
                            'run_interval': config.mikrotik['traffic_retrieve_interval'],
                            'type': traffic_type,
                            'source_ip': source_ip,
                            'destination_ip': destination_ip,
                            'local_ip': local_ip,
                            'bytes_count': bandwidth_count,
                            'packet_count': packet_count
                        })

                    self._out_report_queue.put({'type': 'mikrotik_traffic',
                                                'payload': res})
                    last_accounting_run = time.time()
                    self.LOGGER.info("Traffic data retrieved")

                # Work done, sleep a bit
                self.LOGGER.debug("..")
                sleep(.1)

            except Exception, e:
                self.LOGGER.error("Unexpected exception in loop\n***{}".format(e))
                self.running = False
                break
        # While loop broken
        self.LOGGER.warning("Thread exiting")
        return
