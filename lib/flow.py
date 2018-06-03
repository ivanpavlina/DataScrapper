# from logger import get_logger
from time import time
import re


class Flow:
    def __init__(self, logger, flow):
        self._name = None
        self.LOGGER = logger
        self._flow_config = flow
        self._last_run_timestamp = None

        self._name = str(flow['name'])
        self._params = tuple(flow['params'])
        self._run_interval = float(flow['run_interval'])
        self._mysql_type = str(flow['mysql_type'])
        self._mysql_table = str(flow['mysql_table'])

        # Custom method vars
        self.lan_traffic_usage_first_run = True
        self.interface_usage_list = ['ether1-gateway', 'ether2-master-local']

        # Check if method for flow exists
        if not self.__method_exists(self._name):
            raise Exception('Flow [{}] method not found in flow class'.format(self._name))

    def __method_exists(self, methodName):
        return hasattr(self.__class__, methodName) and callable(getattr(self.__class__, methodName))

    def __run_method(self, methodName, methodArgs=None):
        return getattr(self, methodName, None)(methodArgs)

    def get_flow_name(self):
        return self._name

    def get_flow_mysql_type(self):
        return self._mysql_type

    def get_flow_mysql_table(self):
        return self._mysql_table

    def get_params(self):
        return self._params

    # Called each thread loop pass, check if its time to execute method
    # Thread passes active api client object and it is passed to every method
    def loop_pass(self, client):
        if not self._last_run_timestamp or time() - self._last_run_timestamp > self._run_interval:
            self.LOGGER.debug("Running method {}".format(self._name))

            # Run flow custom method
            method_result = self.__run_method(self._name, client)

            # TODO
            # Check if response is okay
            self._last_run_timestamp = time()
            self.LOGGER.debug("Method finished successfully")

            if method_result:
                return {'name': self._name,
                        'payload': method_result}

    # Custom flow methods. Called exactly as flow name
    # Method must return tuple with all results in order defined in flow config

    # @RouterOSApiClient
    def dhcp_server_leases(self, client):
        lease_resource = client.get_resource('/ip/dhcp-server/lease/')
        lease_list = lease_resource.get()
        self.LOGGER.debug("Retrieved {} leases".format(len(lease_list)))
        result = []
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

            result.append(
                (
                    lease.get('mac-address'),
                    lease.get('address'),
                    lease.get('host-name') or 'unknown',
                    name,
                    color,
                    1 if lease.get('status') == 'bound' else 0
                )
            )

        return result

    # @RouterOSApiClient
    def lan_traffic_usage(self, client):
        self.LOGGER.debug("Retrieving lan trafic data")
        traffic_resource = client.get_resource('/ip/accounting/snapshot/')
        traffic_resource.call('take')
        traffic_list = traffic_resource.get()

        self.LOGGER.debug("Got {} lan traffic rows".format(len(traffic_list)))

        res = []
        # Determine traffic type
        # Use lan range regex for checking which host is in LAN
        # Ex '192\.168\.\d{1,3}\.\d{1,3}' for 192.168.0.0/16
        lan_regex = re.compile('192\.168\.\d{1,3}\.\d{1,3}')
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

            res.append(
                (
                    self._run_interval,
                    traffic_type,
                    source_ip,
                    destination_ip,
                    local_ip,
                    bandwidth_count,
                    packet_count
                )
            )
         
        # If its a first run don't return anything
        if self.lan_traffic_usage_first_run:
            self.lan_traffic_usage_first_run = False
        else:
            return res

    # @RouterOSApiClient
    def interface_usage(self, client):
        self.LOGGER.debug("Retrieving interface traffic data")
        resource = client.get_resource('/interface')

        interface_list = ",".join(self.interface_usage_list)
        interface_traffic_results = resource.call('monitor-traffic',
                                                  arguments={'interface': interface_list, 'once': ''})

        self.LOGGER.debug("Interface traffic data results >> {}".format(interface_traffic_results))
        res = []
        for interface_traffic in interface_traffic_results:
            res.append(
                (
                    interface_traffic['name'],
                    interface_traffic['rx-bits-per-second'],
                    interface_traffic['tx-bits-per-second'],
                    interface_traffic['rx-packets-per-second'],
                    interface_traffic['tx-packets-per-second'],
                    interface_traffic['rx-drops-per-second'],
                    interface_traffic['tx-drops-per-second'],
                    interface_traffic['rx-errors-per-second'],
                    interface_traffic['tx-errors-per-second']
                )
            )
        return res
