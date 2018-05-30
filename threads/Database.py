from threading import Thread
from time import sleep
import MySQLdb
import json
from etc import config
from lib.logger import get_logger
import time
from lib.flow import Flow

class Database(Thread):

    def __init__(self, in_received_queue):
        # Setup thread stuff
        Thread.__init__(self)
        self.threadID = 1
        self.daemon = True
        self.LOGGER = get_logger(type(self).__name__)

        # Setup inputs
        self._in_received_queue = in_received_queue

        # Setup flags
        self.running = False
        self.wantRunning = True  # Can be changed from main

        self._connect_db_client()

        self._flows = []
        for flow in config.flows:
            try:
                obj = Flow(self.LOGGER, flow)
                self._flows.append(obj)
            except Exception, e:
                self.LOGGER.error("Could not setup flow [{}]\n{}".format(flow, e))

    def __get_current_time_ms(self):
        return int(time.time())

    def _connect_db_client(self):
        self.LOGGER.debug("Connecting to MySQL DB")
        # If not connected block thread for 10 seconds and try again
        try:
            self._db_client = MySQLdb.connect(config.mysql['host'],
                                              config.mysql['username'],
                                              config.mysql['password'],
                                              config.mysql['db'])

        except Exception, e:
            # If not connected block thread for few seconds and try again
            self.LOGGER.error("Error connecting to MySQL DB\n***{}".format(e))
            self.LOGGER.error("Sleeping for 5 seconds and trying again...")
            sleep(5)
            return self._connect_db_client()

        self.LOGGER.info("Connected to MySQL DB")

    def _disconnect_db_client(self):
        try:
            self._db_client.close()
        except Exception, e:
            # If not connected block thread for 10 seconds and try again
            self.LOGGER.error("Error disconnecting MySQL DB\n***{}".format(e))

    def _select_query(self, query):
        self.LOGGER.debug("Executing select query\n{}".format(query))
        try:
            cursor = self._db_client.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            # self._db_client.commit()
            self.LOGGER.debug("Got result\n{}".format(results))

            return results
        except Exception, e:
            self.LOGGER.error("Error executing select query\n{}\n***{}".format(query, e))
            return None

    def _update_query(self, query, args):
        self.LOGGER.debug("Executing update query\n{}with agruments:\n{}".format(query, args))
        try:
            cursor = self._db_client.cursor()
            sql_query = query.format(*args)
            self.LOGGER.debug("Merged query:\n{}".format(sql_query))
            affected_rows = cursor.execute(sql_query)
            self._db_client.commit()
            cursor.close()
            return affected_rows
        except Exception, e:
            self.LOGGER.error("Error executing update query {}\nwith agruments:\n{}\n***{}".format(query, args, e))
            return None

    def _insert_query(self, query, args):
        self.LOGGER.debug("Executing insert query\n{}\nwith agruments:\n{}".format(query, args))
        try:
            cursor = self._db_client.cursor()
            affected_rows = cursor.executemany(query, args)
            self._db_client.commit()
            cursor.close()
            self.LOGGER.debug('Data inserted successfully')
            return affected_rows
        except Exception, e:
            self.LOGGER.error("Error executing update query {}\nwith agruments:\n{}\n***{}".format(query, args, e))
            return None

    def _run_stored_procedure(self, sp_name, in_vars=None, out_vars=None, commit=True):
        __outvar_types = {
            'err_code': 'int',
            'err_desc': 'str'
        }

        self.LOGGER.debug("Starting stored procedure [{}]".format(sp_name))
        self.LOGGER.debug("IN vars [{}]".format(in_vars))
        self.LOGGER.debug("OUT vars [{}]".format(out_vars))

        time_start = self.__get_current_time_ms()

        response = True
        try:
            cursor = self._db_client.cursor()

            # Start building stored procedure variables list
            # Just copy input variables first
            sp_args = list(in_vars or [])

            # If there are output variables add them to the list with default value, depending on the type defined above
            if out_vars:
                for var in out_vars:
                    try:
                        if __outvar_types[var] == 'int':
                            sp_args.append(0)
                        elif __outvar_types[var] == 'str':
                            sp_args.append('')
                        else:
                            # If output variable type is not defined RAISE an exception
                            raise Exception
                    except Exception:
                        self.LOGGER.error("Output variable [{}] type not defined".format(var))
                        raise
            self.LOGGER.debug("Stored procedure vars [{}]".format(sp_args))

            # Run stored procedure, RAISE an exception if failed
            result_sp = cursor.callproc(sp_name, sp_args)

            if not result_sp:
                self.LOGGER.error("Unable to run stored procedure")
                raise Exception

            self.LOGGER.debug("Stored procedure executed successfully {}".format(result_sp))

            # If there are out vars let's get their values after running stored procedure
            if out_vars:
                if not in_vars:
                    # If there are not any input variables we can retrieve data from cursor
                    select_results = cursor.fetchall()
                else:
                    # Otherwise additional select query is required

                    # Also cursor object has to be reinitialized..
                    # TODO check if fixable
                    cursor.close()
                    cursor = self._db_client.cursor()

                    # Building select query
                    select_list = []
                    out_idx_start, out_end = len(in_vars or []), len(in_vars or [])+len(out_vars)
                    for out_idx in range(out_idx_start, out_end):
                        select_item = "@_{}_{}".format(sp_name, out_idx)
                        select_list.append(select_item)

                    select_query = "SELECT " + ",".join(select_list)
                    self.LOGGER.debug("Running select query after stored procedure [{}]".format(select_query))

                    # Execute select query
                    result_select = cursor.execute(select_query)
                    if not result_select:
                        self.LOGGER.error("Unable to retrieve output vars values")
                        raise Exception

                    # Build response dictionary
                    select_results = cursor.fetchall()

                self.LOGGER.debug("Raw results: [{}]".format(select_results))
                response = {}
                for idx, var_name in enumerate(out_vars):
                    if __outvar_types[var_name] == 'int':
                        try:
                            response[var_name] = int(select_results[0][idx])
                        except:
                            response[var_name] = 0
                    elif __outvar_types[var_name] == 'str':
                        try:
                            response[var_name] = str(select_results[0][idx])
                        except:
                            response[var_name] = ''

            self.LOGGER.debug("Parsed results: [{}]".format(response))

            cursor.close()

            # If requested commit data
            if commit:
                self.LOGGER.debug("Commiting changes")
                self._db_client.commit()

        except Exception, e:
            # If error occurred return false for error
            self.LOGGER.error("Error executing stored procedure >> {}".format(e))
            response = False

        time_end = self.__get_current_time_ms()
        time_diff = float(time_end - time_start) / 1000
        self.LOGGER.debug("[TIMING][STORED PROCEDURE] {} sec".format(time_diff))

        return response

    def run(self):
        self.running = True
        self.LOGGER.info("Starting loop")
        while True:
            time_start = self.__get_current_time_ms()

            try:
                # Call disconnect if main wants to quit and client is still connected
                if not self.wantRunning and self._db_client.open:
                    self.LOGGER.info("Disconnecting MySQL client")
                    self._disconnect_db_client()

                # Client is disconnected and main wants out, break out while loop
                if not self.wantRunning:
                    self.LOGGER.info("Breaking loop")
                    self.running = False
                    break

                # Connect client
                if not self._db_client.open:
                    self.LOGGER.info("Connecting DB client")
                    return self._connect_db_client()

                # Check if there are updated topics by MQTT
                # If found insert event in DB so we can work it in next step
                if not self._in_received_queue.empty():
                    job = self._in_received_queue.get()

                    if job['name'] == 'dhcp_server_leases':
                        query = """INSERT INTO hosts (mac_address, ip_address, host_name, name, chart_color, active)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE 
                                      mac_address = VALUES(mac_address),
                                      ip_address = VALUES(ip_address),
                                      host_name = VALUES(host_name),
                                      name = VALUES(name),
                                      chart_color = VALUES(chart_color),
                                      active = VALUES(active)"""
                        query_vars = []
                        for data in job['payload']:
                            query_vars.append(
                                (data['mac_address'],
                                 data['ip_address'],
                                 data['host_name'],
                                 data['name'],
                                 data['chart_color'],
                                 data['active']))

                        self._insert_query(query, query_vars)
                        self.LOGGER.info("Inserted lease data into DB")
                    elif job['name'] == 'lan_traffic_usage':
                        query = """INSERT INTO traffic_lan 
                            (run_interval, type, source_ip, destination_ip, local_ip, bytes_count, packet_count)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                        query_vars = []
                        for data in job['payload']:
                            query_vars.append(
                                (data['run_interval'],
                                 data['type'],
                                 data['source_ip'],
                                 data['destination_ip'],
                                 data['local_ip'],
                                 data['bytes_count'],
                                 data['packet_count']))
                        self._insert_query(query, query_vars)
                        self.LOGGER.info("Inserted traffic data into DB")

                    elif job['name'] == 'interface_usage':
                        query = """INSERT INTO traffic_interface
                            (name, 
                             rx_bits_ps, tx_bits_ps,
                             rx_packets_ps, tx_packets_ps,
                             rx_drops_ps, tx_drops_ps,
                             rx_error_ps, tx_error_ps
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                        query_vars = []
                        for data in job['payload']:
                            query_vars.append(
                                (data['name'],
                                 data['rx-bits-per-second'],
                                 data['tx-bits-per-second'],
                                 data['rx-packets-per-second'],
                                 data['tx-packets-per-second'],
                                 data['rx-drops-per-second'],
                                 data['tx-drops-per-second'],
                                 data['rx-errors-per-second'],
                                 data['tx-errors-per-second']))
                        self._insert_query(query, query_vars)
                        self.LOGGER.info("Inserted traffic data into DB")
                    else:
                        self.LOGGER.error("Invalid job type [{}] received".format(job['name']))

                    self._db_client.commit()
            except Exception, e:
                self.LOGGER.error("Unexpected exception in loop\n***{}".format(e))
                self.running = False
                if self._db_client.open:
                    self.LOGGER.info("Disconnecting MySQL client")
                    self._disconnect_db_client()
                break

            time_end = self.__get_current_time_ms()
            time_diff = float(time_end - time_start) / 1000
            self.LOGGER.debug("[TIMING][LOOP] {} sec".format(time_diff))

            sleep(.5)
        # While loop broken
        self.LOGGER.warning("Database Thread exiting")
        return
