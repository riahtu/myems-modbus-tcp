import json
import mysql.connector
import config
from modbus_tk import modbus_tcp
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import math
import telnetlib


# global flag indicates the connectivity with the MQTT broker
mqtt_connected_flag = False


# the on_connect callback function for MQTT client
# refer to http://www.steves-internet-guide.com/client-connections-python-mqtt/
def on_mqtt_connect(client, userdata, flags, rc):
    global mqtt_connected_flag
    if rc == 0:
        mqtt_connected_flag = True  # set flag
        print("MQTT connected OK")
    else:
        print("Bad MQTT connection Returned code=", rc)
        mqtt_connected_flag = False


# the on_disconnect callback function for MQTT client
# refer to http://www.steves-internet-guide.com/client-connections-python-mqtt/
def on_mqtt_disconnect(client, userdata, rc=0):
    global mqtt_connected_flag

    print("DisConnected, result code "+str(rc))
    mqtt_connected_flag = False


########################################################################################################################
# Acquisition Procedures
# Step 1: telnet hosts
# Step 2: Get point list
# Step 3: Read point values from MODBUS slaves
# Step 4: Publish point values to MQTT
# Step 5: Bulk insert point values into historical database
########################################################################################################################


def process(logger, data_source_id, host, port):

    while True:
        # the outermost while loop

        ################################################################################################################
        # Step 1: telnet hosts
        ################################################################################################################
        try:
            telnetlib.Telnet(host, port, 10)
            print("Succeeded to telnet %s:%s in acquisition process ", host, port)
        except Exception as e:
            logger.error("Failed to telnet %s:%s in acquisition process: %s  ", host, port, str(e))
            time.sleep(300)
            continue

        ################################################################################################################
        # Step 2: Get point list
        ################################################################################################################
        cnx_system_db = None
        cursor_system_db = None
        try:
            cnx_system_db = mysql.connector.connect(**config.myems_system_db)
            cursor_system_db = cnx_system_db.cursor()
        except Exception as e:
            logger.error("Error in step 2.1 of acquisition process " + str(e))
            if cursor_system_db:
                cursor_system_db.close()
            if cnx_system_db:
                cnx_system_db.close()
            # sleep and then continue the outermost loop to reload points
            time.sleep(60)
            continue

        try:
            query = (" SELECT id, name, object_type, is_trend, ratio, address "
                     " FROM tbl_points "
                     " WHERE data_source_id = %s "
                     " ORDER BY id ")
            cursor_system_db.execute(query, (data_source_id, ))
            rows_point = cursor_system_db.fetchall()
        except Exception as e:
            logger.error("Error in step 2.2 of acquisition process: " + str(e))
            # sleep several minutes and continue the outer loop to reload points
            time.sleep(60)
            continue
        finally:
            if cursor_system_db:
                cursor_system_db.close()
            if cnx_system_db:
                cnx_system_db.close()

        if rows_point is None or len(rows_point) == 0:
            # there is no points for this data source
            logger.error("Point Not Found in Data Source (ID = %s), acquisition process terminated ", data_source_id)
            # sleep 60 seconds and go back to the begin of outermost while loop to reload points
            time.sleep(60)
            continue

        # There are points for this data source
        point_list = list()
        for row_point in rows_point:
            point_list.append({"id": row_point[0],
                               "name": row_point[1],
                               "object_type": row_point[2],
                               "is_trend": row_point[3],
                               "ratio": row_point[4],
                               "address": row_point[5]})

        ################################################################################################################
        # Step 3: Read point values from MODBUS slaves
        ################################################################################################################
        # connect to historical database
        cnx_historical_db = None
        cursor_historical_db = None
        try:
            cnx_historical_db = mysql.connector.connect(**config.myems_historical_db)
            cursor_historical_db = cnx_historical_db.cursor()
        except Exception as e:
            logger.error("Error in step 3.1 of acquisition process " + str(e))
            if cursor_historical_db:
                cursor_historical_db.close()
            if cnx_historical_db:
                cnx_historical_db.close()
            # sleep 60 seconds and go back to the begin of outermost while loop to reload points
            time.sleep(60)
            continue

        # connect to the mqtt broker for publishing real time data
        mqc = None
        try:
            mqc = mqtt.Client(client_id=str(data_source_id) + "-" + str(time.time()))
            mqc.username_pw_set(config.myems_mqtt_broker['username'], config.myems_mqtt_broker['password'])
            mqc.on_connect = on_mqtt_connect
            mqc.on_disconnect = on_mqtt_disconnect
            mqc.connect_async(config.myems_mqtt_broker['host'], config.myems_mqtt_broker['port'], 60)
            # The loop_start() starts a new thread, that calls the loop method at regular intervals for you.
            # It also handles re-connects automatically.
            mqc.loop_start()
        except Exception as e:
            logger.error("MQTT Client Connection error " + str(e))
            # ignore this exception, does not stop the procedure
            pass

        # connect to the Modbus data source
        master = modbus_tcp.TcpMaster(host=host, port=port, timeout_in_sec=5.0)
        master.set_timeout(5.0)
        print("Ready to connect to %s:%s ", host, port)

        # inner loop to read all point values within a configurable period
        while True:
            is_modbus_tcp_timed_out = False
            energy_value_list = list()
            analog_value_list = list()
            digital_value_list = list()

            # foreach point loop
            for point in point_list:
                try:
                    address = json.loads(point['address'], encoding='utf-8')
                except Exception as e:
                    logger.error("Error in step 3.2 of acquisition process: \n"
                                 "Invalid point address in JSON " + str(e))
                    continue

                if 'slave_id' not in address.keys() \
                    or 'function_code' not in address.keys() \
                    or 'offset' not in address.keys() \
                    or 'number_of_registers' not in address.keys() \
                    or 'format' not in address.keys() \
                    or address['slave_id'] < 1 \
                    or address['function_code'] not in (1, 2, 3, 4) \
                    or address['offset'] < 0 \
                    or address['number_of_registers'] < 0 \
                        or len(address['format']) < 1:

                    logger.error('Data Source(ID=%s), Point(ID=%s) Invalid address data.',
                                 data_source_id, point['id'])
                    # invalid point is found, and go on the foreach point loop to process next point
                    continue

                # read register value for valid point
                try:
                    result = master.execute(slave=address['slave_id'],
                                            function_code=address['function_code'],
                                            starting_address=address['offset'],
                                            quantity_of_x=address['number_of_registers'],
                                            data_format=address['format'])
                except Exception as e:
                    logger.error(str(e) +
                                 " host:" + host + " port:" + str(port) +
                                 " slave_id:" + str(address['slave_id']) +
                                 " function_code:" + str(address['function_code']) +
                                 " starting_address:" + str(address['offset']) +
                                 " quantity_of_x:" + str(address['number_of_registers']) +
                                 " data_format:" + str(address['format']))

                    if 'timed out' in str(e):
                        is_modbus_tcp_timed_out = True
                        # timeout error, break the foreach point loop
                        break
                    else:
                        # exception occurred when read register value, go on the foreach point loop
                        continue

                if result is None or not isinstance(result, tuple) or len(result) == 0:
                    # invalid result, and go on the foreach point loop to process next point
                    logger.error("Error in step 3.3 of acquisition process: \n"
                                 " invalid result: None "
                                 " for point_id: " + str(point['id']))
                    continue

                if not isinstance(result[0], float) and not isinstance(result[0], int) or math.isnan(result[0]):
                    # invalid result, and go on the foreach point loop to process next point
                    logger.error(" Error in step 3.4 of acquisition process:\n"
                                 " invalid result: not float and not int or not a number "
                                 " for point_id: " + str(point['id']))
                    continue

                value = result[0]
                if point['ratio'] is not None and isinstance(point['ratio'], float):
                    value *= point['ratio']

                if point['object_type'] == 'ANALOG_VALUE':
                    analog_value_list.append({'data_source_id': data_source_id,
                                              'point_id': point['id'],
                                              'is_trend': point['is_trend'],
                                              'value': value})
                elif point['object_type'] == 'ENERGY_VALUE':
                    energy_value_list.append({'data_source_id': data_source_id,
                                              'point_id': point['id'],
                                              'is_trend': point['is_trend'],
                                              'value': value})
                elif point['object_type'] == 'DIGITAL_VALUE':
                    digital_value_list.append({'data_source_id': data_source_id,
                                               'point_id': point['id'],
                                               'is_trend': point['is_trend'],
                                               'value': value})

            # end of foreach point loop

            if is_modbus_tcp_timed_out:
                # modbus TCP connection timeout error

                # destroy the modbus master
                del master

                # destroy mqtt client
                mqc.disconnect()
                del mqc

                # close the connection to database
                if cursor_historical_db:
                    cursor_historical_db.close()
                if cnx_historical_db:
                    cnx_historical_db.close()

                # break the inner while loop to reconnect the modbus device
                time.sleep(60)
                break

            ############################################################################################################
            # Step 4: Publish point values to MQTT
            ############################################################################################################
            if len(analog_value_list) > 0 and mqtt_connected_flag:
                for point_value in analog_value_list:
                    try:
                        # publish real time value to mqtt broker
                        payload = json.dumps({"data_source_id": point_value['data_source_id'],
                                              "point_id": point_value['point_id'],
                                              "value": point_value['value']})
                        print('payload=' + str(payload))
                        info = mqc.publish('myems/point/' + str(point_value['point_id']),
                                           payload=payload,
                                           qos=0,
                                           retain=True)
                    except Exception as e:
                        logger.error("MQTT Publish Error in step 4 of acquisition process: " + str(e))
                        # ignore this exception, does not stop the procedure
                        pass

            if len(energy_value_list) > 0 and mqtt_connected_flag:
                for point_value in energy_value_list:
                    try:
                        # publish real time value to mqtt broker
                        payload = json.dumps({"data_source_id": point_value['data_source_id'],
                                              "point_id": point_value['point_id'],
                                              "value": point_value['value']})
                        print('payload=' + str(payload))
                        info = mqc.publish('myems/point/' + str(point_value['point_id']),
                                           payload=payload,
                                           qos=0,
                                           retain=True)
                    except Exception as e:
                        logger.error("MQTT Publish Error in step 4 of acquisition process: " + str(e))
                        # ignore this exception, does not stop the procedure
                        pass

            if len(digital_value_list) > 0 and mqtt_connected_flag:
                for point_value in digital_value_list:
                    try:
                        # publish real time value to mqtt broker
                        payload = json.dumps({"data_source_id": point_value['data_source_id'],
                                              "point_id": point_value['point_id'],
                                              "value": point_value['value']})
                        print('payload=' + str(payload))
                        info = mqc.publish('myems/point/' + str(point_value['point_id']),
                                           payload=payload,
                                           qos=0,
                                           retain=True)
                    except Exception as e:
                        logger.error("MQTT Publish Error in step 4 of acquisition process: " + str(e))
                        # ignore this exception, does not stop the procedure
                        pass

            ############################################################################################################
            # Step 5: Bulk insert point values into historical database
            ############################################################################################################
            # check the connection to the Historical Database
            if not cnx_historical_db.is_connected():
                try:
                    cnx_historical_db = mysql.connector.connect(**config.myems_historical_db)
                    cursor_historical_db = cnx_historical_db.cursor()
                except Exception as e:
                    logger.error("Error in step 5.1 of acquisition process: " + str(e))
                    if cursor_historical_db:
                        cursor_historical_db.close()
                    if cnx_historical_db:
                        cnx_historical_db.close()
                    # sleep some seconds
                    time.sleep(60)
                    continue

            current_datetime_utc = datetime.utcnow()
            # bulk insert values into historical database within a period
            if len(analog_value_list) > 0:
                add_values = (" INSERT INTO tbl_analog_value (point_id, utc_date_time, actual_value) "
                              " VALUES  ")
                trend_value_count = 0

                for point_value in analog_value_list:
                    if point_value['is_trend'] and isinstance(point_value['value'], float):
                        add_values += " (" + str(point_value['point_id']) + ","
                        add_values += "'" + current_datetime_utc.isoformat() + "',"
                        add_values += str(point_value['value']) + "), "
                        trend_value_count += 1

                if trend_value_count > 0:
                    try:
                        # trim ", " at the end of string and then execute
                        cursor_historical_db.execute(add_values[:-2])
                        cnx_historical_db.commit()

                    except Exception as e:
                        logger.error("Error in step 5.2 of acquisition process: " +
                                     "Data Source ID=%s, Point ID=%s Error: %s",
                                     (point_value['data_source_id'],
                                      point_value['point_id'],
                                      str(e)))
                        # ignore this exception
                        pass

            if len(energy_value_list) > 0:
                add_values = (" INSERT INTO tbl_energy_value (point_id, utc_date_time, actual_value) "
                              " VALUES  ")
                trend_value_count = 0

                for point_value in energy_value_list:
                    if point_value['is_trend'] and isinstance(point_value['value'], float):
                        add_values += " (" + str(point_value['point_id']) + ","
                        add_values += "'" + current_datetime_utc.isoformat() + "',"
                        add_values += str(point_value['value']) + "), "
                        trend_value_count += 1

                if trend_value_count > 0:
                    try:
                        # trim ", " at the end of string and then execute
                        cursor_historical_db.execute(add_values[:-2])
                        cnx_historical_db.commit()
                    except Exception as e:
                        logger.error("Error in step 5.3 of acquisition process: " +
                                     "Data Source ID=%s, Point ID=%s Error: %s",
                                     (point_value['data_source_id'],
                                      point_value['point_id'],
                                      str(e)))
                        # ignore this exception
                        pass

            if len(digital_value_list) > 0:
                add_values = (" INSERT INTO tbl_digital_value (point_id, utc_date_time, actual_value) "
                              " VALUES  ")
                trend_value_count = 0

                for point_value in digital_value_list:
                    if point_value['is_trend'] and isinstance(point_value['value'], int):
                        add_values += " (" + str(point_value['point_id']) + ","
                        add_values += "'" + current_datetime_utc.isoformat() + "',"
                        add_values += str(point_value['value']) + "), "
                        trend_value_count += 1

                if trend_value_count > 0:
                    try:
                        # trim ", " at the end of string and then execute
                        cursor_historical_db.execute(add_values[:-2])
                        cnx_historical_db.commit()
                    except Exception as e:
                        logger.error("Error in step 5.4 of acquisition process: " +
                                     "Data Source ID=%s, Point ID=%s Error: %s",
                                     (point_value['data_source_id'],
                                      point_value['point_id'],
                                      str(e)))
                        # ignore this exception
                        pass

            # sleep some seconds
            time.sleep(config.periods['save_to_database'])

        # end of inner while loop

    # end of outermost while loop
