#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
import logging
import socket
import threading
import struct
from smbus2 import SMBusWrapper, i2c_msg
import rlib.rgpio as GPIO
import rlib.common as common
from rlib.common import CONST
import dlib.dsocket as dsocket
import dlib._dlisten as listen
from dlib.dcommon import GLOBAL, DResetTypes, DTolerance, DRange
from dlib.dstatus import STATUS
import dlib.devices.dbase as dbase
from dlib.dconfig import CONFIG, DConfig_Slave_Unilojas_C001

ID = {'modelid': 3,
      'name': 'Unilojas C001',
      'fmt_alarm': 0,
      'fmt_data': 1,
      'fmt_normal': 2,
      'fmt_codi': 0}

C001_READ_LENGHT = 27


# =============================================================================#
class Device():
    threadLock = threading.Lock()

    # smbus_id: 0 = /dev/i2c-0 (port I2C0), 1 = /dev/i2c-1 (port I2C1)
    def __init__(self, smbus_id: int, address: int):
        self._smbus_id = smbus_id
        self._address = address

    @property
    def smbus_id(self):
        return self._smbus_id

    @property
    def address(self):
        return self._address

    def read_data_as_report(self) -> dbase.DReport_C001:
        Device.threadLock.acquire()
        a = None
        try:
            read = i2c_msg.read(self._address, C001_READ_LENGHT)
            with SMBusWrapper(self._smbus_id) as smbus:
                smbus.i2c_rdwr(read)
            a = bytearray(read.buf[:read.len])
        finally:
            Device.threadLock.release()
            return dbase.DReport_C001(a)

    def read_data(self) -> {}:
        Device.threadLock.acquire()
        r = {}
        try:
            read = i2c_msg.read(self._address, C001_READ_LENGHT)
            with SMBusWrapper(self._smbus_id) as smbus:
                smbus.i2c_rdwr(read)
            a = bytearray(read.buf[:read.len])
            r['version'] = struct.unpack('H', a[0:2])[0]
            r['reles'] = a[2]
            r['gpaci'] = a[3]
            r['flow'] = struct.unpack('I', a[4:8])[0]
            r['temp1'] = struct.unpack('i', a[8:12])[0] / 100
            r['temp2'] = struct.unpack('i', a[12:16])[0] / 100
            r['temp3'] = struct.unpack('i', a[16:20])[0] / 100
            r['ad'] = struct.unpack('H', a[20:22])[0]
            r['watchdog_relay'] = a[22]
            r['watchdog_time'] = struct.unpack('i', a[23:27])[0] / 1000
        finally:
            Device.threadLock.release()
            return r

    def rele_state_word(self, state_word: int):
        Device.threadLock.acquire()
        try:
            buffer = [0x64, state_word]
            write = i2c_msg.write(self._address, buffer)
            with SMBusWrapper(self._smbus_id) as smbus:
                smbus.i2c_rdwr(write)
            time.sleep(.3)
        finally:
            Device.threadLock.release()

    def rele_state(self, rele_addr: int, state: bool):
        Device.threadLock.acquire()
        try:
            if rele_addr >= 0x65 and rele_addr <= 0x6b:
                if state:
                    s = 1
                else:
                    s = 0
                buffer = [rele_addr, 0, s]
                write = i2c_msg.write(self._address, buffer)
                with SMBusWrapper(self._smbus_id) as smbus:
                    smbus.i2c_rdwr(write)
                time.sleep(.3)
        finally:
            Device.threadLock.release()

    def rele_pulse(self, rele_addr: int, count: int, enlapse: int):
        Device.threadLock.acquire()
        try:
            if rele_addr >= 0x65 and rele_addr <= 0x6b:
                e = struct.pack('H', enlapse)
                buffer = [rele_addr, 1, count, e[0], e[1]]
                write = i2c_msg.write(self._address, buffer)
                with SMBusWrapper(self._smbus_id) as smbus:
                    smbus.i2c_rdwr(write)
                time.sleep((count * (enlapse / 1000) * 3))
        finally:
            Device.threadLock.release()

    def watch_dog(self, rele_addr: int = 0, enlapse: int = 0):
        Device.threadLock.acquire()
        try:
            if rele_addr >= 0x65 and rele_addr <= 0x6b:
                e = struct.pack('i', enlapse * 1000)
                buffer = [0x32, rele_addr, e[0], e[1], e[2], e[3]]
            else:
                buffer = [0x33]
            write = i2c_msg.write(self._address, buffer)
            with SMBusWrapper(self._smbus_id) as smbus:
                smbus.i2c_rdwr(write)
            time.sleep(.2)
        finally:
            Device.threadLock.release()


# =============================================================================#
class Device_Job(dbase.DSlave_Job):
    def __init__(self, slave_num: int, resources: dict):
        super().__init__(slave_num, resources)
        self._config = DConfig_Slave_Unilojas_C001(CONFIG.rconfig, slave_num)
        self._stack = resources['stack']
        self.nameid = '{} [{} addr:{}]'.format(self.slave_name, ID['name'], self._config.modbus_slave)
        self._alarm_state = False
        # i2c_atmega
        self._i2c_atmega = self._config.i2c_atmega
        # i2c_atmega_addr
        self.i2c_atmega_addr = self._config.i2c_atmega_addr
        # device
        self._device = Device(self._i2c_atmega, self.i2c_atmega_addr)
        # interval
        self._interval = self._config.interval
        # periodic
        self._periodic = self._config.periodic
        # watchdog_relay
        self._wdog_relay = self._config.watchdog_relay
        # watchdog_time
        self._wdog_time = self._config.watchdog_time
        # temp_tolerances
        self._temp_tolerances = {}
        # tolerance temp1
        self._temp_tolerances['temp1'] = self._config.tolerance_temp1
        # tolerance temp2
        self._temp_tolerances['temp2'] = self._config.tolerance_temp2
        # tolerance temp3
        self._temp_tolerances['temp3'] = self._config.tolerance_temp3
        # tolerance ad
        self._temp_tolerances['ad'] = self._config.tolerance_ad
        # temp_alarm_tolerances
        self._temp_alarm_ranges = {}
        # alarme temp1
        self._temp_alarm_ranges['temp1'] = self._config.alarm_temp1
        # alarme temp2
        self._temp_alarm_ranges['temp2'] = self._config.alarm_temp2
        # alarme temp3
        self._temp_alarm_ranges['temp3'] = self._config.alarm_temp3
        # gpiac1
        gpiac1 = self._config.gpi_ac1
        # gpiac2
        gpiac2 = self._config.gpi_ac2
        # gpiac3
        gpiac3 = self._config.gpi_ac3
        # report
        self._report = self._config.report_flag
        # alarm
        self._alarm = self._config.alarm_flag
        # event
        self._event = self._config.event_flag
        # priority report
        self._priority_report = self._config.priority_report
        # priority alarm
        self._priority_alarm = self._config.priority_alarm
        # priority event
        self._priority_event = self._config.priority_event
        # pinlock
        self._pinlock = self._config.pinlock
        # pinlockled
        self._pinlockled = self._config.pinlockled
        # pinbattery
        self._pinbattery = self._config.pinbattery
        # pinresetatmega
        self._pinresetatmega = self._config.pinresetatmega
        # create tolerance class
        self._tolerance = DTolerance(self._temp_tolerances)
        # create range class
        self._range = DRange(self._temp_alarm_ranges)
        # local logger
        self.logger = logging.getLogger(__name__)

    def run(self):
        try:
            self.logger.info('Iniciando job "{}" {}'.format(self.local, self.nameid))
            report_time = time.time() + self._periodic
            starting = True
            previous_data = None
            # inicia GPIOs
            if self._config.lockenable:
                GPIO.setup(self._pinlock, GPIO.GPIO.IN)
                GPIO.setup(self._pinlockled, GPIO.GPIO.OUT)
                lock_enable = True
            else:
                lock_enable = False
            if self._config.batterymonitor:
                GPIO.setup(self._pinbattery, GPIO.GPIO.IN)
                battery_monitor_enable = True
            else:
                battery_monitor_enable = False
            # inicia watchdog
            self._device.watch_dog()
            if self._wdog_time > 0:
                self.logger.debug('Starting WatchDog Relay:{} Time:{}'.format(self._wdog_relay, self._wdog_time))
                self._device.watch_dog(self._wdog_relay + 100, self._wdog_time)
            else:
                self._device.watch_dog()
            while not self.stopped():
                try:
                    if lock_enable:
                        # verifica trava
                        self.lock_monitoring()
                    if battery_monitor_enable:
                        # verifica bateria
                        self.battery_monitoring()
                    # get alarm
                    actual_data = self._device.read_data_as_report()
                    if self._alarm:
                        # check if trigger alarm by gpaci
                        check = self._range.check(actual_data.range(), self._alarm_state)
                        if check and not self._alarm_state:
                            # create alarm header
                            h = dsocket.DSocketHeader_Alarm.create(STATUS.id,
                                                                   int(time.time()),
                                                                   self.slave_num,
                                                                   ID['modelid'],
                                                                   ID['fmt_alarm'])
                            # send alarm
                            self._stack.put(self._priority_alarm, time.time(), h, actual_data.exchange_data())
                            # save alarm
                            self._alarm_state = True
                        elif not check and self._alarm_state:
                            # create alarm header
                            h = dsocket.DSocketHeader_Event.create(STATUS.id,
                                                                   int(time.time()),
                                                                   self.slave_num,
                                                                   ID['modelid'],
                                                                   ID['fmt_normal'])
                            # send alarm
                            self._stack.put(self._priority_alarm, time.time(), h, actual_data.exchange_data())
                            # save alarm
                            self._alarm_state = False
                    if self._report:
                        # check if send report
                        if report_time < time.time():
                            # clear report time counter
                            report_time = time.time() + self._periodic
                            # create report header
                            h = dsocket.DSocketHeader_Report.create(STATUS.id,
                                                                    int(time.time()),
                                                                    self.slave_num,
                                                                    ID['modelid'],
                                                                    ID['fmt_data'])
                            # send report
                            self._stack.put(self._priority_report, time.time(), h, actual_data.exchange_data())
                        if self._event:
                            # check if trigger an event (not applicable on start)
                            if not starting and not self.check_event(previous_data, actual_data):
                                # create report header
                                h = dsocket.DSocketHeader_Event.create(STATUS.id,
                                                                       int(time.time()),
                                                                       self.slave_num,
                                                                       ID['modelid'],
                                                                       ID['fmt_data'])
                                # send event
                                self._stack.put(self._priority_event, time.time(), h, actual_data.exchange_data())

                        # update previous data ...
                        previous_data = dbase.DReport_C001(actual_data.exchange_data())

                except Exception as err:
                    self.logger.debug(str(err))
                finally:
                    # clear start flag
                    if starting:
                        starting = False
                    # wait interval
                    time.sleep(self._interval)

            # para watchdog
            if self._wdog_time > 0:
                self.logger.debug('Stoping WatchDog ...')
                self._device.watch_dog()

            self.logger.info('Terminado job "{}" {}'.format(self.local, self.nameid))

        except Exception as err:
            print(type(err))
            self.logger.fatal('Thread Device_Job fail:{}'.format(str(err)))

    def lock_monitoring(self):
        if GPIO.input(self._config.pinlock):
            STATUS.clear_state(CONST.STATE_LOCK)
            GPIO.output(self._config.pinlockled, GPIO.GPIO.LOW)
        else:
            STATUS.set_state(CONST.STATE_LOCK)
            GPIO.output(self._config.pinlockled, GPIO.GPIO.HIGH)

    def battery_monitoring(self):
        r = self.battery_read()
        if r == GPIO.GPIO.FALLING:
            time.sleep(2)
            if self.battery_read() == GPIO.GPIO.FALLING:
                STATUS.set_state(CONST.STATE_LOAD_BATERY)
                STATUS.clear_state(CONST.STATE_ON_BATERY)
        elif r == GPIO.GPIO.LOW:
            STATUS.clear_state(CONST.STATE_ON_BATERY)
            STATUS.clear_state(CONST.STATE_LOAD_BATERY)
        else:
            STATUS.set_state(CONST.STATE_ON_BATERY)
            STATUS.clear_state(CONST.STATE_LOAD_BATERY)

    def battery_read(self) -> int:
        pin = self._config.pinbattery
        t = time.time() + 1
        a = GPIO.input(pin)
        while t > time.time():
            b = GPIO.input(pin)
            if a != b:
                return GPIO.GPIO.FALLING
        return a

    def check_event(self, previous: dbase.DReport_C001, actual: dbase.DReport_C001) -> bool:
        return self._tolerance.check(previous.tolerance(), actual.tolerance())


# =============================================================================#
class Device_Process(dbase.DSlave_Process):
    TIME_CONN_I2C = .1

    def __init__(self, conn: socket, addr, header: dsocket.DSocketHeaderBasic, data: common.RData, resources: dict):
        super().__init__(conn, addr, header, data)
        self.logger = logging.getLogger(__name__)

    def proc_cmd(self):
        try:
            header = dsocket.DSocketHeader_Cmd(self.header.exchange_data())
        except Exception as err:
            self.logger.debug('Erro inesperado {}'.format(str(err)))

    def proc_cmd_now(self):
        try:
            header = dsocket.DSocketHeader_CmdNow(self.header.exchange_data())
            if header.slave != 0 or (header.modelid != 3 and header.modelid != 0):
                raise ValueError('Invalid device on argument.')
            config = DConfig_Slave_Unilojas_C001(CONFIG.rconfig, header.slave)
            i2c = Device(config.i2c_atmega,
                         config.i2c_atmega_addr)
            # Register/Unregister
            if header.modelid == 0 and header.cmdtype == 0 and header.size == 1:
                if self._data[0] == 1:  # Register
                    STATUS.put_state(CONST.STATE_REGISTER, True)
                    CONFIG.config.set(CONST.MAIN, CONST.REGISTER, str(STATUS.is_state(CONST.STATE_REGISTER)))
                    CONFIG.config.write()
                    listen.replay_cmd_now_file(self._conn, CONST.DWEB_NAME + '.ini')
                elif self._data[0] == 0:  # Unregister
                    STATUS.status = 0
                    # DSTATUS.put_state(CONST.STATE_REGISTER, False)
                    CONFIG.config.set(CONST.MAIN, CONST.REGISTER, str(STATUS.is_state(CONST.STATE_REGISTER)))
                    CONFIG.config.write()
                    listen.replay_ok(self._conn)
                else:
                    listen.replay_err(CONST.RETURNCODE_CMD_INVALID)
            # Get Setup
            elif header.modelid == 0 and header.cmdtype == 1 and header.size == 0:
                listen.replay_cmd_now_file(self._conn, CONST.DWEB_NAME + '.ini')
            # Set Setup
            elif header.modelid == 0 and header.cmdtype == 1 and header.size != 0:
                try:
                    with open(CONST.DWEB_NAME + '.ini', 'wb') as f:
                        f.write(self._data)
                    GLOBAL.reset = DResetTypes.SOFT_RESET
                    GLOBAL.setup = True
                    listen.replay_ok(self._conn)
                except Exception as err:
                    self.logger.debug('Erro gravando setup {}'.format(str(err)))
                    listen.replay_err(CONST.RETURNCODE_WRITE_SETUP_ERR)
            # Restart/Reboot/Stop
            elif header.modelid == 0 and header.cmdtype == 2 and header.size == 1:
                if self._data[0] == 0:  # Restart
                    listen.replay_ok(self._conn)
                    time.sleep(5)
                    GLOBAL.reset = DResetTypes.SOFT_RESET
                elif self._data[0] == 1:  # Reboot
                    listen.replay_ok(self._conn)
                    time.sleep(5)
                    GLOBAL.reset = DResetTypes.HARD_RESET
                elif self._data[0] == 2:  # Stop
                    listen.replay_ok(self._conn)
                    time.sleep(5)
                    GLOBAL.running = False
                else:
                    listen.replay_err(CONST.RETURNCODE_CMD_INVALID)
            # Read
            elif header.modelid == 3 and header.cmdtype == 0 and header.size == 0:
                r = i2c.read_data_as_report()
                listen.replay_cmd_now(self._conn, r.exchange_data())
            # ReleStateWord
            elif header.modelid == 3 and header.cmdtype == 1 and header.size == 1:
                i2c.rele_state_word(self._data[0])
                time.sleep(Device_Process.TIME_CONN_I2C)
                r = i2c.read_data_as_report()
                if r.reles == self._data[0]:
                    listen.replay_ok(self._conn)
                else:
                    listen.replay_err(self._conn, CONST.RETURNCODE_RELE_FAIL)
            # ReleState
            elif header.modelid == 3 and header.cmdtype == 2 and header.size == 2 and self.rele_addr_check(
                    self._data[0]) and self.rele_state_check(self._data[1]):
                if self._data[1] == 0x00:
                    s = False
                else:
                    s = True
                i2c.rele_state(self._data[0] + 100, s)
                time.sleep(Device_Process.TIME_CONN_I2C)
                r = i2c.read_data_as_report()
                if r.get_rele(self._data[0] - 1) == s:
                    listen.replay_ok(self._conn)
                else:
                    listen.replay_err(self._conn, CONST.ETURNCODE_RELE_FAIL)
            # RelePulse
            elif header.modelid == 3 and header.cmdtype == 3 and header.size == 4 and self.rele_addr_check(
                    self._data[0]) and self.pulses_check(
                self._data[1]) and self.pulse_time_check(self._data.get_byte(common.RByteType.BYTE16, 2)):
                i2c.rele_pulse(self._data[0] + 100, self._data[1], self._data.get_byte(common.RByteType.BYTE16, 2))
                time.sleep(Device_Process.TIME_CONN_I2C)
                listen.replay_ok(self._conn)
            else:
                listen.replay_err(self._conn, CONST.RETURNCODE_CMD_INVALID)
        except Exception as err:
            self.logger.debug('Erro inesperado {}'.format(str(err)))
            listen.replay_err(self._conn, CONST.RETURNCODE_UNKNOWN)

    def rele_addr_check(self, addr: int) -> bool:
        return addr >= 1 and addr <= 7

    def rele_state_check(self, state: int) -> bool:
        return state == 0x00 or state == 0xff

    def pulses_check(self, pulses) -> bool:
        return pulses >= 1 and pulses <= 10

    def pulse_time_check(self, tm) -> bool:
        return tm >= 30 and tm <= 1000


# =============================================================================#
def reset_atmega():
    GPIO.output('PA06', GPIO.GPIO.LOW)
    time.sleep(.1)
    GPIO.output('PA06', GPIO.GPIO.HIGH)
    time.sleep(.5)


if __name__ == '__main__':
    # GPIO.setwarnings(False)
    # GPIO.setmode(12)
    # GPIO.setup('PA06', GPIO.GPIO.OUT)
    # reset_atmega()

    c001 = Device(1, 0x08)  # 7 bit address (will be left shifted to add the read write bit)

    # print(c001.read_data())
    # c001.watch_dog(0x67, 10)
    c001.watch_dog()
    print(c001.read_data())

    # while True:
    #     time.sleep(5)
    #     print(c001.read_data())


    # while True:
    #     print(c001.read_data())
    #     c001.rele_pulse(0x65, 5, 120)
    #     print(c001.read_data())
    #     c001.rele_pulse(0x66, 10, 100)
    #     print(c001.read_data())
    #     c001.rele_pulse(0x67, 10, 50)
    #     print(c001.read_data())
    #     c001.rele_pulse(0x68, 10, 150)
    #     print(c001.read_data())
    #     c001.rele_pulse(0x69, 10, 200)
    #     print(c001.read_data())
    #     c001.rele_pulse(0x6A, 15, 75)
    #     print(c001.read_data())
    #     c001.rele_pulse(0x6B, 5, 300)
    #     print(c001.read_data())
    #     c001.rele_state_word(0x55)
    #     print(c001.read_data())
    #     c001.rele_state_word(0x2a)
    #     print(c001.read_data())
    #     c001.rele_state_word(0x7f)
    #     print(c001.read_data())
    #     c001.rele_state_word(0x3f)
    #     print(c001.read_data())
    #     c001.rele_state_word(0x3e)
    #     print(c001.read_data())
    #     c001.rele_state_word(0x1e)
    #     print(c001.read_data())
    #     c001.rele_state_word(0x0e)
    #     print(c001.read_data())
    #     c001.rele_state_word(0x0a)
    #     print(c001.read_data())
    #     c001.rele_state_word(0x0)
    #     print(c001.read_data())
