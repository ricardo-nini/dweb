#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import socket
import time
import threading
import struct
from smbus2 import SMBusWrapper, i2c_msg
import rlib.rgpio as GPIO
import rlib.common as common
import dlib.dsocket as dsocket
import dlib._dlisten as listen
import dlib.dslave as dslave
from dlib.dcommon import GLOBAL, DResetTypes
from dlib.dstatus import STATUS
from dlib.dconfig import DConfig_Slave, CONFIG
from rlib.common import CONST
from rlib.common import RByteType as BTYPE

ID = {'modelid': 3,
      'name': 'Unilojas C001',
      'fmt_alarm': 0,
      'fmt_data': 1,
      'fmt_normal': 2,
      'fmt_codi': 0}

C001_READ_LENGHT = 27


# =============================================================================#
class DUnilojas_C001_Config(DConfig_Slave):
    def __init__(self, rconfig: common.RConfig, slave_num: int):
        super().__init__(rconfig, slave_num)

    @property
    def modbus_slave(self):
        return 0  # proprio dispositivo, sempre 0

    @property
    def batterymonitor(self) -> bool:
        return self.config.getboolean(self.slave_name, CONST.BATTERYMONITOR, fallback=False)

    @property
    def pinbattery(self) -> str:
        return self.config.get(self.slave_name, CONST.PINBATTERY)

    @property
    def pinresetatmega(self) -> str:
        return self.config.get(self.slave_name, CONST.PINRESETATMEGA)

    @property
    def lockenable(self) -> bool:
        return self.config.getboolean(self.slave_name, CONST.LOCKENABLE, fallback=False)

    @property
    def pinlock(self) -> str:
        return self.config.get(self.slave_name, CONST.PINLOCK)

    @property
    def pinlockled(self) -> str:
        return self.config.get(self.slave_name, CONST.PINLOCKLED)

    @property
    def i2c_atmega(self) -> int:
        atmega = self.config.getint(self.slave_name, CONST.I2C_ATMEGA)
        if self.config.has_option(self.slave_name, CONST.I2C_DISPLAY):
            display = self.config.getint(self.slave_name, CONST.I2C_DISPLAY)
            if atmega == display:
                raise ValueError('{} = {}'.format(CONST.I2C_ATMEGA, CONST.I2C_DISPLAY))
        return atmega

    @property
    def i2c_atmega_addr(self) -> int:
        return self.config.getint(self.slave_name, CONST.I2C_ATMEGA_ADDR)

    @property
    def i2c_display(self) -> int:
        display = self.config.getint(self.slave_name, CONST.I2C_DISPLAY)
        if self.config.has_option(self.slave_name, CONST.I2C_ATMEGA):
            atmega = self.config.getint(self.slave_name, CONST.I2C_ATMEGA)
            if display == atmega:
                raise ValueError('{} = {}'.format(CONST.I2C_DISPLAY, CONST.I2C_ATMEGA))
        return display

    @property
    def i2c_display_addr(self) -> int:
        return self.config.getint(self.slave_name, CONST.I2C_DISPLAY_ADDR)

    @property
    def interval(self) -> float:
        return self.config.getfloat(self.slave_name, CONST.INTERVAL, fallback=500) / 1000

    @property
    def periodic(self) -> int:
        return self.config.getint(self.slave_name, CONST.PERIODIC, fallback=0) * 60

    @property
    def watchdog_relay(self) -> int:
        return 7  # Fixado no rele 7 para essa funcao

    @property
    def watchdog_time(self) -> int:
        return self.config.getint(self.slave_name, CONST.WATCHDOG_TIME, fallback=0)

    @property
    def tolerance_temp1(self) -> []:
        return self.get_tolerance(self.slave_name, '{}_{}{}'.format(CONST.TOLERANCE, CONST.TEMP, '1'))

    @property
    def tolerance_temp2(self) -> []:
        return self.get_tolerance(self.slave_name, '{}_{}{}'.format(CONST.TOLERANCE, CONST.TEMP, '2'))

    @property
    def tolerance_temp3(self) -> []:
        return self.get_tolerance(self.slave_name, '{}_{}{}'.format(CONST.TOLERANCE, CONST.TEMP, '3'))

    @property
    def tolerance_ad(self) -> []:
        return self.get_tolerance(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.AD))

    @property
    def alarm_temp1(self) -> []:
        return self.get_range(self.slave_name, '{}_{}{}'.format(CONST.ALARM, CONST.TEMP, '1'))

    @property
    def alarm_temp2(self) -> []:
        return self.get_range(self.slave_name, '{}_{}{}'.format(CONST.ALARM, CONST.TEMP, '2'))

    @property
    def alarm_temp3(self) -> []:
        return self.get_range(self.slave_name, '{}_{}{}'.format(CONST.ALARM, CONST.TEMP, '3'))

    @property
    def gpi_ac1(self):
        return self.config.getint(self.slave_name, '{}{}'.format(CONST.GPI_AC, '1'), fallback=0)

    @property
    def gpi_ac2(self):
        return self.config.getint(self.slave_name, '{}{}'.format(CONST.GPI_AC, '2'), fallback=0)

    @property
    def gpi_ac3(self):
        return self.config.getint(self.slave_name, '{}{}'.format(CONST.GPI_AC, '3'), fallback=0)

    @property
    def gpi_ac4(self):
        return self.config.getint(self.slave_name, '{}{}'.format(CONST.GPI_AC, '4'), fallback=0)

    @property
    def report_flag(self) -> bool:
        return self.config.getboolean(self.slave_name, CONST.REPORT, fallback=False)

    @property
    def alarm_flag(self) -> bool:
        return self.config.getboolean(self.slave_name, CONST.ALARM, fallback=False)

    @property
    def event_flag(self) -> bool:
        return self.config.getboolean(self.slave_name, CONST.EVENT, fallback=False)

    @property
    def priority_report(self) -> int:
        return self.config.getint(self.slave_name, '{}_{}'.format(CONST.PRIORITY, CONST.REPORT), fallback=0)

    @property
    def priority_alarm(self) -> int:
        return self.config.getint(self.slave_name, '{}_{}'.format(CONST.PRIORITY, CONST.ALARM), fallback=0)

    @property
    def priority_event(self) -> int:
        return self.config.getint(self.slave_name, '{}_{}'.format(CONST.PRIORITY, CONST.EVENT), fallback=0)


# =============================================================================#
class DUnilojas_C001_Report(object):
    _SIZE = 27
    _VERSION = 0
    _RELES = 2
    _GPACI = 3
    _FLOW = 4
    _TEMP1 = 8
    _TEMP2 = 12
    _TEMP3 = 16
    _AD = 20
    _WDOG_RELAY = 22
    _WDOG_TIME = 23

    def __init__(self, data=None):
        if not data:
            self._data = common.RData(bytearray(DUnilojas_C001_Report._SIZE))
        elif len(data) < DUnilojas_C001_Report._SIZE:
            self._data = common.RData(data)
            for x in range(len(data), DUnilojas_C001_Report._SIZE):
                self._data.append(0)
        else:
            self._data = common.RData(data[:DUnilojas_C001_Report._SIZE])

    @property
    def version(self):
        return self._data.get_byte(BTYPE.BYTE16, DUnilojas_C001_Report._VERSION)

    @version.setter
    def version(self, value):
        self._data.set_byte(BTYPE.BYTE16, DUnilojas_C001_Report._VERSION, value)

    @property
    def reles(self):
        return self._data.get_byte(BTYPE.BYTE8, DUnilojas_C001_Report._RELES)

    @reles.setter
    def reles(self, value):
        self._data.set_byte(BTYPE.BYTE8, DUnilojas_C001_Report._RELES, value)

    @property
    def gpaci(self):
        return self._data.get_byte(BTYPE.BYTE8, DUnilojas_C001_Report._GPACI)

    @gpaci.setter
    def gpaci(self, value):
        self._data.set_byte(BTYPE.BYTE8, DUnilojas_C001_Report._GPACI, value)

    @property
    def flow(self):
        return self._data.get_byte(BTYPE.BYTE32, DUnilojas_C001_Report._FLOW)

    @flow.setter
    def flow(self, value):
        self._data.set_byte(BTYPE.BYTE32, DUnilojas_C001_Report._FLOW, value)

    @property
    def temp1(self):
        return self._data.get_byte(BTYPE.BYTE32, DUnilojas_C001_Report._TEMP1, signed=True) / 100

    @temp1.setter
    def temp1(self, value):
        self._data.set_byte(BTYPE.BYTE32, DUnilojas_C001_Report._TEMP1, value * 100, signed=True)

    @property
    def temp2(self):
        return self._data.get_byte(BTYPE.BYTE32, DUnilojas_C001_Report._TEMP2, signed=True) / 100

    @temp2.setter
    def temp2(self, value):
        self._data.set_byte(BTYPE.BYTE32, DUnilojas_C001_Report._TEMP2, value * 100, signed=True)

    @property
    def temp3(self):
        return self._data.get_byte(BTYPE.BYTE32, DUnilojas_C001_Report._TEMP3, signed=True) / 100

    @temp3.setter
    def temp3(self, value):
        self._data.set_byte(BTYPE.BYTE32, DUnilojas_C001_Report._TEMP3, value * 100, signed=True)

    @property
    def ad(self):
        return self._data.get_byte(BTYPE.BYTE16, DUnilojas_C001_Report._AD)

    @ad.setter
    def ad(self, value):
        self._data.set_byte(BTYPE.BYTE16, DUnilojas_C001_Report._AD, value)

    @property
    def wdog_relay(self):
        return self._data.get_byte(BTYPE.BYTE8, DUnilojas_C001_Report._WDOG_RELAY)

    @wdog_relay.setter
    def wdog_relay(self, value):
        self._data.set_byte(BTYPE.BYTE8, DUnilojas_C001_Report._WDOG_RELAY, value)

    @property
    def wdog_time(self):
        return self._data.get_byte(BTYPE.BYTE32, DUnilojas_C001_Report._WDOG_TIME, signed=True) / 1000

    @wdog_time.setter
    def wdog_time(self, value):
        self._data.set_byte(BTYPE.BYTE32, DUnilojas_C001_Report._WDOG_TIME, value * 1000, signed=True)

    def set_rele(self, addr, value=True):
        self._data.put_bit(addr, value, DUnilojas_C001_Report._RELES)

    def get_rele(self, addr) -> bool:
        return self._data.is_bit(addr, DUnilojas_C001_Report._RELES)

    def dump(self) -> str:
        return self._data.dump()

    def exchange_data(self):
        return self._data

    def __str__(self):
        return 'Version:{0} Reles:{1:b} GPACI:{2:b} Flow:{3} Temp1:{4} Temp2:{5} Temp3:{6} AD:{7} WDog_Relay:{8} WDog_Time:{9}'.format(
            self.version,
            self.reles,
            self.gpaci,
            self.flow,
            self.temp1,
            self.temp2,
            self.temp3,
            self.ad,
            self.wdog_relay,
            self.wdog_time)

    def tolerance(self) -> {}:
        ret = {}
        ret['temp1'] = self.temp1
        ret['temp2'] = self.temp2
        ret['temp3'] = self.temp3
        ret['ad'] = self.ad
        return ret

    def range(self) -> {}:
        ret = {}
        ret['temp1'] = self.temp1
        ret['temp2'] = self.temp2
        ret['temp3'] = self.temp3
        return ret


# =============================================================================#
class DUnilojas_C001():
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

    def read_data_as_report(self) -> DUnilojas_C001_Report:
        DUnilojas_C001.threadLock.acquire()
        a = None
        try:
            read = i2c_msg.read(self._address, C001_READ_LENGHT)
            with SMBusWrapper(self._smbus_id) as smbus:
                smbus.i2c_rdwr(read)
            a = bytearray(read.buf[:read.len])
        finally:
            DUnilojas_C001.threadLock.release()
            return DUnilojas_C001_Report(a)

    def read_data(self) -> {}:
        DUnilojas_C001.threadLock.acquire()
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
            DUnilojas_C001.threadLock.release()
            return r

    def rele_state_word(self, state_word: int):
        DUnilojas_C001.threadLock.acquire()
        try:
            buffer = [0x64, state_word]
            write = i2c_msg.write(self._address, buffer)
            with SMBusWrapper(self._smbus_id) as smbus:
                smbus.i2c_rdwr(write)
            time.sleep(.3)
        finally:
            DUnilojas_C001.threadLock.release()

    def rele_state(self, rele_addr: int, state: bool):
        DUnilojas_C001.threadLock.acquire()
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
            DUnilojas_C001.threadLock.release()

    def rele_pulse(self, rele_addr: int, count: int, enlapse: int):
        DUnilojas_C001.threadLock.acquire()
        try:
            if rele_addr >= 0x65 and rele_addr <= 0x6b:
                e = struct.pack('H', enlapse)
                buffer = [rele_addr, 1, count, e[0], e[1]]
                write = i2c_msg.write(self._address, buffer)
                with SMBusWrapper(self._smbus_id) as smbus:
                    smbus.i2c_rdwr(write)
                time.sleep((count * (enlapse / 1000) * 3))
        finally:
            DUnilojas_C001.threadLock.release()

    def watch_dog(self, rele_addr: int = 0, enlapse: int = 0):
        DUnilojas_C001.threadLock.acquire()
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
            DUnilojas_C001.threadLock.release()


# =============================================================================#
class DUnilojas_C001_Thread(dslave.DSlave_Thread):
    def __init__(self, slave_num: int, resources: dict):
        super().__init__(slave_num, resources)
        self._stack = resources['stack']
        self.nameid = '{} [{} addr:{}]'.format(self.slave_name, ID['name'], 0)
        self._alarm_state = False
        # i2c_atmega
        self._i2c_atmega = CONFIG.slaves[self.slave_num].i2c_atmega
        # i2c_atmega_addr
        self.i2c_atmega_addr = CONFIG.slaves[self.slave_num].i2c_atmega_addr
        # device
        self._device = DUnilojas_C001(self._i2c_atmega, self.i2c_atmega_addr)
        # interval
        self._interval = CONFIG.slaves[self.slave_num].interval
        # periodic
        self._periodic = CONFIG.slaves[self.slave_num].periodic
        # watchdog_relay
        self._wdog_relay = CONFIG.slaves[self.slave_num].watchdog_relay
        # watchdog_time
        self._wdog_time = CONFIG.slaves[self.slave_num].watchdog_time
        # temp_tolerances
        self._temp_tolerances = {}
        # tolerance temp1
        self._temp_tolerances['temp1'] = CONFIG.slaves[self.slave_num].tolerance_temp1
        # tolerance temp2
        self._temp_tolerances['temp2'] = CONFIG.slaves[self.slave_num].tolerance_temp2
        # tolerance temp3
        self._temp_tolerances['temp3'] = CONFIG.slaves[self.slave_num].tolerance_temp3
        # tolerance ad
        self._temp_tolerances['ad'] = CONFIG.slaves[self.slave_num].tolerance_ad
        # temp_alarm_tolerances
        self._temp_alarm_ranges = {}
        # alarme temp1
        self._temp_alarm_ranges['temp1'] = CONFIG.slaves[self.slave_num].alarm_temp1
        # alarme temp2
        self._temp_alarm_ranges['temp2'] = CONFIG.slaves[self.slave_num].alarm_temp2
        # alarme temp3
        self._temp_alarm_ranges['temp3'] = CONFIG.slaves[self.slave_num].alarm_temp3
        # gpiac1
        gpiac1 = CONFIG.slaves[self.slave_num].gpi_ac1
        # gpiac2
        gpiac2 = CONFIG.slaves[self.slave_num].gpi_ac2
        # gpiac3
        gpiac3 = CONFIG.slaves[self.slave_num].gpi_ac3
        # report
        self._report = CONFIG.slaves[self.slave_num].report_flag
        # alarm
        self._alarm = CONFIG.slaves[self.slave_num].alarm_flag
        # event
        self._event = CONFIG.slaves[self.slave_num].event_flag
        # priority report
        self._priority_report = CONFIG.slaves[self.slave_num].priority_report
        # priority alarm
        self._priority_alarm = CONFIG.slaves[self.slave_num].priority_alarm
        # priority event
        self._priority_event = CONFIG.slaves[self.slave_num].priority_event
        # pinlock
        self._pinlock = CONFIG.slaves[self.slave_num].pinlock
        # pinlockled
        self._pinlockled = CONFIG.slaves[self.slave_num].pinlockled
        # pinbattery
        self._pinbattery = CONFIG.slaves[self.slave_num].pinbattery
        # pinresetatmega
        self._pinresetatmega = CONFIG.slaves[self.slave_num].pinresetatmega
        # create tolerance class
        self._tolerance = dslave.DTolerance(self._temp_tolerances)
        # create range class
        self._range = dslave.DRange(self._temp_alarm_ranges)
        # local logger
        self.logger = logging.getLogger(__name__)

    def run(self):
        try:
            self.logger.info('Iniciando job "{}" {}'.format(self.local, self.nameid))
            report_time = time.time() + self._periodic
            starting = True
            previous_data = None
            # inicia GPIOs
            if CONFIG.slaves[self.slave_num].lockenable:
                GPIO.setup(self._pinlock, GPIO.GPIO.IN)
                GPIO.setup(self._pinlockled, GPIO.GPIO.OUT)
                lock_enable = True
            else:
                lock_enable = False
            if CONFIG.slaves[self.slave_num].batterymonitor:
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
                        previous_data = DUnilojas_C001_Report(actual_data.exchange_data())

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
            self.logger.fatal('Thread DUnilojas_C001_Thread fail:{}'.format(str(err)))

    def lock_monitoring(self):
        if GPIO.input(CONFIG.slaves[0].pinlock):
            STATUS.clear_state(CONST.STATE_LOCK)
            GPIO.output(CONFIG.slaves[0].pinlockled, GPIO.GPIO.LOW)
        else:
            STATUS.set_state(CONST.STATE_LOCK)
            GPIO.output(CONFIG.slaves[0].pinlockled, GPIO.GPIO.HIGH)

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
        pin = CONFIG.slaves[0].pinbattery
        t = time.time() + 1
        a = GPIO.input(pin)
        while t > time.time():
            b = GPIO.input(pin)
            if a != b:
                return GPIO.GPIO.FALLING
        return a

    def check_event(self, previous: DUnilojas_C001_Report, actual: DUnilojas_C001_Report) -> bool:
        return self._tolerance.check(previous.tolerance(), actual.tolerance())


# =============================================================================#
class DUnilojas_C001_Process(listen.DListenProcess):
    TIME_CONN_I2C = .1

    def __init__(self, conn: socket, addr, header: dsocket.DSocketHeaderBasic, data: common.RData, resources: dict):
        super().__init__(conn, addr, header, data, resources)
        # self._netmeter = resources['netmeter']
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
            i2c = DUnilojas_C001(CONFIG.slaves[header.slave].i2c_atmega,
                                 CONFIG.slaves[header.slave].i2c_atmega_addr)
            # Register/Unregister
            if header.modelid == 0 and header.cmdtype == 0 and header.size == 1:
                if self._data[0] == 1:  # Register
                    STATUS.put_state(CONST.STATE_REGISTER, True)
                    CONFIG.main.register = str(STATUS.is_state(CONST.STATE_REGISTER))
                    CONFIG.write()
                    listen.replay_cmd_now_file(self._conn, CONST.DWEB_NAME + '.ini')
                elif self._data[0] == 0:  # Unregister
                    STATUS.status = 0
                    # DSTATUS.put_state(CONST.STATE_REGISTER, False)
                    CONFIG.main.register = str(STATUS.is_state(CONST.STATE_REGISTER))
                    CONFIG.write()
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
            # NetMeter
            # elif header.modelid == 0 and header.cmdtype == 3 and header.size == 1 and CONFIG.netmeter.netmeter:
            #     if self._data[0] == 0:  # Get netstat
            #         r = json.dumps(self._netmeter.netstats._asdict())
            #         d = common.RData(r.encode())
            #         listen.replay_cmd_now(self._conn, d)
            #     elif self._data[0] == 1:  # Restart counter stat
            #         self._netmeter.reset_counter()
            #         listen.replay_ok(self._conn)
            #     else:
            #         listen.replay_err(CONST.RETURNCODE_CMD_INVALID)
            # Read
            elif header.modelid == 3 and header.cmdtype == 0 and header.size == 0:
                r = i2c.read_data_as_report()
                listen.replay_cmd_now(self._conn, r.exchange_data())
            # ReleStateWord
            elif header.modelid == 3 and header.cmdtype == 1 and header.size == 1:
                i2c.rele_state_word(self._data[0])
                time.sleep(DUnilojas_C001_Process.TIME_CONN_I2C)
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
                time.sleep(DUnilojas_C001_Process.TIME_CONN_I2C)
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
                time.sleep(DUnilojas_C001_Process.TIME_CONN_I2C)
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

    c001 = DUnilojas_C001(1, 0x08)  # 7 bit address (will be left shifted to add the read write bit)

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
