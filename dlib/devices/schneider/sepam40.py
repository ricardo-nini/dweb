#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
import logging
import socket
import rlib.common as common
from rlib.common import CONST
import rlib.rmodbus as rmodbus
from rlib.rmodbus import RByteType as BTYPE
import dlib.dsocket as dsocket
import dlib._dlisten as listen
from dlib.dcommon import GLOBAL, is_demanda_time, DTolerance
from dlib.dstatus import STATUS
import dlib.devices.dbase as dbase
from dlib.dconfig import CONFIG, DConfig_Slave_Schneider_SEPAM40

ID = {'modelid': 2,
      'name': 'Schneider SEPAM40',
      'fmt_alarm': 1,
      'fmt_meter': 0,
      'fmt_meter_alarm': 2}


# =============================================================================#
class Device_Alarm(dbase.DAlarm_Rele):
    _SIZE = 4
    _flag50_51 = 24
    _flag50N_51N = 25
    _flag27 = 26
    _flag59 = 27
    _flag81 = 28
    _flag67 = 29
    _flag67N = 30
    _flag47_1 = 31
    _flag32P = 16
    _flag50BF = 17
    _flag79_1 = 18
    _flag79_2 = 19
    _flag79_3 = 20
    _flag79_4 = 21
    _flag79_5 = 22
    _flagDJ = 23
    _flagBanderolaAtiva = 8

    def __init__(self, data=None, offset=0):
        super().__init__(Device_Alarm._SIZE, data, offset)

    @property
    def flag50_51(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag50_51, self._offset)

    @flag50_51.setter
    def flag50_51(self, flag):
        self._data.put_bit(Device_Alarm._flag50_51, flag, self._offset)

    @property
    def flag50N_51N(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag50N_51N, self._offset)

    @flag50N_51N.setter
    def flag50N_51N(self, flag):
        self._data.put_bit(Device_Alarm._flag50N_51N, flag, self._offset)

    @property
    def flag27(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag27, self._offset)

    @flag27.setter
    def flag27(self, flag):
        self._data.put_bit(Device_Alarm._flag27, flag, self._offset)

    @property
    def flag59(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag59, self._offset)

    @flag59.setter
    def flag59(self, flag):
        self._data.put_bit(Device_Alarm._flag59, flag, self._offset)

    @property
    def flag81(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag81, self._offset)

    @flag81.setter
    def flag81(self, flag):
        self._data.put_bit(Device_Alarm._flag81, flag, self._offset)

    @property
    def flag67(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag67, self._offset)

    @flag67.setter
    def flag67(self, flag):
        self._data.put_bit(Device_Alarm._flag67, flag, self._offset)

    @property
    def flag67N(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag67N, self._offset)

    @flag67N.setter
    def flag67N(self, flag):
        self._data.put_bit(Device_Alarm._flag67N, flag, self._offset)

    @property
    def flag47_1(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag47_1, self._offset)

    @flag47_1.setter
    def flag47_1(self, flag):
        self._data.put_bit(Device_Alarm._flag47_1, flag, self._offset)

    @property
    def flag32P(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag32P, self._offset)

    @flag32P.setter
    def flag32P(self, flag):
        self._data.put_bit(Device_Alarm._flag32P, flag, self._offset)

    @property
    def flag50BF(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag50BF, self._offset)

    @flag50BF.setter
    def flag50BF(self, flag):
        self._data.put_bit(Device_Alarm._flag50BF, flag, self._offset)

    @property
    def flag79_1(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag79_1, self._offset)

    @flag79_1.setter
    def flag79_1(self, flag):
        self._data.put_bit(Device_Alarm._flag79_1, flag, self._offset)

    @property
    def flag79_2(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag79_2, self._offset)

    @flag79_2.setter
    def flag79_2(self, flag):
        self._data.put_bit(Device_Alarm._flag79_2, flag, self._offset)

    @property
    def flag79_3(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag79_3, self._offset)

    @flag79_3.setter
    def flag79_3(self, flag):
        self._data.put_bit(Device_Alarm._flag79_3, flag, self._offset)

    @property
    def flag79_4(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag79_4, self._offset)

    @flag79_4.setter
    def flag79_4(self, flag):
        self._data.put_bit(Device_Alarm._flag79_4, flag, self._offset)

    @property
    def flag79_5(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag79_5, self._offset)

    @flag79_5.setter
    def flag79_5(self, flag):
        self._data.put_bit(Device_Alarm._flag79_5, flag, self._offset)

    @property
    def flagDJ(self) -> bool:
        return self._data.is_bit(Device_Alarm._flagDJ, self._offset)

    @flagDJ.setter
    def flagDJ(self, flag):
        self._data.put_bit(Device_Alarm._flagDJ, flag, self._offset)

    @property
    def flagBanderolaAtiva(self) -> bool:
        return self._data.is_bit(Device_Alarm._flagBanderolaAtiva, self._offset)

    @flagBanderolaAtiva.setter
    def flagBanderolaAtiva(self, flag):
        self._data.put_bit(Device_Alarm._flagBanderolaAtiva, flag, self._offset)

    def __str__(self):
        return 'flag50_51:{} flag50N_51N:{} flag27:{} flag59:{} ' \
               'flag81:{} flag67:{} flag67N:{} ' \
               'flag47_1:{} flag32P:{} flag50BF:{} ' \
               'flag79_1:{} flag79_2:{} flag79_3:{} flag79_4:{} flag79_5:{} ' \
               'flagDJ:{} flagBanderolaAtiva:{}'.format(self.flag50_51, self.flag50N_51N, self.flag27, self.flag59,
                                                        self.flag81, self.flag67, self.flag67N,
                                                        self.flag47_1, self.flag32P, self.flag50BF,
                                                        self.flag79_1, self.flag79_2, self.flag79_3, self.flag79_4,
                                                        self.flag79_5,
                                                        self.flagDJ, self.flagBanderolaAtiva)


# =============================================================================#
class Device():
    def __init__(self, slave: int, modbus: rmodbus.RModbusComm):
        self.slave = slave
        self._modbus = modbus
        self.logger = logging.getLogger(__name__)

    def get_meter(self) -> dbase.DReport_Rele:
        ret = dbase.DReport_Rele()

        send = []
        # Frequencia, PotenciaAtiva, PotenciaReativa, PotenciaAparente, NC, NC, FatorPotencia (7)
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 330, 7))
        # TensaoVab, TensaoVbc, TensaoVca, TensaoA, TensaoB, TensaoC (6)
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 321, 6))
        # CorrenteA, CorrenteB, CorrenteC (3)
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 310, 3))

        recv = []
        for x in range(0, len(send)):
            recv.append(self._modbus.exchange(send[x]))

        r = rmodbus.RModbusReadResponse(recv[0].get_exchange_data())
        ret.frequency = r.get_byte(BTYPE.BYTE16, 0) / 100
        ret.factor = (r.get_byte(BTYPE.BYTE16, 12, signed=True) * -1) / 100

        r = rmodbus.RModbusReadResponse(recv[1].get_exchange_data())
        ret.voltA = r.get_byte(BTYPE.BYTE16, 6) * 10
        ret.voltB = r.get_byte(BTYPE.BYTE16, 8) * 10
        ret.voltC = r.get_byte(BTYPE.BYTE16, 10) * 10

        r = rmodbus.RModbusReadResponse(recv[2].get_exchange_data())
        ret.currenteA = r.get_byte(BTYPE.BYTE16, 0)
        ret.currenteB = r.get_byte(BTYPE.BYTE16, 2)
        ret.currenteC = r.get_byte(BTYPE.BYTE16, 4)
        ret.currenteN = 0

        return ret

    def get_alarm(self) -> Device_Alarm:
        ret = Device_Alarm()

        send = []
        # flag50_51, flag50N_51N
        send.append(rmodbus.RModbusReadCoilStatus.create(self.slave, 4112, 8))
        # flag27, flag59, flag81
        send.append(rmodbus.RModbusReadCoilStatus.create(self.slave, 4130, 13))
        # flag67, flag67N, flag47_1, flag32P, flag50BF
        send.append(rmodbus.RModbusReadCoilStatus.create(self.slave, 4144, 7))
        # flag79_1, flag79_2, flag79_3, flag79_4, flagDJ, flagBanderolaAtiva
        send.append(rmodbus.RModbusReadCoilStatus.create(self.slave, 4208, 16))
        # flag79_5
        send.append(rmodbus.RModbusReadCoilStatus.create(self.slave, 4236, 1))

        recv = []
        for x in range(0, len(send)):
            recv.append(self._modbus.exchange(send[x]))

        r = rmodbus.RModbusReadResponse(recv[0].get_exchange_data())
        ret.flag50_51 = r.is_bit(0) or r.is_bit(1) or r.is_bit(2) or r.is_bit(3)
        ret.flag50N_51N = r.is_bit(4) or r.is_bit(5) or r.is_bit(6) or r.is_bit(7)

        r = rmodbus.RModbusReadResponse(recv[1].get_exchange_data())
        ret.flag27 = r.is_bit(0) or r.is_bit(1)
        ret.flag59 = r.is_bit(3) or r.is_bit(4) or r.is_bit(5) or r.is_bit(6)
        ret.flag81 = r.is_bit(7) or r.is_bit(8) or r.is_bit(9) or r.is_bit(10) or r.is_bit(11) or r.is_bit(12)

        r = rmodbus.RModbusReadResponse(recv[2].get_exchange_data())
        ret.flag67 = r.is_bit(0) or r.is_bit(1)
        ret.flag67N = r.is_bit(2) or r.is_bit(3)
        ret.flag47_1 = r.is_bit(4)
        ret.flag32P = r.is_bit(5)
        ret.flag50BF = r.is_bit(6)

        r = rmodbus.RModbusReadResponse(recv[3].get_exchange_data())
        ret.flag79_1 = r.is_bit(0)
        ret.flag79_2 = r.is_bit(1)
        ret.flag79_3 = r.is_bit(2)
        ret.flag79_4 = r.is_bit(3)
        ret.flagDJ = r.is_bit(8)
        ret.flagBanderolaAtiva = r.is_bit(7)

        r = rmodbus.RModbusReadResponse(recv[4].get_exchange_data())
        ret.flag79_5 = r.is_bit(0)

        return ret


# =============================================================================#
class Device_Job(dbase.DSlave_Job):
    def __init__(self, slave_num: int, resources: dict):
        super().__init__(slave_num, resources)
        self._config = DConfig_Slave_Schneider_SEPAM40(CONFIG.rconfig, slave_num)
        self._stack = resources['stack']
        self._modbus = resources['modbus']
        GLOBAL.modbus[str(slave_num)] = False
        self._device = Device(self._config.modbus_slave, self._modbus)
        self.nameid = '{} [{} addr:{}]'.format(self.slave_name, ID['name'], self._config.modbus_slave)
        # interval
        self._interval = self._config.interval
        # periodic
        self._periodic = self._config.periodic
        # meter_tolerances
        self._meter_tolerances = {}
        # tolerance current
        self._meter_tolerances['currentA'] = self._config.tolerance_current
        self._meter_tolerances['currentB'] = self._config.tolerance_current
        self._meter_tolerances['currentC'] = self._config.tolerance_current
        # tolerance neutro
        self._meter_tolerances['currentN'] = self._config.tolerance_neutro
        # tolerance volt
        self._meter_tolerances['voltA'] = self._config.tolerance_voltage
        self._meter_tolerances['voltB'] = self._config.tolerance_voltage
        self._meter_tolerances['voltC'] = self._config.tolerance_voltage
        # tolerance factor
        self._meter_tolerances['factor'] = self._config.tolerance_factor
        # tolerance frequency
        self._meter_tolerances['frequency'] = self._config.tolerance_frequency
        # meter_alarms
        self._meter_alarms = {}
        # alarm current
        self._meter_alarms['currentA'] = self._config.alarm_current
        self._meter_alarms['currentB'] = self._config.alarm_current
        self._meter_alarms['currentC'] = self._config.alarm_current
        # alarm neutro
        self._meter_alarms['currentN'] = self._config.alarm_neutro
        # alarm volt
        self._meter_alarms['voltA'] = self._config.alarm_voltage
        self._meter_alarms['voltB'] = self._config.alarm_voltage
        self._meter_alarms['voltC'] = self._config.alarm_voltage
        # alarm factor
        self._meter_alarms['factor'] = self._config.alarm_factor
        # alarm frequency
        self._meter_alarms['frequency'] = self._config.alarm_frequency
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
        # create tolerance class
        self._meter_tolerance = DTolerance(self._meter_tolerances)
        # create alarm class
        self._meter_alarm = DTolerance(self._meter_alarms)
        # local logger
        self.logger = logging.getLogger(__name__)

    def run(self):
        try:
            self.logger.info('Iniciando job "{}" {}'.format(self.local, self.nameid))
            report_time = time.time() + self._periodic
            starting = True
            previous_meter = dbase.DReport_Rele()
            previous_alarm = Device_Alarm()
            demanda_lock = False
            while not self.stopped():
                try:
                    actual_meter = None
                    if self._alarm:
                        # get alarm
                        actual_alarm = self._device.get_alarm()
                        # self.logger.debug('{} [Alarm] {}'.format(self.nameid, actual_alarm))
                        if actual_alarm is not None:
                            # check if trigger alarm
                            if (starting and not actual_alarm.is_normal()) or (
                                        not starting and actual_alarm.exchange_data() != previous_alarm.exchange_data()):
                                # create alarm header
                                h = dsocket.DSocketHeader_Alarm.create(STATUS.id,
                                                                       int(time.time()),
                                                                       self.slave_num,
                                                                       ID['modelid'],
                                                                       ID['fmt_alarm'])
                                # send alarm
                                self._stack.put(self._priority_alarm, time.time(), h, actual_alarm.exchange_data())
                            # update previous data ...
                            previous_alarm = Device_Alarm(actual_alarm.exchange_data())

                        # check meter_alarm
                        # get actual meter
                        actual_meter = self._device.get_meter()
                        # check if trigger an event (not applicable on start)
                        if not starting and not self.check_alarm(previous_meter, actual_meter):
                            # create report header
                            h = dsocket.DSocketHeader_Alarm.create(STATUS.id,
                                                                   int(time.time()),
                                                                   self.slave_num,
                                                                   ID['modelid'],
                                                                   ID['fmt_meter_alarm'])
                            # send meter_alarm
                            self._stack.put(self._priority_event, time.time(), h, actual_meter.exchange_data())

                    if self._report:
                        # get actual meter
                        if not self._alarm:
                            actual_meter = self._device.get_meter()
                        # self.logger.debug('{} [Meter] {}'.format(self.nameid, actual_meter))
                        if actual_meter is not None:
                            # check if send report
                            demanda, demanda_lock = is_demanda_time(demanda_lock)
                            if (self._periodic == 0 and demanda) or (self._periodic != 0 and report_time < time.time()):
                                # clear report time counter
                                report_time = time.time() + self._periodic
                                # create report header
                                h = dsocket.DSocketHeader_Report.create(STATUS.id,
                                                                        int(time.time()),
                                                                        self.slave_num,
                                                                        ID['modelid'],
                                                                        ID['fmt_meter'])
                                # send report
                                self._stack.put(self._priority_report, time.time(), h, actual_meter.exchange_data())
                            if self._event:
                                # check if trigger an event (not applicable on start)
                                if not starting and not self.check_event(previous_meter, actual_meter):
                                    # create report header
                                    h = dsocket.DSocketHeader_Event.create(STATUS.id,
                                                                           int(time.time()),
                                                                           self.slave_num,
                                                                           ID['modelid'],
                                                                           ID['fmt_meter'])
                                    # send event
                                    self._stack.put(self._priority_event, time.time(), h, actual_meter.exchange_data())

                            # update previous data ...
                            previous_meter = dbase.DReport_Rele(actual_meter.exchange_data())

                    # flag modbus
                    if not GLOBAL.modbus[str(self.slave_num)]:
                        GLOBAL.modbus[str(self.slave_num)] = True

                except Exception as err:
                    self.logger.debug(str(err))
                    GLOBAL.modbus[str(self.slave_num)] = False
                finally:
                    # clear start flag
                    if starting:
                        starting = False
                    # wait interval
                    time.sleep(self._interval)

            self.logger.info('Terminado job "{}" {}'.format(self.local, self.nameid))

        except Exception as err:
            print(type(err))
            self.logger.fatal('Thread Device_Job fail:{}'.format(str(err)))

    def check_event(self, previous: dbase.DReport_Rele, actual: dbase.DReport_Rele) -> bool:
        return self._meter_tolerance.check(previous.tolerance(), actual.tolerance())

    def check_alarm(self, previous: dbase.DReport_Rele, actual: dbase.DReport_Rele) -> bool:
        return self._meter_alarm.check(previous.tolerance(), actual.tolerance())


# =============================================================================#
class Device_Process(dbase.DSlave_Process):
    def __init__(self, conn: socket, addr, header: dsocket.DSocketHeaderBasic, data: common.RData, resources: dict):
        super().__init__(conn, addr, header, data)
        self._modbus = resources['modbus']
        self.logger = logging.getLogger(__name__)

    def proc_cmd(self):
        try:
            header = dsocket.DSocketHeader_Cmd(self.header.exchange_data())
        except Exception as err:
            self.logger.debug('Erro inesperado {}'.format(str(err)))

    def proc_cmd_now(self):
        try:
            header = dsocket.DSocketHeader_CmdNow(self.header.exchange_data())
            if header.modelid != 2:
                raise ValueError('Invalid device on argument.')
            config = DConfig_Slave_Schneider_SEPAM40(CONFIG.rconfig, header.slave)
            rele = Device(config.modbus_slave, self._modbus)
            # Meter
            if header.cmdtype == 0:
                r = rele.get_meter()
                listen.replay_cmd_now(self._conn, r.exchange_data())
            # Flags
            elif header.cmdtype == 1:
                r = rele.get_alarm()
                listen.replay_cmd_now(self._conn, r.exchange_data())
            else:
                listen.replay_err(self._conn, CONST.RETURNCODE_CMD_INVALID)
        except rmodbus.RModbusError as err:
            listen.replay_err(self._conn, err.code + 2000)
        except Exception as err:
            self.logger.debug('Erro inesperado {}'.format(str(err)))
            listen.replay_err(self._conn, CONST.RETURNCODE_UNKNOWN)
