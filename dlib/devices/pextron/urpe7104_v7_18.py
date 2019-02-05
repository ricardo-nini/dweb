#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
import json
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
from dlib.dconfig import CONFIG, DConfig_Slave_Pextron_URPE7104_v7_18

ID = {'modelid': 4,
      'name': 'Pextron URPE7104_V7_18',
      'fmt_alarm': 2,
      'fmt_meter': 1,
      'fmt_meter_alarm': 1}


# =============================================================================#
class Device_Alarm(dbase.DAlarm_Rele):
    _SIZE = 4
    _flag50A = 24
    _flag50B = 25
    _flag50C = 26
    _flag50N = 27
    _flag51A_TP = 28  # Temporizado
    _flag51B_TP = 29  # Temporizado
    _flag51C_TP = 30  # Temporizado
    _flag51N_TP = 31  # Temporizado

    def __init__(self, data=None, offset=0):
        super().__init__(Device_Alarm._SIZE, data, offset)

    @property
    def flag50A(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag50A, self._offset)

    @flag50A.setter
    def flag50A(self, flag):
        self._data.put_bit(Device_Alarm._flag50A, flag, self._offset)

    @property
    def flag50B(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag50B, self._offset)

    @flag50B.setter
    def flag50B(self, flag):
        self._data.put_bit(Device_Alarm._flag50B, flag, self._offset)

    @property
    def flag50C(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag50C, self._offset)

    @flag50C.setter
    def flag50C(self, flag):
        self._data.put_bit(Device_Alarm._flag50C, flag, self._offset)

    @property
    def flag50N(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag50N, self._offset)

    @flag50N.setter
    def flag50N(self, flag):
        self._data.put_bit(Device_Alarm._flag50N, flag, self._offset)

    @property
    def flag51A_TP(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag51A_TP, self._offset)

    @flag51A_TP.setter
    def flag51A_TP(self, flag):
        self._data.put_bit(Device_Alarm._flag51A_TP, flag, self._offset)

    @property
    def flag51B_TP(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag51B_TP, self._offset)

    @flag51B_TP.setter
    def flag51B_TP(self, flag):
        self._data.put_bit(Device_Alarm._flag51B_TP, flag, self._offset)

    @property
    def flag51C_TP(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag51C_TP, self._offset)

    @flag51C_TP.setter
    def flag51C_TP(self, flag):
        self._data.put_bit(Device_Alarm._flag51C_TP, flag, self._offset)

    @property
    def flag51N_TP(self) -> bool:
        return self._data.is_bit(Device_Alarm._flag51N_TP, self._offset)

    @flag51N_TP.setter
    def flag51N_TP(self, flag):
        self._data.put_bit(Device_Alarm._flag51N_TP, flag, self._offset)

    def __str__(self):
        return 'flag50A:{} flag50B:{} flag50C:{} ' \
               'flag51A_TP:{} flag51B_TP:{} flag51C_TP:{} ' \
               'flag50N:{} flag51N_TP:{}'.format(self.flag50A, self.flag50B, self.flag50C,
                                                 self.flag51A_TP, self.flag51B_TP, self.flag51C_TP,
                                                 self.flag50N, self.flag51N_TP)


# =============================================================================#
class Device():
    def __init__(self, slave: int, dip2: bool, modbus: rmodbus.RModbusComm):
        self.slave = slave
        self._dip2 = dip2
        self._modbus = modbus
        self.logger = logging.getLogger(__name__)

    def get_meter(self) -> dbase.DReport_ReleCorrente:
        ret = dbase.DReport_ReleCorrente()

        send = []
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 12, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 132, 4))

        recv = []
        for x in range(0, len(send)):
            recv.append(self._modbus.exchange(send[x]))

        r = rmodbus.RModbusReadResponse(recv[0].get_exchange_data())
        rtc = self._calc_rtc(r.get_byte(BTYPE.BYTE16, 0))

        r = rmodbus.RModbusReadResponse(recv[1].get_exchange_data())
        ret.currenteA = int((r.get_byte(BTYPE.BYTE16, 0) * (1 / 256) * rtc))
        ret.currenteB = int((r.get_byte(BTYPE.BYTE16, 2) * (1 / 256) * rtc))
        ret.currenteC = int((r.get_byte(BTYPE.BYTE16, 4) * (1 / 256) * rtc))
        ret.currenteN = int((r.get_byte(BTYPE.BYTE16, 6) * (1 / 256) * rtc))

        return ret

    def get_alarm(self) -> Device_Alarm:
        ret = Device_Alarm()

        send = rmodbus.RModbusReadCoilStatus.create(self.slave, 20, 8)
        recv = self._modbus.exchange(send)
        r = rmodbus.RModbusReadResponse(recv.get_exchange_data())
        ret.flag51A_TP = r.is_bit(0)
        ret.flag51B_TP = r.is_bit(1)
        ret.flag51C_TP = r.is_bit(2)
        ret.flag51N_TP = r.is_bit(3)
        ret.flag50A = r.is_bit(4)
        ret.flag50B = r.is_bit(5)
        ret.flag50C = r.is_bit(6)
        ret.flag50N = r.is_bit(7)

        return ret

    def rearme_remoto_bandeirolas(self):
        send = rmodbus.RModbusForceSingleCoil.create(self.slave, 48, True)
        recv = self._modbus.exchange(send)
        if recv.response_type() != rmodbus.RModbusResponceType.FORCE_SINGLE_COIL_RESPONSE:
            raise rmodbus.RModbusError(rmodbus.RMODBUS.EXCEPTION_INVALID_FUNCTION)

    def reset_registros(self):
        send = rmodbus.RModbusForceSingleCoil.create(self.slave, 49, True)
        recv = self._modbus.exchange(send)
        if recv.response_type() != rmodbus.RModbusResponceType.FORCE_SINGLE_COIL_RESPONSE:
            raise rmodbus.RModbusError(rmodbus.RMODBUS.EXCEPTION_INVALID_FUNCTION)

    def get_data(self) -> str:
        coils = self._get_coils()
        registers = self._get_registers()
        ret = {}
        ret.update(coils)
        ret.update(registers)
        return json.dumps(ret)

    def set_data(self, data2set: str):
        regs = json.loads(data2set, parse_int=int, parse_float=float)
        if isinstance(regs, dict):
            if 'corrente_intantaneo_fase' in regs and isinstance(regs['corrente_intantaneo_fase'], int) and (
                            regs['corrente_intantaneo_fase'] >= 1 and regs['rtc'] <= 2500):
                self._set_any_register(regs['corrente_intantaneo_fase'], 0)

            if 'rtp' in regs and isinstance(regs['rtp'], int) and (regs['rtp'] >= 1 and regs['rtp'] <= 4300):
                self._set_any_register(regs['rtp'], 13)
            if 'tempo_de_registro_de_perfil_de_carga' in regs and \
                    isinstance(regs['tempo_de_registro_de_perfil_de_carga'], int) and \
                    (regs['tempo_de_registro_de_perfil_de_carga'] >= 1 and
                             regs['tempo_de_registro_de_perfil_de_carga'] <= 241):
                self._set_any_register(regs['tempo_de_registro_de_perfil_de_carga'], 26)
            if 'habilita_registro_de_oscilografia' in regs and isinstance(regs['habilita_registro_de_oscilografia'],
                                                                          bool):
                if regs['habilita_registro_de_oscilografia']:
                    r = 256
                else:
                    r = 0
                self._set_any_register(r, 27)
            if 'ano' in regs and isinstance(regs['ano'], int) and (regs['ano'] >= 0 and regs['ano'] <= 99):
                r = common.dec2BCD(regs['ano']) << 8
                self._set_any_register(r, 28)
            if 'mes' in regs and isinstance(regs['mes'], int) and (regs['mes'] >= 1 and regs['mes'] <= 12):
                r = common.dec2BCD(regs['mes']) << 8
                self._set_any_register(r, 29)
            if 'dia' in regs and isinstance(regs['dia'], int) and (regs['dia'] >= 1 and regs['dia'] <= 31):
                r = common.dec2BCD(regs['dia']) << 8
                self._set_any_register(r, 30)
            if 'hora' in regs and isinstance(regs['hora'], int) and (regs['hora'] >= 0 and regs['hora'] <= 23):
                r = common.dec2BCD(regs['hora']) << 8
                self._set_any_register(r, 31)
            if 'minutos' in regs and isinstance(regs['minutos'], int) and (
                            regs['minutos'] >= 0 and regs['minutos'] <= 59):
                r = common.dec2BCD(regs['minutos']) << 8
                self._set_any_register(r, 32)
            if 'segundos' in regs and isinstance(regs['segundos'], int) \
                    and (regs['segundos'] >= 0 and regs['segundos'] <= 59):
                r = common.dec2BCD(regs['segundos']) << 8
                self._set_any_register(r, 33)
            if 'habilita_programacao' in regs and isinstance(regs['habilita_programacao'], bool):
                if regs['habilita_programacao']:
                    r = 256
                else:
                    r = 0
                self._set_any_register(r, 44)
            if 'oscilografia_de_leitura' in regs and isinstance(regs['oscilografia_de_leitura'], int) and \
                    (regs['oscilografia_de_leitura'] >= 0 and regs['oscilografia_de_leitura'] <= 7):
                self._set_any_register(regs['oscilografia_de_leitura'], 66)

    def _get_coils(self) -> {}:
        send = []
        send.append(rmodbus.RModbusReadCoilStatus.create(self.slave, 32, 8))
        recv = []
        for x in range(0, len(send)):
            recv.append(self._modbus.exchange(send[x]))
        r = rmodbus.RModbusReadResponse(recv[0].get_exchange_data())
        saida_ipn = r.is_bit(0)
        saida_ipf = r.is_bit(2)
        saida_51_51N_15_18 = r.is_bit(4)
        saida_51_51N_25_28 = r.is_bit(5)
        saida_50_50N_11_14 = r.is_bit(6)
        saida_50_50N_21_24 = r.is_bit(7)
        return dict(saida_ipn=saida_ipn,
                    saida_ipf=saida_ipf,
                    saida_51_51N_15_18=saida_51_51N_15_18,
                    saida_51_51N_25_28=saida_51_51N_25_28,
                    saida_50_50N_11_14=saida_50_50N_11_14,
                    saida_50_50N_21_24=saida_50_50N_21_24)

    def _get_registers(self) -> {}:
        send = []
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 0, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 1, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 2, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 3, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 4, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 5, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 6, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 7, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 8, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 9, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 10, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 11, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 12, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 128, 4))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 136, 2))
        recv = []
        for x in range(0, len(send)):
            recv.append(self._modbus.exchange(send[x]))

        r = rmodbus.RModbusReadResponse(recv[12].get_exchange_data())
        rtc = self._calc_rtc(r.get_byte(BTYPE.BYTE16, 0))

        r = rmodbus.RModbusReadResponse(recv[0].get_exchange_data())
        corrente_intantaneo_fase = r.get_byte(BTYPE.BYTE16, 0, bigendian=True) * (1 / 256)
        r = rmodbus.RModbusReadResponse(recv[1].get_exchange_data())
        corrente_intantaneo_neutro = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True) * (1 / 256)
        r = rmodbus.RModbusReadResponse(recv[2].get_exchange_data())
        corrente_partida_temp_fase = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True) * (1 / 256)
        r = rmodbus.RModbusReadResponse(recv[3].get_exchange_data())
        corrente_partida_temp_neutro = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True) * (1 / 256)
        r = rmodbus.RModbusReadResponse(recv[4].get_exchange_data())
        curva_temp_fase = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True)
        r = rmodbus.RModbusReadResponse(recv[5].get_exchange_data())
        curva_temp_neutro = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True)
        r = rmodbus.RModbusReadResponse(recv[6].get_exchange_data())
        const_dt_fase = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True) * (1 / 256)
        r = rmodbus.RModbusReadResponse(recv[7].get_exchange_data())
        const_dt_neutro = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True) * (1 / 256)
        r = rmodbus.RModbusReadResponse(recv[8].get_exchange_data())
        corrente_partida_td_fase = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True) * (1 / 256)
        r = rmodbus.RModbusReadResponse(recv[9].get_exchange_data())
        corrente_partida_td_neutro = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True) * (1 / 256)
        r = rmodbus.RModbusReadResponse(recv[10].get_exchange_data())
        td_fase = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True) * (1 / 256)
        r = rmodbus.RModbusReadResponse(recv[11].get_exchange_data())
        td_neutro = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=True) * (1 / 256)

        r = rmodbus.RModbusReadResponse(recv[13].get_exchange_data())
        reg_corrente_max_fase_a = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=False) * (1 / 256) * rtc
        reg_corrente_max_fase_b = r.get_byte(rmodbus.RByteType.BYTE16, 1, bigendian=False) * (1 / 256) * rtc
        reg_corrente_max_fase_c = r.get_byte(rmodbus.RByteType.BYTE16, 2, bigendian=False) * (1 / 256) * rtc
        reg_corrente_max_neutro = r.get_byte(rmodbus.RByteType.BYTE16, 3, bigendian=False) * (1 / 256) * rtc

        r = rmodbus.RModbusReadResponse(recv[14].get_exchange_data())
        tipo_do_rele = r.get_byte(rmodbus.RByteType.BYTE16, 0, bigendian=False)
        versao_do_rele = r.get_byte(rmodbus.RByteType.BYTE16, 1, bigendian=False)

        return dict(corrente_intantaneo_fase=corrente_intantaneo_fase,
                    corrente_intantaneo_neutro=corrente_intantaneo_neutro,
                    corrente_partida_temp_fase=corrente_partida_temp_fase,
                    corrente_partida_temp_neutro=corrente_partida_temp_neutro,
                    curva_temp_fase=curva_temp_fase,
                    curva_temp_neutro=curva_temp_neutro,
                    const_dt_fase=const_dt_fase,
                    const_dt_neutro=const_dt_neutro,
                    corrente_partida_td_fase=corrente_partida_td_fase,
                    corrente_partida_td_neutro=corrente_partida_td_neutro,
                    td_fase=td_fase,
                    td_neutro=td_neutro,
                    reg_corrente_max_fase_a=reg_corrente_max_fase_a,
                    reg_corrente_max_fase_b=reg_corrente_max_fase_b,
                    reg_corrente_max_fase_c=reg_corrente_max_fase_c,
                    reg_corrente_max_neutro=reg_corrente_max_neutro,
                    tipo_do_rele=tipo_do_rele,
                    versao_do_rele=versao_do_rele,
                    rtc=rtc)

    def _set_any_register(self, value, addr):
        send = rmodbus.RModbusPresetSingleRegister.create(self.slave, addr, value)
        recv = self._modbus.exchange(send)
        if recv.response_type() == rmodbus.RModbusResponceType.EXCEPTION_RESPONSE:
            raise rmodbus.RModbusError(rmodbus.RMODBUS.EXCEPTION_MESSAGE, recv.get_exchange_data())

    def _calc_rtc(self, value):
        ret = value * (1 / 256)
        if not self._dip2:
            ret = ret * 10
        return ret


# =============================================================================#
class Device_Job(dbase.DSlave_Job):
    def __init__(self, slave_num: int, resources: dict):
        super().__init__(slave_num, resources)
        self._config = DConfig_Slave_Pextron_URPE7104_v7_18(CONFIG.rconfig, slave_num)
        self._stack = resources['stack']
        self._modbus = resources['modbus']
        GLOBAL.modbus[str(slave_num)] = False
        self._device = Device(self._config.modbus_slave, self._config.dip2, self._modbus)
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
        # meter_alarms
        self._meter_alarms = {}
        # alarm current
        self._meter_alarms['currentA'] = self._config.alarm_current
        self._meter_alarms['currentB'] = self._config.alarm_current
        self._meter_alarms['currentC'] = self._config.alarm_current
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
            previous_meter = dbase.DReport_ReleCorrente()
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
                            previous_meter = dbase.DReport_ReleCorrente(actual_meter.exchange_data())

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

    def check_event(self, previous: dbase.DReport_ReleCorrente, actual: dbase.DReport_ReleCorrente) -> bool:
        return self._meter_tolerance.check(previous.tolerance(), actual.tolerance())

    def check_alarm(self, previous: dbase.DReport_ReleCorrente, actual: dbase.DReport_ReleCorrente) -> bool:
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
            if header.modelid != 4:
                raise ValueError('Invalid device on argument.')
            config = DConfig_Slave_Pextron_URPE7104_v7_18(CONFIG.rconfig, header.slave)
            rele = Device(config.modbus_slave, config.dip2, self._modbus)
            # Meter
            if header.cmdtype == 0:
                r = rele.get_meter()
                listen.replay_cmd_now(self._conn, r.exchange_data())
            # Flags
            elif header.cmdtype == 1:
                r = rele.get_alarm()
                listen.replay_cmd_now(self._conn, r.exchange_data())
            # Get Data
            elif header.cmdtype == 2:
                r = common.RData(rele.get_data().encode())
                listen.replay_cmd_now(self._conn, r)
            else:
                listen.replay_err(self._conn, CONST.RETURNCODE_CMD_INVALID)
        except rmodbus.RModbusError as err:
            listen.replay_err(self._conn, err.code + 2000)
        except Exception as err:
            self.logger.debug('Erro inesperado {}'.format(str(err)))
            listen.replay_err(self._conn, CONST.RETURNCODE_UNKNOWN)
