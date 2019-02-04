#!/usr/bin/python3
# -*- coding: utf-8 -*-

import math
import time
import json
import logging
import socket
import rlib.rmodbus as rmodbus
from rlib.rmodbus import RByteType as BTYPE
import rlib.common as common
import dlib.dsocket as dsocket
import dlib._dlisten as listen
import dlib.dreport as dreport
import dlib.dslave as dslave
from dlib.dcommon import GLOBAL, is_demanda_time
from dlib.dstatus import STATUS
from dlib.dconfig import DConfig_Slave_Reles, CONFIG
from rlib.common import CONST

ID = {'modelid': 1,
      'name': 'Pextron URP1439TU',
      'fmt_alarm': 0,
      'fmt_meter': 0,
      'fmt_meter_alarm': 1}


# =============================================================================#
class DPextron_URP1439TU_Config(DConfig_Slave_Reles):
    def __init__(self, rconfig: common.RConfig, slave_num: int):
        super().__init__(rconfig, slave_num)


# =============================================================================#
class DPextron_URP1439TU_AlarmeRele(dreport.DAlarmRele):
    _SIZE = 4
    _flag27_0 = 24
    _flag27A = 25
    _flag27B = 26
    _flag27C = 27
    _flag59A = 28
    _flag59B = 29
    _flag59C = 30
    _flag50A = 31
    _flag50B = 16
    _flag50C = 17
    _flag51A_TD = 18  # Tempo definido
    _flag51B_TD = 19  # Tempo definido
    _flag51C_TD = 20  # Tempo definido
    _flag51A_TP = 21  # Temporizado
    _flag51B_TP = 22  # Temporizado
    _flag51C_TP = 23  # Temporizado
    _flag81 = 8
    _flag50N = 9
    _flag51N_TD = 10  # Tempo definido
    _flag51N_TP = 11  # Temporizado
    _flag47 = 12
    _flag86 = 13
    _flag79_BR = 14  # Bloqueio de religamento
    _flag79_FR = 15  # Falha de religamento
    _flag94 = 0
    _flag79 = 1  # Religamento

    def __init__(self, data=None, offset=0):
        super().__init__(DPextron_URP1439TU_AlarmeRele._SIZE, data, offset)

    @property
    def flag27_0(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag27_0, self._offset)

    @flag27_0.setter
    def flag27_0(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag27_0, flag, self._offset)

    @property
    def flag27A(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag27A, self._offset)

    @flag27A.setter
    def flag27A(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag27A, flag, self._offset)

    @property
    def flag27B(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag27B, self._offset)

    @flag27B.setter
    def flag27B(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag27B, flag, self._offset)

    @property
    def flag27C(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag27C, self._offset)

    @flag27C.setter
    def flag27C(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag27C, flag, self._offset)

    @property
    def flag59A(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag59A, self._offset)

    @flag59A.setter
    def flag59A(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag59A, flag, self._offset)

    @property
    def flag59B(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag59B, self._offset)

    @flag59B.setter
    def flag59B(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag59B, flag, self._offset)

    @property
    def flag59C(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag59C, self._offset)

    @flag59C.setter
    def flag59C(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag59C, flag, self._offset)

    @property
    def flag50A(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag50A, self._offset)

    @flag50A.setter
    def flag50A(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag50A, flag, self._offset)

    @property
    def flag50B(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag50B, self._offset)

    @flag50B.setter
    def flag50B(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag50B, flag, self._offset)

    @property
    def flag50C(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag50C, self._offset)

    @flag50C.setter
    def flag50C(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag50C, flag, self._offset)

    @property
    def flag51A_TD(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag51A_TD, self._offset)

    @flag51A_TD.setter
    def flag51A_TD(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag51A_TD, flag, self._offset)

    @property
    def flag51B_TD(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag51B_TD, self._offset)

    @flag51B_TD.setter
    def flag51B_TD(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag51B_TD, flag, self._offset)

    @property
    def flag51C_TD(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag51C_TD, self._offset)

    @flag51C_TD.setter
    def flag51C_TD(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag51C_TD, flag, self._offset)

    @property
    def flag51A_TP(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag51A_TP, self._offset)

    @flag51A_TP.setter
    def flag51A_TP(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag51A_TP, flag, self._offset)

    @property
    def flag51B_TP(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag51B_TP, self._offset)

    @flag51B_TP.setter
    def flag51B_TP(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag51B_TP, flag, self._offset)

    @property
    def flag51C_TP(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag51C_TP, self._offset)

    @flag51C_TP.setter
    def flag51C_TP(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag51C_TP, flag, self._offset)

    @property
    def flag81(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag81, self._offset)

    @flag81.setter
    def flag81(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag81, flag, self._offset)

    @property
    def flag47(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag47, self._offset)

    @flag47.setter
    def flag47(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag47, flag, self._offset)

    @property
    def flag86(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag86, self._offset)

    @flag86.setter
    def flag86(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag86, flag, self._offset)

    @property
    def flag94(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag94, self._offset)

    @flag94.setter
    def flag94(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag94, flag, self._offset)

    @property
    def flag50N(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag50N, self._offset)

    @flag50N.setter
    def flag50N(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag50N, flag, self._offset)

    @property
    def flag51N_TD(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag51N_TD, self._offset)

    @flag51N_TD.setter
    def flag51N_TD(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag51N_TD, flag, self._offset)

    @property
    def flag51N_TP(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag51N_TP, self._offset)

    @flag51N_TP.setter
    def flag51N_TP(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag51N_TP, flag, self._offset)

    @property
    def flag79(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag79, self._offset)

    @flag79.setter
    def flag79(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag79, flag, self._offset)

    @property
    def flag79_BR(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag79_BR, self._offset)

    @flag79_BR.setter
    def flag79_BR(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag79_BR, flag, self._offset)

    @property
    def flag79_FR(self) -> bool:
        return self._data.is_bit(DPextron_URP1439TU_AlarmeRele._flag79_FR, self._offset)

    @flag79_FR.setter
    def flag79_FR(self, flag):
        self._data.put_bit(DPextron_URP1439TU_AlarmeRele._flag79_FR, flag, self._offset)

    def __str__(self):
        return 'flag27_0:{} flag27A:{} flag27B:{} flag27C:{} ' \
               'flag59A:{} flag59B:{} flag59C:{} ' \
               'flag50A:{} flag50B:{} flag50C:{} ' \
               'flag51A_TD:{} flag51B_TD:{} flag51C_TD:{} ' \
               'flag51A_TP:{} flag51B_TP:{} flag51C_TP:{} ' \
               'flag81:{} flag47:{} flag86:{} flag94:{} ' \
               'flag50N:{} flag51N_TD:{} flag51N_TP:{} ' \
               'flag79:{} flag79_BR:{} flag79_FR:{}'.format(self.flag27_0, self.flag27A, self.flag27B, self.flag27C,
                                                            self.flag59A, self.flag59B, self.flag59C,
                                                            self.flag50A, self.flag50B, self.flag50C,
                                                            self.flag51A_TD, self.flag51B_TD, self.flag51C_TD,
                                                            self.flag51A_TP, self.flag51B_TP, self.flag51C_TP,
                                                            self.flag81, self.flag47, self.flag86, self.flag94,
                                                            self.flag50N, self.flag51N_TD, self.flag51N_TP,
                                                            self.flag79, self.flag79_BR, self.flag79_FR)


# =============================================================================#
class DPextron_URP1439TU():
    def __init__(self, slave: int, modbus: rmodbus.RModbusComm):
        self.slave = slave
        self._modbus = modbus
        self.logger = logging.getLogger(__name__)

    def get_meter(self) -> dreport.DReportRele:
        ret = dreport.DReportRele()

        send = []
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 0, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 13, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 153, 3))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 159, 8))

        recv = []
        for x in range(0, len(send)):
            recv.append(self._modbus.exchange(send[x]))

        r = rmodbus.RModbusReadResponse(recv[0].get_exchange_data())
        rtc = r.get_byte(BTYPE.BYTE16, 0)

        r = rmodbus.RModbusReadResponse(recv[1].get_exchange_data())
        rtp = r.get_byte(BTYPE.BYTE16, 0)

        r = rmodbus.RModbusReadResponse(recv[2].get_exchange_data())
        fatorraw = (self._calculate_factor(r.get_byte(BTYPE.BYTE16, 0)) + self._calculate_factor(
            r.get_byte(BTYPE.BYTE16, 2)) + self._calculate_factor(r.get_byte(BTYPE.BYTE16, 4))) / 3
        # fator = int(fatorraw * 100)
        ret.factor = fatorraw

        r = rmodbus.RModbusReadResponse(recv[3].get_exchange_data())
        ret.frequency = int((r.get_byte(BTYPE.BYTE16, 0) * 1 / 256))
        ret.currenteA = int((r.get_byte(BTYPE.BYTE16, 2) * 1 / 256 * rtc))
        ret.currenteB = int((r.get_byte(BTYPE.BYTE16, 4) * 1 / 256 * rtc))
        ret.currenteC = int((r.get_byte(BTYPE.BYTE16, 6) * 1 / 256 * rtc))
        ret.currenteN = int((r.get_byte(BTYPE.BYTE16, 8) * 1 / 256 * rtc))
        ret.voltA = int(((r.get_byte(BTYPE.BYTE16, 10) * 1 / 128 * rtp)) / math.sqrt(3))
        ret.voltB = int(((r.get_byte(BTYPE.BYTE16, 12) * 1 / 128 * rtp)) / math.sqrt(3))
        ret.voltC = int(((r.get_byte(BTYPE.BYTE16, 14) * 1 / 128 * rtp)) / math.sqrt(3))

        return ret

    def get_alarm(self) -> DPextron_URP1439TU_AlarmeRele:
        ret = DPextron_URP1439TU_AlarmeRele()

        send = rmodbus.RModbusReadCoilStatus.create(self.slave, 1, 34)
        recv = self._modbus.exchange(send)
        r = rmodbus.RModbusReadResponse(recv.get_exchange_data())
        ret.flag27_0 = r.is_bit(0)
        ret.flag47 = r.is_bit(1)
        ret.flag27A = r.is_bit(2)
        ret.flag59A = r.is_bit(3)
        ret.flag50A = r.is_bit(4)
        ret.flag51A_TD = r.is_bit(5)
        ret.flag51A_TP = r.is_bit(6)
        ret.flag86 = r.is_bit(8)
        ret.flag79 = r.is_bit(9)
        ret.flag27B = r.is_bit(10)
        ret.flag59B = r.is_bit(11)
        ret.flag50B = r.is_bit(12)
        ret.flag51B_TD = r.is_bit(13)
        ret.flag51B_TP = r.is_bit(14)
        ret.flag27C = r.is_bit(18)
        ret.flag59C = r.is_bit(19)
        ret.flag50C = r.is_bit(20)
        ret.flag51C_TD = r.is_bit(21)
        ret.flag51C_TP = r.is_bit(22)
        ret.flag81 = r.is_bit(23)
        ret.flag50N = r.is_bit(28)
        ret.flag51N_TD = r.is_bit(29)
        ret.flag51N_TP = r.is_bit(30)
        ret.flag79_BR = r.is_bit(31)
        ret.flag79_FR = r.is_bit(32)
        ret.flag94 = r.is_bit(33)

        return ret

    def rearme_remoto_bandeirolas(self):
        send = rmodbus.RModbusForceSingleCoil.create(self.slave, 48, True)
        recv = self._modbus.exchange(send)
        if recv.response_type() != rmodbus.RModbusResponceType.FORCE_SINGLE_COIL_RESPONSE:
            raise rmodbus.RModbusError(rmodbus.RMODBUS.EXCEPTION_INVALID_FUNCTION)

    def reset_registros_corrente_tensao(self):
        send = rmodbus.RModbusForceSingleCoil.create(self.slave, 49, True)
        recv = self._modbus.exchange(send)
        if recv.response_type() != rmodbus.RModbusResponceType.FORCE_SINGLE_COIL_RESPONSE:
            raise rmodbus.RModbusError(rmodbus.RMODBUS.EXCEPTION_INVALID_FUNCTION)

    def reset_86_79(self):
        send = rmodbus.RModbusForceSingleCoil.create(self.slave, 50, True)
        recv = self._modbus.exchange(send)
        if recv.response_type() != rmodbus.RModbusResponceType.FORCE_SINGLE_COIL_RESPONSE:
            raise rmodbus.RModbusError(rmodbus.RMODBUS.EXCEPTION_INVALID_FUNCTION)

    def pulse_saida_rele(self):
        self._pulse_any_coil(44)

    def pulse_saida_ba(self):
        self._pulse_any_coil(45)

    def pulse_saida_v_ok_79(self):
        self._pulse_any_coil(46)

    def pulse_saida_trip(self):
        self._pulse_any_coil(47)

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
            if 'rtc' in regs and isinstance(regs['rtc'], int) and (regs['rtc'] >= 1 and regs['rtc'] <= 2500):
                self._set_any_register(regs['rtc'], 0)
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
        send.append(rmodbus.RModbusReadCoilStatus.create(self.slave, 35, 2))
        send.append(rmodbus.RModbusReadCoilStatus.create(self.slave, 42, 6))
        recv = []
        for x in range(0, len(send)):
            recv.append(self._modbus.exchange(send[x]))
        r = rmodbus.RModbusReadResponse(recv[0].get_exchange_data())
        bobina_ba = r.is_bit(0)
        auto_check = r.is_bit(1)
        r = rmodbus.RModbusReadResponse(recv[1].get_exchange_data())
        estado_disjuntor = r.is_bit(0)
        funcao_bloqueio_86 = r.is_bit(1)
        saida_rele = r.is_bit(2)
        saida_ba = r.is_bit(3)
        saida_v_ok_79 = r.is_bit(4)
        saida_trip = r.is_bit(5)
        return dict(bobina_ba=bobina_ba,
                    auto_check=auto_check,
                    estado_disjuntor=estado_disjuntor,
                    funcao_bloqueio_86=funcao_bloqueio_86,
                    saida_rele=saida_rele,
                    saida_ba=saida_ba,
                    saida_v_ok_79=saida_v_ok_79,
                    saida_trip=saida_trip)

    def _get_registers(self) -> {}:
        send = []
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 0, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 13, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 26, 8))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 44, 3))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 66, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 128, 4))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 136, 2))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 158, 1))
        send.append(rmodbus.RModbusReadHoldingRegister.create(self.slave, 167, 1))
        recv = []
        for x in range(0, len(send)):
            recv.append(self._modbus.exchange(send[x]))
        r = rmodbus.RModbusReadResponse(recv[0].get_exchange_data())
        rtc = r.get_byte(rmodbus.RByteType.BYTE16, 0)
        r = rmodbus.RModbusReadResponse(recv[1].get_exchange_data())
        rtp = r.get_byte(rmodbus.RByteType.BYTE16, 0)
        r = rmodbus.RModbusReadResponse(recv[2].get_exchange_data())
        tempo_de_registro_de_perfil_de_carga = r.get_byte(rmodbus.RByteType.BYTE16, 0)
        habilita_registro_de_oscilografia = r.get_byte(rmodbus.RByteType.BYTE16, 2) == 256
        ano = int('%x' % (r.get_byte(rmodbus.RByteType.BYTE16, 4) >> 8))
        mes = int('%x' % (r.get_byte(rmodbus.RByteType.BYTE16, 6) >> 8))
        dia = int('%x' % (r.get_byte(rmodbus.RByteType.BYTE16, 8) >> 8))
        hora = int('%x' % (r.get_byte(rmodbus.RByteType.BYTE16, 10) >> 8))
        minutos = int('%x' % (r.get_byte(rmodbus.RByteType.BYTE16, 12) >> 8))
        segundos = int('%x' % (r.get_byte(rmodbus.RByteType.BYTE16, 14) >> 8))
        r = rmodbus.RModbusReadResponse(recv[3].get_exchange_data())
        habilita_programacao = r.get_byte(rmodbus.RByteType.BYTE16, 0) == 256
        multiplicador_corrente_oscilografia = r.get_byte(rmodbus.RByteType.BYTE16, 2) / 256
        multiplicador_tensao_oscilografia = r.get_byte(rmodbus.RByteType.BYTE16, 4) / 256
        r = rmodbus.RModbusReadResponse(recv[4].get_exchange_data())
        oscilografia_de_leitura = r.get_byte(rmodbus.RByteType.BYTE16, 0)
        r = rmodbus.RModbusReadResponse(recv[5].get_exchange_data())
        reg_corrente_max_fase = (r.get_byte(rmodbus.RByteType.BYTE16, 0) / 256) * rtc
        reg_corrente_max_neutro = (r.get_byte(rmodbus.RByteType.BYTE16, 2) / 256) * rtc
        reg_tensao_min_fase = (r.get_byte(rmodbus.RByteType.BYTE16, 4) / 128) * rtp
        reg_tensao_max_fase = (r.get_byte(rmodbus.RByteType.BYTE16, 6) / 128) * rtp
        r = rmodbus.RModbusReadResponse(recv[6].get_exchange_data())
        tipo_do_rele = r.get_byte(rmodbus.RByteType.BYTE16, 0)
        versao_do_rele = r.get_byte(rmodbus.RByteType.BYTE16, 2)
        r = rmodbus.RModbusReadResponse(recv[7].get_exchange_data())
        quantidade_de_oscilografias = r.get_byte(rmodbus.RByteType.BYTE16, 0)
        r = rmodbus.RModbusReadResponse(recv[8].get_exchange_data())
        alimentacao_auxiliar = r.get_byte(rmodbus.RByteType.BYTE16, 0) / 256
        return dict(rtc=rtc,
                    rtp=rtp,
                    tempo_de_registro_de_perfil_de_carga=tempo_de_registro_de_perfil_de_carga,
                    habilita_registro_de_oscilografia=habilita_registro_de_oscilografia,
                    ano=ano,
                    mes=mes,
                    dia=dia,
                    hora=hora,
                    minutos=minutos,
                    segundos=segundos,
                    habilita_programacao=habilita_programacao,
                    multiplicador_corrente_oscilografia=multiplicador_corrente_oscilografia,
                    multiplicador_tensao_oscilografia=multiplicador_tensao_oscilografia,
                    oscilografia_de_leitura=oscilografia_de_leitura,
                    reg_corrente_max_fase=reg_corrente_max_fase,
                    reg_corrente_max_neutro=reg_corrente_max_neutro,
                    reg_tensao_min_fase=reg_tensao_min_fase,
                    reg_tensao_max_fase=reg_tensao_max_fase,
                    tipo_do_rele=tipo_do_rele,
                    versao_do_rele=versao_do_rele,
                    quantidade_de_oscilografias=quantidade_de_oscilografias,
                    alimentacao_auxiliar=alimentacao_auxiliar)

    def _set_any_register(self, value, addr):
        send = rmodbus.RModbusPresetSingleRegister.create(self.slave, addr, value)
        recv = self._modbus.exchange(send)
        if recv.response_type() == rmodbus.RModbusResponceType.EXCEPTION_RESPONSE:
            raise rmodbus.RModbusError(rmodbus.RMODBUS.EXCEPTION_MESSAGE, recv.get_exchange_data())

    def _set_any_coil(self, value: bool, addr):
        send = rmodbus.RModbusForceSingleCoil.create(self.slave, addr, value)
        recv = self._modbus.exchange(send)
        if recv.response_type() != rmodbus.RModbusResponceType.FORCE_SINGLE_COIL_RESPONSE:
            raise rmodbus.RModbusError(rmodbus.RMODBUS.EXCEPTION_INVALID_FUNCTION)

    def _pulse_any_coil(self, addr):
        # get actual state
        send = rmodbus.RModbusReadCoilStatus.create(self.slave, addr, 1)
        r = rmodbus.RModbusReadResponse(self._modbus.exchange(send).get_exchange_data())
        s = r.is_bit(0)
        # send inverse state
        self._set_any_coil((not s), addr)
        # send actual state
        self._set_any_coil(s, addr)

    def _calculate_factor(self, value: int) -> float:
        if value > 16384:
            return ((65536 - value) * (1 / 16384)) * -1
        else:
            return value * (1 / 16384)


# =============================================================================#
class DPextron_URP1439TU_Thread(dslave.DSlave_Thread):
    def __init__(self, slave_num: int, resources: dict):
        super().__init__(slave_num, resources)
        self._stack = resources['stack']
        self._modbus = resources['modbus']
        GLOBAL.modbus[str(slave_num)] = False
        self._device = DPextron_URP1439TU(self.modbus_slave, self._modbus)
        self.nameid = '{} [{} addr:{}]'.format(self.slave_name, ID['name'], self.modbus_slave)
        # interval
        self._interval = CONFIG.slaves[self.slave_num].interval
        # periodic
        self._periodic = CONFIG.slaves[self.slave_num].periodic
        # meter_tolerances
        self._meter_tolerances = {}
        # tolerance current
        self._meter_tolerances['currentA'] = CONFIG.slaves[self.slave_num].tolerance_current
        self._meter_tolerances['currentB'] = CONFIG.slaves[self.slave_num].tolerance_current
        self._meter_tolerances['currentC'] = CONFIG.slaves[self.slave_num].tolerance_current
        # tolerance neutro
        self._meter_tolerances['currentN'] = CONFIG.slaves[self.slave_num].tolerance_neutro
        # tolerance volt
        self._meter_tolerances['voltA'] = CONFIG.slaves[self.slave_num].tolerance_voltage
        self._meter_tolerances['voltB'] = CONFIG.slaves[self.slave_num].tolerance_voltage
        self._meter_tolerances['voltC'] = CONFIG.slaves[self.slave_num].tolerance_voltage
        # tolerance factor
        self._meter_tolerances['factor'] = CONFIG.slaves[self.slave_num].tolerance_factor
        # tolerance frequency
        self._meter_tolerances['frequency'] = CONFIG.slaves[self.slave_num].tolerance_frequency
        # meter_alarms
        self._meter_alarms = {}
        # alarm current
        self._meter_alarms['currentA'] = CONFIG.slaves[self.slave_num].alarm_current
        self._meter_alarms['currentB'] = CONFIG.slaves[self.slave_num].alarm_current
        self._meter_alarms['currentC'] = CONFIG.slaves[self.slave_num].alarm_current
        # alarm neutro
        self._meter_alarms['currentN'] = CONFIG.slaves[self.slave_num].alarm_neutro
        # alarm volt
        self._meter_alarms['voltA'] = CONFIG.slaves[self.slave_num].alarm_voltage
        self._meter_alarms['voltB'] = CONFIG.slaves[self.slave_num].alarm_voltage
        self._meter_alarms['voltC'] = CONFIG.slaves[self.slave_num].alarm_voltage
        # alarm factor
        self._meter_alarms['factor'] = CONFIG.slaves[self.slave_num].alarm_factor
        # alarm frequency
        self._meter_alarms['frequency'] = CONFIG.slaves[self.slave_num].alarm_frequency
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
        # create tolerance class
        self._meter_tolerance = dslave.DTolerance(self._meter_tolerances)
        # create alarm class
        self._meter_alarm = dslave.DTolerance(self._meter_alarms)
        # local logger
        self.logger = logging.getLogger(__name__)

    def run(self):
        try:
            self.logger.info('Iniciando job "{}" {}'.format(self.local, self.nameid))
            report_time = time.time() + self._periodic
            starting = True
            previous_meter = dreport.DReportRele()
            previous_alarm = DPextron_URP1439TU_AlarmeRele()
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
                            previous_alarm = DPextron_URP1439TU_AlarmeRele(actual_alarm.exchange_data())

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
                            previous_meter = dreport.DReportRele(actual_meter.exchange_data())

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
            self.logger.fatal('Thread DPextron_URP1439TU_Thread fail:{}'.format(str(err)))

    def check_event(self, previous: dreport.DReportRele, actual: dreport.DReportRele) -> bool:
        return self._meter_tolerance.check(previous.tolerance(), actual.tolerance())

    def check_alarm(self, previous: dreport.DReportRele, actual: dreport.DReportRele) -> bool:
        return self._meter_alarm.check(previous.tolerance(), actual.tolerance())


# =============================================================================#
class DPextron_URP1439TU_Process(listen.DListenProcess):
    def __init__(self, conn: socket, addr, header: dsocket.DSocketHeaderBasic, data: common.RData, resources: dict):
        super().__init__(conn, addr, header, data, resources)
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
            if header.modelid != 1:
                raise ValueError('Invalid device on argument.')
            rele = DPextron_URP1439TU(CONFIG.slaves[header.slave].modbus_slave, self._modbus)
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
            # Pulse saidas rele
            elif header.cmdtype == 3:
                if self._data[0] == 0x00:
                    rele.pulse_saida_rele()
                elif self._data[0] == 0x01:
                    rele.pulse_saida_ba()
                elif self._data[0] == 0x02:
                    rele.pulse_saida_v_ok_79()
                elif self._data[0] == 0x03:
                    rele.pulse_saida_trip()
                else:
                    listen.replay_err(self._conn, CONST.RETURNCODE_CMD_INVALID)
                    return
                listen.replay_ok(self._conn)
            else:
                listen.replay_err(self._conn, CONST.RETURNCODE_CMD_INVALID)
        except rmodbus.RModbusError as err:
            listen.replay_err(self._conn, err.code + 2000)
        except Exception as err:
            self.logger.debug('Erro inesperado {}'.format(str(err)))
            listen.replay_err(self._conn, CONST.RETURNCODE_UNKNOWN)
