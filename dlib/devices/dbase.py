#!/usr/bin/python3
# -*- coding: utf-8 -*-

import threading
import logging
import socket
from rlib.common import CONST, RByteType as BTYPE, RData
import dlib.dsocket as dsocket
from dlib._dlisten import replay_err
from dlib.dconfig import CONFIG, DConfig_Slave


# =============================================================================#
class DReport_Rele(object):
    _SIZE = 24
    _CURRENTEA = 0
    _CURRENTEB = 2
    _CURRENTEC = 4
    _CURRENTEN = 6
    _VOLTA = 8
    _VOLTB = 12
    _VOLTC = 16
    _FREQUENCY = 20
    _FACTOR = 22

    def __init__(self, data=None):
        if not data:
            self._data = RData(bytearray(DReport_Rele._SIZE))
        elif len(data) < DReport_Rele._SIZE:
            self._data = RData(data)
            for x in range(len(data), DReport_Rele._SIZE):
                self._data.append(0)
        else:
            self._data = RData(data[:DReport_Rele._SIZE])

    @property
    def currenteA(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Rele._CURRENTEA, signed=True)

    @currenteA.setter
    def currenteA(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_Rele._CURRENTEA, value, signed=True)

    @property
    def currenteB(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Rele._CURRENTEB, signed=True)

    @currenteB.setter
    def currenteB(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_Rele._CURRENTEB, value, signed=True)

    @property
    def currenteC(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Rele._CURRENTEC, signed=True)

    @currenteC.setter
    def currenteC(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_Rele._CURRENTEC, value, signed=True)

    @property
    def currenteN(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Rele._CURRENTEN, signed=True)

    @currenteN.setter
    def currenteN(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_Rele._CURRENTEN, value, signed=True)

    @property
    def voltA(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_Rele._VOLTA, signed=True)

    @voltA.setter
    def voltA(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReport_Rele._VOLTA, value, signed=True)

    @property
    def voltB(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_Rele._VOLTB, signed=True)

    @voltB.setter
    def voltB(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReport_Rele._VOLTB, value, signed=True)

    @property
    def voltC(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_Rele._VOLTC, signed=True)

    @voltC.setter
    def voltC(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReport_Rele._VOLTC, value, signed=True)

    @property
    def frequency(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Rele._FREQUENCY) / 100

    @frequency.setter
    def frequency(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_Rele._FREQUENCY, int(value * 100))

    @property
    def factor(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Rele._FACTOR, signed=True) / 100

    @factor.setter
    def factor(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_Rele._FACTOR, int(value * 100), signed=True)

    def dump(self) -> str:
        return self._data.dump()

    def exchange_data(self):
        return self._data

    def __str__(self):
        return 'CurrenteA:{} CurrentB:{} CurrentC:{} CurrenteN:{} VoltA:{} VoltB:{} VoltC:{} Factor:{} Frequency:{}'.format(
            self.currenteA,
            self.currenteB,
            self.currenteC,
            self.currenteN,
            self.voltA,
            self.voltB,
            self.voltC,
            self.factor,
            self.frequency)

    def tolerance(self) -> {}:
        ret = {}
        ret['currentA'] = self.currenteA
        ret['currentB'] = self.currenteB
        ret['currentC'] = self.currenteC
        ret['currentN'] = self.currenteN
        ret['voltA'] = self.voltA
        ret['voltB'] = self.voltB
        ret['voltC'] = self.voltC
        ret['factor'] = self.factor
        ret['frequency'] = self.frequency
        return ret


# =============================================================================#
class DReport_ReleCorrente(object):
    _SIZE = 8
    _CURRENTEA = 0
    _CURRENTEB = 2
    _CURRENTEC = 4
    _CURRENTEN = 6

    def __init__(self, data=None):
        if not data:
            self._data = RData(bytearray(DReport_ReleCorrente._SIZE))
        elif len(data) < DReport_ReleCorrente._SIZE:
            self._data = RData(data)
            for x in range(len(data), DReport_ReleCorrente._SIZE):
                self._data.append(0)
        else:
            self._data = RData(data[:DReport_ReleCorrente._SIZE])

    @property
    def currenteA(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_ReleCorrente._CURRENTEA, signed=True)

    @currenteA.setter
    def currenteA(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_ReleCorrente._CURRENTEA, value, signed=True)

    @property
    def currenteB(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_ReleCorrente._CURRENTEB, signed=True)

    @currenteB.setter
    def currenteB(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_ReleCorrente._CURRENTEB, value, signed=True)

    @property
    def currenteC(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_ReleCorrente._CURRENTEC, signed=True)

    @currenteC.setter
    def currenteC(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_ReleCorrente._CURRENTEC, value, signed=True)

    @property
    def currenteN(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_ReleCorrente._CURRENTEN, signed=True)

    @currenteN.setter
    def currenteN(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_ReleCorrente._CURRENTEN, value, signed=True)

    def dump(self) -> str:
        return self._data.dump()

    def exchange_data(self):
        return self._data

    def __str__(self):
        return 'CurrenteA:{} CurrentB:{} CurrentC:{} CurrenteN:{}'.format(
            self.currenteA,
            self.currenteB,
            self.currenteC,
            self.currenteN)

    def tolerance(self) -> {}:
        ret = {}
        ret['currentA'] = self.currenteA
        ret['currentB'] = self.currenteB
        ret['currentC'] = self.currenteC
        ret['currentN'] = self.currenteN
        return ret


# =============================================================================#
class DAlarm_Rele(object):
    def __init__(self, size: int, data=None, offset=0):
        if not data:
            self._data = RData(bytearray(size))
        elif len(data) < size:
            self._data = RData(data)
            for x in range(len(data), size):
                self._data.append(0)
        else:
            self._data = RData(data[:size])
        self._offset = offset

    def is_flag(self, flag) -> bool:
        return self._data.is_bit(flag, self._offset)

    def set_flag(self, flag):
        self._data.set_bit(flag, self._offset)

    def clear_flag(self, flag):
        self._data.clear_bit(flag, self._offset)

    def toggle_flag(self, flag):
        self._data.toggle_bit(flag, self._offset)

    def put_flag(self, flag, value):
        if value:
            self.set_flag(flag)
        else:
            self.clear_flag(flag)

    def dump(self) -> str:
        return self._data.dump()

    def exchange_data(self):
        return self._data

    def is_normal(self):
        for x in range(0, len(self._data)):
            if self._data[x] != 0:
                return False
        return True

    def is_equal(self, other):
        if (len(self._data) != len(other.exchange_data())):
            return False
        for x in range(0, len(self._data)):
            if self._data[x] != other[x]:
                return False
        return True


# =============================================================================#
class DReport_Codi(DAlarm_Rele):
    _SIZE = 24
    _flag_tarifacao_indutiva = 7
    _flag_tarifacao_capacitiva = 6
    _flag_fim_intervalo_reativo = 5
    _flag_reposicao_demanda = 4
    _flag_tarifa_reativa = 3
    _NS = 1
    _TP = 3
    _PH = 4
    _PA = 5
    _PR = 7
    _PTA = 9
    _PTR = 13
    _PTAP = 17
    _FPT = 21

    __FATOR = 1000

    def __init__(self, data=None):
        super().__init__(DReport_Codi._SIZE, data)
        self._offset = 0
        if not data:
            self._data = RData(bytearray(DReport_Codi._SIZE))
        elif len(data) < DReport_Codi._SIZE:
            self._data = RData(data)
            for x in range(len(data), DReport_Codi._SIZE):
                self._data.append(0)
        else:
            self._data = RData(data[:DReport_Codi._SIZE])

    @property
    def tarifacao_indutiva(self) -> bool:
        return self._data.is_bit(DReport_Codi._flag_tarifacao_indutiva, self._offset)

    @tarifacao_indutiva.setter
    def tarifacao_indutiva(self, flag):
        self._data.put_bit(DReport_Codi._flag_tarifacao_indutiva, flag, self._offset)

    @property
    def tarifacao_capacitiva(self) -> bool:
        return self._data.is_bit(DReport_Codi._flag_tarifacao_capacitiva, self._offset)

    @tarifacao_capacitiva.setter
    def tarifacao_capacitiva(self, flag):
        self._data.put_bit(DReport_Codi._flag_tarifacao_capacitiva, flag, self._offset)

    @property
    def fim_intervalo_reativo(self) -> bool:
        return self._data.is_bit(DReport_Codi._flag_fim_intervalo_reativo, self._offset)

    @fim_intervalo_reativo.setter
    def fim_intervalo_reativo(self, flag):
        self._data.put_bit(DReport_Codi._flag_fim_intervalo_reativo, flag, self._offset)

    @property
    def reposicao_demanda(self) -> bool:
        return self._data.is_bit(DReport_Codi._flag_reposicao_demanda, self._offset)

    @reposicao_demanda.setter
    def reposicao_demanda(self, flag):
        self._data.put_bit(DReport_Codi._flag_reposicao_demanda, flag, self._offset)

    @property
    def tarifa_reativa(self) -> bool:
        return self._data.is_bit(DReport_Codi._flag_tarifa_reativa, self._offset)

    @tarifa_reativa.setter
    def tarifa_reativa(self, flag):
        self._data.put_bit(DReport_Codi._flag_tarifa_reativa, flag, self._offset)

    @property
    def numero_segundos(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Codi._NS)

    @numero_segundos.setter
    def numero_segundos(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_Codi._NS, value)

    @property
    def tipo_tarifa(self):
        return self._data.get_byte(BTYPE.BYTE8, DReport_Codi._TP)

    @tipo_tarifa.setter
    def tipo_tarifa(self, value):
        self._data.set_byte(BTYPE.BYTE8, DReport_Codi._TP, value)

    @property
    def posto_horario(self):
        return self._data.get_byte(BTYPE.BYTE8, DReport_Codi._PH)

    @posto_horario.setter
    def posto_horario(self, value):
        self._data.set_byte(BTYPE.BYTE8, DReport_Codi._PH, value)

    @property
    def pulsos_ativos(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Codi._PA)

    @pulsos_ativos.setter
    def pulsos_ativos(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_Codi._PA, value)

    @property
    def pulsos_reativos(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Codi._PR)

    @pulsos_reativos.setter
    def pulsos_reativos(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_Codi._PR, value)

    @property
    def potencia_ativa(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_Codi._PTA)

    @potencia_ativa.setter
    def potencia_ativa(self, value: float):
        self._data.set_byte(BTYPE.BYTE32, DReport_Codi._PTA, int(round(value * DReport_Codi.__FATOR, 0)))

    @property
    def potencia_reativa(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_Codi._PTR)

    @potencia_reativa.setter
    def potencia_reativa(self, value: float):
        self._data.set_byte(BTYPE.BYTE32, DReport_Codi._PTR, int(round(value * DReport_Codi.__FATOR, 0)))

    @property
    def potencia_aparente(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_Codi._PTAP)

    @potencia_aparente.setter
    def potencia_aparente(self, value: float):
        self._data.set_byte(BTYPE.BYTE32, DReport_Codi._PTAP, int(round(value * DReport_Codi.__FATOR, 0)))

    @property
    def fator_potencia(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_Codi._FPT)

    @fator_potencia.setter
    def fator_potencia(self, value: float):
        self._data.set_byte(BTYPE.BYTE32, DReport_Codi._FPT, int(round(value * DReport_Codi.__FATOR, 0)))

    def __str__(self):
        return 'TI:{0!r} TC:{1!r} FIR:{2!r} RD:{3!r} TR:{4!r} NS:{5} TPT:{6} PH:{7} PA:{8} PR:{9} PTA:{10:.2f} PTR:{11:.2f} PTAP:{12:.2f} FP:{13:.2f}'.format(
            self.tarifacao_indutiva,
            self.tarifacao_capacitiva,
            self.fim_intervalo_reativo,
            self.reposicao_demanda,
            self.tarifa_reativa,
            self.numero_segundos,
            self.tipo_tarifa,
            self.posto_horario,
            self.pulsos_ativos,
            self.pulsos_reativos,
            self.potencia_ativa / DReport_Codi.__FATOR,
            self.potencia_reativa / DReport_Codi.__FATOR,
            self.potencia_aparente / DReport_Codi.__FATOR,
            self.fator_potencia / DReport_Codi.__FATOR)


# =============================================================================#
class DSlave_Job(threading.Thread):
    def __init__(self, slave_num: int, resources: dict):
        threading.Thread.__init__(self)
        self._config = DConfig_Slave(CONFIG.rconfig, slave_num)
        self.slave_num = slave_num
        self.slave_name = self._config.slave_name
        self.local = self._config.local
        self._resources = resources
        self._stop_event = threading.Event()
        self.logger = logging.getLogger(__name__)

    def run(self):
        raise NotImplementedError

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


# =============================================================================#
class DSlave_Process(threading.Thread):
    def __init__(self, conn: socket, addr, header: dsocket.DSocketHeaderBasic, data: RData):
        threading.Thread.__init__(self)
        self._conn = conn
        self._addr = addr
        self._header = header
        self._data = data

    @property
    def addr(self):
        return self._addr

    @property
    def conn(self):
        return self._conn

    @property
    def header(self):
        return self._header

    @property
    def data(self):
        return self._data

    def run(self):
        if self.header.messagetype == dsocket.DSocketMessagesType.CMD_NOW:
            self.proc_cmd_now()
        elif self.header.messagetype == dsocket.DSocketMessagesType.CMD:
            self.proc_cmd()
        else:
            replay_err(self._conn, CONST.RETURNCODE_CMD_INVALID)

    def proc_cmd_now(self):
        raise NotImplementedError

    def proc_cmd(self):
        raise NotImplementedError


# =============================================================================#
class DReport_C001(object):
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
            self._data = RData(bytearray(DReport_C001._SIZE))
        elif len(data) < DReport_C001._SIZE:
            self._data = RData(data)
            for x in range(len(data), DReport_C001._SIZE):
                self._data.append(0)
        else:
            self._data = RData(data[:DReport_C001._SIZE])

    @property
    def version(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_C001._VERSION)

    @version.setter
    def version(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_C001._VERSION, value)

    @property
    def reles(self):
        return self._data.get_byte(BTYPE.BYTE8, DReport_C001._RELES)

    @reles.setter
    def reles(self, value):
        self._data.set_byte(BTYPE.BYTE8, DReport_C001._RELES, value)

    @property
    def gpaci(self):
        return self._data.get_byte(BTYPE.BYTE8, DReport_C001._GPACI)

    @gpaci.setter
    def gpaci(self, value):
        self._data.set_byte(BTYPE.BYTE8, DReport_C001._GPACI, value)

    @property
    def flow(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_C001._FLOW)

    @flow.setter
    def flow(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReport_C001._FLOW, value)

    @property
    def temp1(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_C001._TEMP1, signed=True) / 100

    @temp1.setter
    def temp1(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReport_C001._TEMP1, value * 100, signed=True)

    @property
    def temp2(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_C001._TEMP2, signed=True) / 100

    @temp2.setter
    def temp2(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReport_C001._TEMP2, value * 100, signed=True)

    @property
    def temp3(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_C001._TEMP3, signed=True) / 100

    @temp3.setter
    def temp3(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReport_C001._TEMP3, value * 100, signed=True)

    @property
    def ad(self):
        return self._data.get_byte(BTYPE.BYTE16, DReport_C001._AD)

    @ad.setter
    def ad(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReport_C001._AD, value)

    @property
    def wdog_relay(self):
        return self._data.get_byte(BTYPE.BYTE8, DReport_C001._WDOG_RELAY)

    @wdog_relay.setter
    def wdog_relay(self, value):
        self._data.set_byte(BTYPE.BYTE8, DReport_C001._WDOG_RELAY, value)

    @property
    def wdog_time(self):
        return self._data.get_byte(BTYPE.BYTE32, DReport_C001._WDOG_TIME, signed=True) / 1000

    @wdog_time.setter
    def wdog_time(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReport_C001._WDOG_TIME, value * 1000, signed=True)

    def set_rele(self, addr, value=True):
        self._data.put_bit(addr, value, DReport_C001._RELES)

    def get_rele(self, addr) -> bool:
        return self._data.is_bit(addr, DReport_C001._RELES)

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
