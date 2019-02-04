#!/usr/bin/python3
# -*- coding: utf-8 -*-

import rlib.common as common
from rlib.common import RByteType as BTYPE


# =============================================================================#
class DReportRele(object):
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
            self._data = common.RData(bytearray(DReportRele._SIZE))
        elif len(data) < DReportRele._SIZE:
            self._data = common.RData(data)
            for x in range(len(data), DReportRele._SIZE):
                self._data.append(0)
        else:
            self._data = common.RData(data[:DReportRele._SIZE])

    @property
    def currenteA(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportRele._CURRENTEA, signed=True)

    @currenteA.setter
    def currenteA(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportRele._CURRENTEA, value, signed=True)

    @property
    def currenteB(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportRele._CURRENTEB, signed=True)

    @currenteB.setter
    def currenteB(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportRele._CURRENTEB, value, signed=True)

    @property
    def currenteC(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportRele._CURRENTEC, signed=True)

    @currenteC.setter
    def currenteC(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportRele._CURRENTEC, value, signed=True)

    @property
    def currenteN(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportRele._CURRENTEN, signed=True)

    @currenteN.setter
    def currenteN(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportRele._CURRENTEN, value, signed=True)

    @property
    def voltA(self):
        return self._data.get_byte(BTYPE.BYTE32, DReportRele._VOLTA, signed=True)

    @voltA.setter
    def voltA(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReportRele._VOLTA, value, signed=True)

    @property
    def voltB(self):
        return self._data.get_byte(BTYPE.BYTE32, DReportRele._VOLTB, signed=True)

    @voltB.setter
    def voltB(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReportRele._VOLTB, value, signed=True)

    @property
    def voltC(self):
        return self._data.get_byte(BTYPE.BYTE32, DReportRele._VOLTC, signed=True)

    @voltC.setter
    def voltC(self, value):
        self._data.set_byte(BTYPE.BYTE32, DReportRele._VOLTC, value, signed=True)

    @property
    def frequency(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportRele._FREQUENCY) / 100

    @frequency.setter
    def frequency(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportRele._FREQUENCY, int(value * 100))

    @property
    def factor(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportRele._FACTOR, signed=True) / 100

    @factor.setter
    def factor(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportRele._FACTOR, int(value * 100), signed=True)

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
class DReportReleCorrente(object):
    _SIZE = 8
    _CURRENTEA = 0
    _CURRENTEB = 2
    _CURRENTEC = 4
    _CURRENTEN = 6

    def __init__(self, data=None):
        if not data:
            self._data = common.RData(bytearray(DReportReleCorrente._SIZE))
        elif len(data) < DReportReleCorrente._SIZE:
            self._data = common.RData(data)
            for x in range(len(data), DReportReleCorrente._SIZE):
                self._data.append(0)
        else:
            self._data = common.RData(data[:DReportReleCorrente._SIZE])

    @property
    def currenteA(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportReleCorrente._CURRENTEA, signed=True)

    @currenteA.setter
    def currenteA(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportReleCorrente._CURRENTEA, value, signed=True)

    @property
    def currenteB(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportReleCorrente._CURRENTEB, signed=True)

    @currenteB.setter
    def currenteB(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportReleCorrente._CURRENTEB, value, signed=True)

    @property
    def currenteC(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportReleCorrente._CURRENTEC, signed=True)

    @currenteC.setter
    def currenteC(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportReleCorrente._CURRENTEC, value, signed=True)

    @property
    def currenteN(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportReleCorrente._CURRENTEN, signed=True)

    @currenteN.setter
    def currenteN(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportReleCorrente._CURRENTEN, value, signed=True)

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
class DAlarmRele(object):
    def __init__(self, size: int, data=None, offset=0):
        if not data:
            self._data = common.RData(bytearray(size))
        elif len(data) < size:
            self._data = common.RData(data)
            for x in range(len(data), size):
                self._data.append(0)
        else:
            self._data = common.RData(data[:size])
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
class DReportCodi(DAlarmRele):
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
        super().__init__(DReportCodi._SIZE, data)
        self._offset = 0
        if not data:
            self._data = common.RData(bytearray(DReportCodi._SIZE))
        elif len(data) < DReportCodi._SIZE:
            self._data = common.RData(data)
            for x in range(len(data), DReportCodi._SIZE):
                self._data.append(0)
        else:
            self._data = common.RData(data[:DReportCodi._SIZE])

    @property
    def tarifacao_indutiva(self) -> bool:
        return self._data.is_bit(DReportCodi._flag_tarifacao_indutiva, self._offset)

    @tarifacao_indutiva.setter
    def tarifacao_indutiva(self, flag):
        self._data.put_bit(DReportCodi._flag_tarifacao_indutiva, flag, self._offset)

    @property
    def tarifacao_capacitiva(self) -> bool:
        return self._data.is_bit(DReportCodi._flag_tarifacao_capacitiva, self._offset)

    @tarifacao_capacitiva.setter
    def tarifacao_capacitiva(self, flag):
        self._data.put_bit(DReportCodi._flag_tarifacao_capacitiva, flag, self._offset)

    @property
    def fim_intervalo_reativo(self) -> bool:
        return self._data.is_bit(DReportCodi._flag_fim_intervalo_reativo, self._offset)

    @fim_intervalo_reativo.setter
    def fim_intervalo_reativo(self, flag):
        self._data.put_bit(DReportCodi._flag_fim_intervalo_reativo, flag, self._offset)

    @property
    def reposicao_demanda(self) -> bool:
        return self._data.is_bit(DReportCodi._flag_reposicao_demanda, self._offset)

    @reposicao_demanda.setter
    def reposicao_demanda(self, flag):
        self._data.put_bit(DReportCodi._flag_reposicao_demanda, flag, self._offset)

    @property
    def tarifa_reativa(self) -> bool:
        return self._data.is_bit(DReportCodi._flag_tarifa_reativa, self._offset)

    @tarifa_reativa.setter
    def tarifa_reativa(self, flag):
        self._data.put_bit(DReportCodi._flag_tarifa_reativa, flag, self._offset)

    @property
    def numero_segundos(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportCodi._NS)

    @numero_segundos.setter
    def numero_segundos(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportCodi._NS, value)

    @property
    def tipo_tarifa(self):
        return self._data.get_byte(BTYPE.BYTE8, DReportCodi._TP)

    @tipo_tarifa.setter
    def tipo_tarifa(self, value):
        self._data.set_byte(BTYPE.BYTE8, DReportCodi._TP, value)

    @property
    def posto_horario(self):
        return self._data.get_byte(BTYPE.BYTE8, DReportCodi._PH)

    @posto_horario.setter
    def posto_horario(self, value):
        self._data.set_byte(BTYPE.BYTE8, DReportCodi._PH, value)

    @property
    def pulsos_ativos(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportCodi._PA)

    @pulsos_ativos.setter
    def pulsos_ativos(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportCodi._PA, value)

    @property
    def pulsos_reativos(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportCodi._PR)

    @pulsos_reativos.setter
    def pulsos_reativos(self, value):
        self._data.set_byte(BTYPE.BYTE16, DReportCodi._PR, value)

    @property
    def potencia_ativa(self):
        return self._data.get_byte(BTYPE.BYTE32, DReportCodi._PTA)

    @potencia_ativa.setter
    def potencia_ativa(self, value: float):
        self._data.set_byte(BTYPE.BYTE32, DReportCodi._PTA, int(round(value * DReportCodi.__FATOR, 0)))

    @property
    def potencia_reativa(self):
        return self._data.get_byte(BTYPE.BYTE32, DReportCodi._PTR)

    @potencia_reativa.setter
    def potencia_reativa(self, value: float):
        self._data.set_byte(BTYPE.BYTE32, DReportCodi._PTR, int(round(value * DReportCodi.__FATOR, 0)))

    @property
    def potencia_aparente(self):
        return self._data.get_byte(BTYPE.BYTE32, DReportCodi._PTAP)

    @potencia_aparente.setter
    def potencia_aparente(self, value: float):
        self._data.set_byte(BTYPE.BYTE32, DReportCodi._PTAP, int(round(value * DReportCodi.__FATOR, 0)))

    @property
    def fator_potencia(self):
        return self._data.get_byte(BTYPE.BYTE16, DReportCodi._FPT)

    @fator_potencia.setter
    def fator_potencia(self, value: float):
        self._data.set_byte(BTYPE.BYTE32, DReportCodi._FPT, int(round(value * DReportCodi.__FATOR, 0)))

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
            self.potencia_ativa / DReportCodi.__FATOR,
            self.potencia_reativa / DReportCodi.__FATOR,
            self.potencia_aparente / DReportCodi.__FATOR,
            self.fator_potencia / DReportCodi.__FATOR)
