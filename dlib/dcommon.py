#!/usr/bin/python3
# -*- coding: utf-8 -*-

import enum
import datetime
import threading
import configparser
from rlib.common import CONST, RConfig

CONST.ERROR_LEVEL = 40
CONST.JOIN_TIMEOUT = 120
CONST.APPNAME = 'DWeb'
CONST.MAIN = 'Main'
CONST.REGISTER = 'Register'
CONST.INTERVAL = 'Interval'
CONST.SLAVES = 'Slaves'
CONST.HOST = 'Host'
CONST.GPIOTYPE = 'GpioType'
CONST.LOGCONFIG = 'LogConfig'
CONST.PIDFILE = 'PidFile'
CONST.GLOBAL = 'Global'
CONST.STATUS = 'Status'
CONST.CONFIG = 'Config'
CONST.TIMEOUT = 'Timeout'
CONST.SOCKETTIMEOUT = 'SocketTimeout'
CONST.PORT = 'Port'
CONST.ALIVE = 'Alive'
CONST.SERVER = 'Server'
CONST.IFACE = 'Iface'
CONST.BACKUPTIME = 'BackupTime'
CONST.BACKUPCOUNT = 'BackupCount'
CONST.DBFILE = 'Dbfile'
CONST.STACK = 'Stack'
CONST.IDFILE = 'IdFile'

# constants for use in state bit mask
CONST.STATE_LOCK = 7  # 32768 (Bloqueio local, em manutenção)
CONST.STATE_REGISTER = 8  # 1
CONST.STATE_MODBUS = 9  # 2
CONST.STATE_CODI = 10  # 4
CONST.STATE_BACKUP = 11  # 8
CONST.STATE_ON_BATERY = 12  # 16
CONST.STATE_LOAD_BATERY = 13  # 32
CONST.STATE_ERRO_IP_LISTEN = 14  # 64
CONST.STATE_PPP_NOT_CONNECTED = 15  # 128

# return codes
CONST.RETURNCODE_OK = 0  # Retorno ok
CONST.RETURNCODE_CMD_INVALID = 1000  # Comando invalido
CONST.RETURNCODE_WRITE_SETUP_ERR = 1001  # Erro gravando setup
CONST.RETURNCODE_NO_REGISTERED = 1002  # Sem registro
CONST.RETURNCODE_RECV_ERR = 1003  # Erro de recepcao
CONST.RETURNCODE_SIZE_ERR = 1004  # Tamanho errado recebido
CONST.RETURNCODE_CRC_ERR = 1005  # Erro de CRC
CONST.RETURNCODE_HEADER_INVALID = 1006  # Header invalido
CONST.RETURNCODE_LOCKED = 1007  # Device local locked
CONST.RETURNCODE_RELE_FAIL = 1008  # Rele fail

# modbus errors
CONST.RETURNCODE_MODBUS_ITEM_NOT_EXIST = 2000
CONST.RETURNCODE_MODBUS_NOT_APPLICABLE = 2001
CONST.RETURNCODE_MODBUS_INVALID_DATA = 2002
CONST.RETURNCODE_MODBUS_INVALID_CRC = 2003
CONST.RETURNCODE_MODBUS_INVALID_FUNCTION = 2004
CONST.RETURNCODE_MODBUS_INVALID_SIZE = 2005
CONST.RETURNCODE_MODBUS_INVALID_BYTECOUNT = 2006
CONST.RETURNCODE_MODBUS_NO_ANSWER = 2007
CONST.RETURNCODE_MODBUS_UNKNOWN = 2008
CONST.RETURNCODE_MODBUS_NOT_USEFUL = 2009
CONST.RETURNCODE_MODBUS_MESSAGE = 2010
CONST.RETURNCODE_UNKNOWN = 0xffff  # Erro desconhecido

CONFIG = RConfig(interpolation=configparser.ExtendedInterpolation())


# =============================================================================#
class DResetTypes(enum.Enum):
    NO_RESET = 0
    SOFT_RESET = 1
    HARD_RESET = 2


# =============================================================================#
class DWebGlobal(object):
    def __init__(self, boot=True, setup=False):
        self.modbus = {}
        self.boot = boot
        self.setup = setup
        self._reset = DResetTypes.NO_RESET

    @property
    def reset(self):
        return self._reset

    @reset.setter
    def reset(self, value: DResetTypes):
        self._reset = value


# =============================================================================#
GLOBAL = DWebGlobal()


# =============================================================================#
# Testa se esta no tempo demanda (0, 15, 30, 45 min)
def is_demanda_time(lock: bool) -> (bool, bool):
    t = datetime.datetime.time(datetime.datetime.now())
    if (t.minute == 0 or t.minute == 15 or t.minute == 30 or t.minute == 45) and not lock:
        return (True, True)  # trigger and lock
    elif (t.minute != 0 and t.minute != 15 and t.minute != 30 and t.minute != 45) and lock:
        return (False, False)  # unlock
    else:
        return (False, lock)  # no trigger


# =============================================================================#
# Testa se esta no tempo disconnect (5, 35)
def is_disconnect_time(lock: bool) -> (bool, bool):
    t = datetime.datetime.time(datetime.datetime.now())
    if (t.minute == 5 or t.minute == 35) and not lock:
        return (True, True)  # trigger and lock
    elif (t.minute != 5 and t.minute != 35) and lock:
        return (False, False)  # unlock
    else:
        return (False, lock)  # no trigger
