#!/usr/bin/python3
# -*- coding: utf-8 -*-

import enum
import datetime
from rlib.common import CONST

CONST.ERROR_LEVEL = 40
CONST.JOIN_TIMEOUT = 60
CONST.APPNAME = 'DWeb'
CONST.MAIN = 'Main'
CONST.REGISTER = 'Register'
CONST.INTERVAL = 'Interval'
CONST.SLAVES = 'Slaves'
CONST.SLAVE = 'Slave'
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
CONST.LOCALSERVER = 'LocalServer'
CONST.IFACE = 'Iface'
CONST.BACKUPTIME = 'BackupTime'
CONST.BACKUPCOUNT = 'BackupCount'
CONST.DBFILE = 'Dbfile'
CONST.STACK = 'Stack'
CONST.IDFILE = 'IdFile'
CONST.MODELID = 'ModelID'
CONST.MODBUS_SLAVE = 'Modbus_Slave'
CONST.LOCAL = 'Local'
CONST.DESC = 'Desc'
CONST.PERIODIC = 'Periodic'
CONST.TOLERANCE = 'Tolerance'
CONST.TEMP = 'Temp'
CONST.CURRENT = 'Current'
CONST.NEUTRO = 'Neutro'
CONST.VOLT = 'Volt'
CONST.FACTOR = 'Factor'
CONST.FREQUENCY = 'Frequency'
CONST.AD = 'AD'
CONST.GPI_AC = 'GPI_AC'
CONST.REPORT = 'Report'
CONST.ALARM = 'Alarm'
CONST.EVENT = 'Event'
CONST.PRIORITY = 'Priority'
CONST.GPIOTYPE = 'GpioType'
CONST.PINBATTERY = 'PinBattery'
CONST.PINRESETGSM = 'PinResetGSM'
CONST.RESETGSMTIME = 'ResetGSMTime'
CONST.DISCONNECT_TIME = 'Disconnect_Time'
CONST.PINRESETATMEGA = 'PinResetAtmega'
CONST.PINLOCK = 'PinLock'
CONST.PINLOCKLED = 'PinLockLed'
CONST.I2C_ATMEGA = 'I2C_Atmega'
CONST.I2C_ATMEGA_ADDR = 'I2C_Atmega_Address'
CONST.I2C_DISPLAY = 'I2C_Display'
CONST.I2C_DISPLAY_ADDR = 'I2C_Display_Address'
CONST.BATTERYMONITOR = 'BatteryMonitor'
CONST.LOCKENABLE = 'LockEnable'
CONST.CODI = 'Codi'
CONST.WATCHDOG_TIME = 'WatchDog_Time'

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

# Lista de dispositivos por modelid que sao devices
DEVICES_CATALOG = (
    0,  # padrao
    3,  # unilojas_c001
)


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
        self._path = str()
        self._args = ()
        self._reset = DResetTypes.NO_RESET

    @property
    def reset(self) -> DResetTypes:
        return self._reset

    @reset.setter
    def reset(self, value: DResetTypes):
        self._reset = value

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, value: str):
        self._path = value

    @property
    def args(self):
        return self._args

    @args.setter
    def args(self, value):
        self._args = value


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
