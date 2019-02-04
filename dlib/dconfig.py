#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging.config
from pathlib import Path
import rlib.common as common
from rlib.common import RConfigParms, RConfig, RConfigError, CONST


# =============================================================================#
class DConfig_Main(RConfigParms):
    def __init__(self, rconfig: RConfig):
        super().__init__(CONST.MAIN, rconfig)

    @property
    def version(self) -> str:
        return self.config.get(self._section, CONST.VERSION)

    @version.setter
    def version(self, value):
        self.config.set(self._section, CONST.VERSION, value)

    @property
    def register(self) -> bool:
        return self.config.getboolean(self._section, CONST.REGISTER, fallback=False)

    @register.setter
    def register(self, value):
        self.config.set(self._section, CONST.REGISTER, value)

    @property
    def interval(self) -> int:
        return self.config.getint(self._section, CONST.INTERVAL, fallback=1)

    @interval.setter
    def interval(self, value):
        self.config.set(self._section, CONST.INTERVAL, value)

    @property
    def slaves(self) -> int:
        return self.config.getint(self._section, CONST.SLAVES)

    @slaves.setter
    def slaves(self, value):
        self.config.set(self._section, CONST.SLAVES, value)

    @property
    def host(self) -> str:
        return self.config.get(self._section, CONST.HOST)

    @host.setter
    def host(self, value):
        self.config.set(self._section, CONST.HOST, value)

    @property
    def gpiotype(self) -> int:
        return self.config.getint(self._section, CONST.GPIOTYPE, fallback=12)

    @gpiotype.setter
    def gpiotype(self, value):
        self.config.set(self._section, CONST.GPIOTYPE, value)

    @property
    def logconfig(self) -> str:
        return self.config.get(self._section, CONST.LOGCONFIG)

    @logconfig.setter
    def logconfig(self, value):
        self.config.set(self._section, CONST.LOGCONFIG, value)

    @property
    def idfile(self) -> str:
        return self.config.get(self._section, CONST.IDFILE)

    @idfile.setter
    def idfile(self, value):
        self.config.conf.set(self._section, CONST.IDFILE, value)

    @property
    def pidfile(self) -> str:
        if self.config.has_option(CONST.PIDFILE):
            return self.config.get(self._section, CONST.PIDFILE)
        else:
            raise RConfigError(CONST.PIDFILE)

    @pidfile.setter
    def pidfile(self, value):
        self.config.set(self._section, CONST.PIDFILE, value)


# =============================================================================#
class DConfig_Listen(RConfigParms):
    def __init__(self, rconfig: RConfig, server_section):
        self._server_section = server_section
        super().__init__(server_section, rconfig, CONST.MAIN)

    @property
    def server_section(self):
        return self._server_section

    @property
    def iface(self) -> str:
        if self.config.has_option(self._section, CONST.IFACE):
            return self.config.get(self._section, CONST.IFACE)
        elif len(self._main_section) != 0 and self.config.has_option(self._main_section, CONST.IFACE):
            return self.config.get(self._main_section, CONST.IFACE)
        else:
            raise RConfigError(CONST.IFACE)

    @iface.setter
    def iface(self, value):
        self.config.set(self._section, CONST.IFACE, value)

    @property
    def port(self) -> int:
        return self.config.getint(self._section, CONST.PORT, fallback=9094)

    @port.setter
    def port(self, value):
        self.config.set(self._section, CONST.PORT, value)

    @property
    def timeout(self) -> int:
        return self.config.getint(self._section, CONST.TIMEOUT, fallback=60)

    @timeout.setter
    def timeout(self, value):
        self.config.set(self._section, CONST.TIMEOUT, value)

    @property
    def sockettimeout(self) -> int:
        return self.config.getint(self._section, CONST.SOCKETTIMEOUT, fallback=60)

    @sockettimeout.setter
    def sockettimeout(self, value):
        self.config.set(self._section, CONST.SOCKETTIMEOUT, value)


# =============================================================================#
class DConfig_Alive(RConfigParms):
    def __init__(self, rconfig: RConfig):
        super().__init__(CONST.ALIVE, rconfig, CONST.MAIN)

    @property
    def host(self) -> str:
        if self.config.has_option(self._section, CONST.HOST):
            return self.config.get(self._section, CONST.HOST)
        elif len(self._main_section) != 0 and self.config.has_option(self._main_section, CONST.HOST):
            return self.config.get(self._main_section, CONST.HOST)
        else:
            raise RConfigError(CONST.HOST)

    @host.setter
    def host(self, value):
        self.config.set(self._section, CONST.PORT, value)

    @property
    def port(self) -> int:
        return self.config.getint(self._section, CONST.PORT, fallback=9093)

    @port.setter
    def port(self, value):
        self.config.set(self._section, CONST.PORT, value)

    @property
    def iface(self) -> str:
        if self.config.has_option(self._section, CONST.IFACE):
            return self.config.get(self._section, CONST.IFACE)
        elif len(self._main_section) != 0 and self.config.has_option(self._main_section, CONST.IFACE):
            return self.config.get(self._main_section, CONST.IFACE)
        else:
            raise RConfigError(CONST.IFACE)

    @iface.setter
    def iface(self, value):
        self.config.set(self._section, CONST.IFACE, value)

    @property
    def timeout(self) -> int:
        return self.config.getint(self._section, CONST.TIMEOUT, fallback=30)

    @timeout.setter
    def timeout(self, value):
        self.config.set(self._section, CONST.TIMEOUT, value)

    @property
    def interval(self) -> int:
        return self.config.getint(self._section, CONST.INTERVAL, fallback=900)

    @interval.setter
    def interval(self, value):
        self.config.set(self._section, CONST.INTERVAL, value)


# =============================================================================#
class DConfig_Stack(RConfigParms):
    def __init__(self, rconfig: RConfig):
        super().__init__(CONST.STACK, rconfig, CONST.MAIN)

    @property
    def host(self) -> str:
        if self.config.has_option(self._section, CONST.HOST):
            return self.config.get(self._section, CONST.HOST)
        elif len(self._main_section) != 0 and self.config.has_option(self._main_section, CONST.HOST):
            return self.config.get(self._main_section, CONST.HOST)
        else:
            raise RConfigError(CONST.HOST)

    @host.setter
    def host(self, value):
        self.config.set(self._section, CONST.PORT, value)

    @property
    def port(self) -> int:
        return self.config.getint(self._section, CONST.PORT, fallback=9091)

    @port.setter
    def port(self, value):
        self.config.set(self._section, CONST.PORT, value)

    @property
    def timeout(self) -> int:
        return self.config.getint(self._section, CONST.TIMEOUT, fallback=30)

    @timeout.setter
    def timeout(self, value):
        self.config.set(self._section, CONST.TIMEOUT, value)

    @property
    def interval(self) -> int:
        return self.config.getint(self._section, CONST.INTERVAL, fallback=1000)

    @interval.setter
    def interval(self, value):
        self.config.set(self._section, CONST.INTERVAL, value)

    @property
    def backuptime(self) -> int:
        return self.config.getint(self._section, CONST.BACKUPTIME, fallback=60)

    @backuptime.setter
    def backuptime(self, value):
        self.config.set(self._section, CONST.BACKUPTIME, value)

    @property
    def backupcount(self) -> int:
        return self.config.getint(self._section, CONST.BACKUPCOUNT, fallback=10)

    @backupcount.setter
    def backupcount(self, value):
        self.config.set(self._section, CONST.BACKUPCOUNT, value)

    @property
    def dbfile(self) -> str:
        return self.config.get(self._section, CONST.DBFILE, fallback='stack.db')

    @dbfile.setter
    def dbfile(self, value):
        self.config.set(self._section, CONST.DBFILE, value)


# =============================================================================#
class DConfig_Slave(RConfigParms):
    def __init__(self, rconfig: RConfig, slave_num: int):
        self._slave_name = '{}:{}'.format(CONST.SLAVE, slave_num)
        if not rconfig.config.has_section(self._slave_name):
            raise ValueError(self._slave_name)
        super().__init__(self._slave_name, rconfig, CONST.MAIN)
        self._slave_num = slave_num

    @property
    def slave_num(self) -> int:
        return self._slave_num

    @property
    def slave_name(self) -> str:
        return self._slave_name

    @property
    def local(self) -> str:
        return self.config.get(self.slave_name, CONST.LOCAL)

    @local.setter
    def local(self, value: str):
        self.config.set(self.slave_name, CONST.LOCAL, value)

    @property
    def desc(self) -> str:
        return self.config.get(self.slave_name, CONST.DESC)

    @desc.setter
    def desc(self, value: str):
        self.config.set(self.slave_name, CONST.DESC, value)

    @property
    def modelid(self) -> int:
        return self.config.getint(self.slave_name, CONST.MODELID)

    def get_alarm(self, slave_name, name) -> []:
        return self.get_tolerance(slave_name, name)

    def get_tolerance(self, slave_name, name) -> []:
        if self.config.has_option(slave_name, name):
            value = [x.strip() for x in self.config.get(slave_name, name).split(',')]
            if len(value) != 3:
                raise ValueError(name)
            ret = []
            ret.append(float(value[0]))
            ret.append(float(value[1]))
            ret.append(common.string2bool(value[2]))
            return ret
        else:
            return None

    def get_range(self, slave_name, name) -> []:
        if self.config.has_option(slave_name, name):
            value = [x.strip() for x in self.config.get(slave_name, name).split(',')]
            if len(value) != 3:
                raise ValueError(name)
            ret = []
            ret.append(float(value[0]))
            ret.append(float(value[1]))
            ret.append(float(value[2]))
            return ret
        else:
            return None


# =============================================================================#
class DConfig_Slave_Reles(DConfig_Slave):
    def __init__(self, rconfig: RConfig, slave_num: int):
        super().__init__(rconfig, slave_num)

    @property
    def modbus_slave(self) -> int:
        return self.config.getint(self.slave_name, CONST.MODBUS_SLAVE)

    @property
    def interval(self) -> float:
        return self.config.getfloat(self.slave_name, CONST.INTERVAL, fallback=1000) / 1000

    @property
    def periodic(self) -> int:
        return self.config.getint(self.slave_name, CONST.PERIODIC, fallback=0) * 60

    @property
    def tolerance_current(self) -> []:
        return self.get_tolerance(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.CURRENT))

    @property
    def tolerance_neutro(self) -> []:
        return self.get_tolerance(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.NEUTRO))

    @property
    def tolerance_voltage(self) -> []:
        return self.get_tolerance(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.VOLT))

    @property
    def tolerance_factor(self) -> []:
        return self.get_tolerance(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.FACTOR))

    @property
    def tolerance_frequency(self) -> []:
        return self.get_tolerance(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.FREQUENCY))

    @property
    def alarm_current(self) -> []:
        return self.get_alarm(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.CURRENT))

    @property
    def alarm_neutro(self) -> []:
        return self.get_alarm(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.NEUTRO))

    @property
    def alarm_voltage(self) -> []:
        return self.get_alarm(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.VOLT))

    @property
    def alarm_factor(self) -> []:
        return self.get_alarm(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.FACTOR))

    @property
    def alarm_frequency(self) -> []:
        return self.get_alarm(self.slave_name, '{}_{}'.format(CONST.TOLERANCE, CONST.FREQUENCY))

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
class DConfig(object):
    @property
    def config(self):
        return self.rconfig.config

    def load(self, rconfig: RConfig, path, args, alternative_path='/etc/dweb'):
        self.rconfig = rconfig
        # load config
        if len(args.config) != 0:
            p = Path(args.config)
            if p.is_file():
                if len(p.parents) > 0:
                    path = str(p.parents[0])
                else:
                    path = str()
                self.rconfig.read(p.name, path)
            else:
                raise FileNotFoundError('file:{}'.format(args.config))
        else:
            self.rconfig.read('{}.ini'.format(CONST.APPNAME).lower(), (path, alternative_path))
        self._load_sections()

    def load_logger(self):
        if self.config.has_option(CONST.MAIN, CONST.LOGCONFIG):
            logfile = str(Path(self.rconfig.path_config.parents[0], self.config.get(CONST.MAIN, CONST.LOGCONFIG)))
            logging.config.fileConfig(logfile, disable_existing_loggers=False)

    def is_local_server(self) -> bool:
        return self.rconfig.config.has_section(CONST.LOCALSERVER)

    def _load_sections(self):
        self.main = DConfig_Main(self.rconfig)
        self.alive = DConfig_Alive(self.rconfig)
        self.stack = DConfig_Stack(self.rconfig)
        self.listen = DConfig_Listen(self.rconfig, CONST.SERVER)
        self.local_listen = DConfig_Listen(self.rconfig, CONST.LOCALSERVER)
        self.slaves = {}


# =============================================================================#
CONFIG = DConfig()
