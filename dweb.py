#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import threading
import time
import os
import sys
import logging
import subprocess
import traceback
import logging.config
import rlib.rdaemon as rdaemon
import rlib.rgpio as GPIO
import rlib.rmodbus as rmodbus
import dlib.dalive as dalive
import dlib.dstack as dstack
import dlib.dlisten as dlisten
from rlib.common import CONST, RConfigError, RConfigParms
from dlib.dcommon import DResetTypes, CONFIG, GLOBAL
from dlib.dstatus import STATUS

__version__ = '1.0.0'


# =============================================================================#
class DWebConfig(RConfigParms):
    @property
    def version(self) -> str:
        return self._config.conf.get(self._section, CONST.VERSION)

    @version.setter
    def version(self, value):
        self._config.conf.set(self._section, CONST.VERSION, value)

    @property
    def register(self) -> bool:
        return self._config.conf.getboolean(self._section, CONST.REGISTER, fallback=False)

    @register.setter
    def register(self, value):
        self._config.conf.set(self._section, CONST.REGISTER, value)

    @property
    def interval(self) -> int:
        return self._config.conf.getint(self._section, CONST.INTERVAL, fallback=1)

    @interval.setter
    def interval(self, value):
        self._config.conf.set(self._section, CONST.INTERVAL, value)

    @property
    def slaves(self) -> int:
        return self._config.conf.getint(self._section, CONST.SLAVES)

    @slaves.setter
    def slaves(self, value):
        self._config.conf.set(self._section, CONST.SLAVES, value)

    @property
    def host(self) -> str:
        return self._config.conf.get(self._section, CONST.HOST)

    @host.setter
    def host(self, value):
        self._config.conf.set(self._section, CONST.HOST, value)

    @property
    def gpiotype(self) -> int:
        return self._config.conf.getint(self._section, CONST.GPIOTYPE, fallback=12)

    @gpiotype.setter
    def gpiotype(self, value):
        self._config.conf.set(self._section, CONST.GPIOTYPE, value)

    @property
    def logconfig(self) -> str:
        return self._config.conf.get(self._section, CONST.LOGCONFIG)

    @logconfig.setter
    def logconfig(self, value):
        self._config.conf.set(self._section, CONST.LOGCONFIG, value)

    @property
    def idfile(self) -> str:
        return self._config.conf.get(self._section, CONST.IDFILE)

    @idfile.setter
    def idfile(self, value):
        self._config.conf.set(self._section, CONST.IDFILE, value)

    @property
    def pidfile(self) -> str:
        return self._config.conf.get(self._section, CONST.PIDFILE)

    @pidfile.setter
    def pidfile(self, value):
        self._config.conf.set(self._section, CONST.PIDFILE, value)


# =============================================================================#
class DModbusComm(rmodbus.RModbusComm):
    threadLock = threading.Lock()

    def __init__(self, parms: rmodbus.RModbusParms):
        super().__init__(parms)
        self.logger = logging.getLogger(__name__)

    def exchange(self, send: rmodbus.RModbusMessage) -> rmodbus.RModbusMessage:
        DModbusComm.threadLock.acquire()
        try:
            if STATUS.is_state(CONST.DCONST.STATE_MODBUS) and all(x == False for x in GLOBAL.modbus.values()):
                self.logger.debug('Tentei abrir')
                if self.is_open():
                    self.close()
                self.open()
            r = super().exchange(send)
            if STATUS.is_state(CONST.DCONST.STATE_MODBUS) and all(x == True for x in GLOBAL.modbus.values()):
                STATUS.toggle_state(CONST.DCONST.STATE_MODBUS)
            return r
        except Exception as err:
            if not STATUS.is_state(CONST.DCONST.STATE_MODBUS) and STATUS.is_state(CONST.DCONST.STATE_REGISTER):
                STATUS.toggle_state(CONST.DCONST.STATE_MODBUS)
            self.logger.debug('Erro no modbus: {}'.format(err))
            raise
        finally:
            DModbusComm.threadLock.release()


# =============================================================================#
class DWeb(rdaemon.Daemonize):
    lock = threading.Lock()

    def __init__(self, app, pid, privileged_action=None,
                 user=None, group=None, verbose=False, logger=None,
                 foreground=False, chdir="/"):
        super().__init__(app, pid, privileged_action, user, group, verbose, logger,
                         foreground, chdir)
        self.resources = {}
        self.config = DWebConfig(CONST.MAIN, CONFIG)
        self.logger = logging.getLogger(__name__)
        self._stop_event = threading.Event()

    def run(self, *args):
        # create global control object
        GLOBAL.boot = True
        GLOBAL.reset = DResetTypes.NO_RESET

        try:
            self.logger.info('Iniciando DWeb -> {}'.format(self.config.str()))

            # init status object
            STATUS.clear()

            # le arquivo .id
            STATUS.read_id_from_file(str(Path(CONFIG.p0.parents[0], self.config.idfile)))

            # set register state
            if self.config.register:
                STATUS.set_state(CONST.STATE_REGISTER)
            else:
                STATUS.clear_state(CONST.STATE_REGISTER)

            # init GPIOs (trigger exception if config error)
            GPIO.setwarnings(False)
            GPIO.setmode(self.config.gpiotype)

            # inicia alive thread
            self.alive = dalive.DAlive()
            STATUS.send_alive_function = self.alive.send_alive_now
            self.alive.start()

            # inicia modbus
            config_modbus = rmodbus.RModbusConfig('Modbus', CONFIG)
            modbus_parms = config_modbus.read()
            self.logger.info(config_modbus.str())
            self.modbus = DModbusComm(modbus_parms)
            self.activate_modbus()

            # inicia stack
            self.stack = dstack.DSendStack()
            self.stack.start()
            time.sleep(2)  # wait for stack start ...

            # create resources dict
            self.resources['modbus'] = self.modbus
            self.resources['stack'] = self.stack

            # inicia server
            self.server = dlisten.DListen(self.resources)
            self.resources['server'] = self.server
            self.server.start()

            # main loop
            while self.is_running():
                self.wait_time(20)
                break
                # interval wait

            # check if restart
            self.soft_reset = GLOBAL.reset == DResetTypes.SOFT_RESET

            self.logger.info('Terminando com reset:{}'.format(GLOBAL.reset))

            # stop threads and wait ...
            self.stop_services()

            # deactivate modbus port
            self.deactivate_modbus()

            self.logger.info('Terminado !')

            if GLOBAL.reset == DResetTypes.HARD_RESET:
                self.logger.info('Reboot ...')
                subprocess.call(['shutdown', '-r', '-t', '1'])

        except Exception as e:
            if logging.getLogger().getEffectiveLevel() >= CONST.ERROR_LEVEL:
                self.logger.error(e)
            else:
                for line in traceback.format_exc().split("\n"):
                    self.logger.error(line)

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def wait_time(self, timeout):
        self._stop_event.wait(timeout)

    def stop_main(self):
        self._stop_event.set()

    def stop_services(self):
        self.server.stop()
        self.stack.stop()
        self.alive.stop()
        self.server.join(CONST.JOIN_TIMEOUT)
        self.stack.join(CONST.JOIN_TIMEOUT)
        self.alive.join(CONST.JOIN_TIMEOUT)

    def sigterm(self, signum, frame):
        self.logger.info('Recebido signal {}.'.format(signum))
        self.stop_main()

    def exit(self):
        super().exit()

    def activate_modbus(self):
        try:
            if self.modbus.is_open():
                self.modbus.close()
            self.modbus.open()
            STATUS.clear_state(CONST.STATE_MODBUS)
        except Exception as err:
            self.logger.error('Erro modbus: {}'.format(str(err)))
            STATUS.set_state(CONST.STATE_MODBUS)

    def deactivate_modbus(self):
        if self.modbus.is_open():
            self.modbus.close()


# =============================================================================#
if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog='DWeb Service', description='Eletric Cabin Manage and Monitoring System.')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {}'.format(__version__))
    parser.add_argument('--config', type=str, help='Config file.', default=str())
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--start', action='store_true', help='Start service.', default=False)
    action.add_argument('--stop', action='store_true', help='Stop service.', default=False)
    action.add_argument('--debug', action='store_true', help='Debug mode, foreground running.', default=False)
    args = parser.parse_args()

    # get path
    path = os.path.dirname(os.path.abspath(sys.argv[0]))

    # load config
    if len(args.config) != 0:
        p = Path(args.config)
        if p.is_file():
            if len(p.parents) > 0:
                path = str(p.parents[0])
            else:
                path = str()
            CONFIG.read(p.name, path)
        else:
            raise FileNotFoundError('file:{}'.format(args.config))
    else:
        CONFIG.read('{}.ini'.format(CONST.APPNAME).lower(), (path, '/etc/dweb'))

    # load logger
    if CONFIG.conf.has_option(CONST.MAIN, CONST.LOGCONFIG):
        logfile = str(Path(CONFIG._p0.parents[0], CONFIG.conf.get(CONST.MAIN, CONST.LOGCONFIG)))
        logging.config.fileConfig(logfile, disable_existing_loggers=False)

    # get pidfile
    if CONFIG.conf.has_option(CONST.MAIN, CONST.PIDFILE):
        pidfile = CONFIG.conf.get(CONST.MAIN, CONST.PIDFILE)
    else:
        raise RConfigError(CONST.PIDFILE)

    # start dweb class
    if args.debug:
        d = DWeb(app='dweb', pid=pidfile, foreground=True, chdir=path)
        d.start()
    elif args.start:
        d = DWeb(app='dweb', pid=pidfile, foreground=False, chdir=path)
        getattr(d, 'start')()
    else:
        d = DWeb(app='dweb', pid=pidfile, foreground=False, chdir=path)
        getattr(d, 'stop')()
