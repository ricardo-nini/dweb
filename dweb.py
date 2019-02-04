#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import threading
import time
import os
import sys
import subprocess
import traceback
import logging
import rlib.rdaemon as rdaemon
import rlib.rgpio as GPIO
import rlib.rmodbus as rmodbus
import dlib.dalive as dalive
import dlib.dstack as dstack
import dlib.dlisten as dlisten
from rlib.common import CONST, RData
from dlib.dcommon import DResetTypes, GLOBAL
from dlib.dstatus import STATUS
from dlib.dconfig import RConfig, CONFIG, DConfig_Slave
import dlib.dsocket as dsocket

# add import devices here
import dlib.devices.unilojas.c001 as unilojas_c001
import dlib.devices.pextron.urp1439tu as pextron_urp1439tu
import dlib.devices.pextron.urpe7104_v7_18 as pextron_urpe7104_v7_18
import dlib.devices.schneider.sepam40 as schneider_sepam40

__version__ = '1.0.0'


# =============================================================================#
class DModbusComm(rmodbus.RModbusComm):
    threadLock = threading.Lock()

    def __init__(self, parms: rmodbus.RModbusParms):
        super().__init__(parms)
        self.logger = logging.getLogger(__name__)

    def exchange(self, send: rmodbus.RModbusMessage) -> rmodbus.RModbusMessage:
        DModbusComm.threadLock.acquire()
        try:
            if STATUS.is_state(CONST.STATE_MODBUS) and all(x == False for x in GLOBAL.modbus.values()):
                self.logger.debug('Tentei abrir')
                if self.is_open():
                    self.close()
                self.open()
            r = super().exchange(send)
            if STATUS.is_state(CONST.STATE_MODBUS) and all(x == True for x in GLOBAL.modbus.values()):
                STATUS.toggle_state(CONST.STATE_MODBUS)
            return r
        except Exception as err:
            if not STATUS.is_state(CONST.STATE_MODBUS) and STATUS.is_state(CONST.STATE_REGISTER):
                STATUS.toggle_state(CONST.STATE_MODBUS)
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
        self.logger = logging.getLogger(__name__)
        self._jobs_started = False
        self._jobs = {}
        self._stop_event = threading.Event()

    def run(self, *args):
        # get time boot
        time_boot = int(time.time())

        # create global control object
        GLOBAL.boot = True
        GLOBAL.reset = DResetTypes.NO_RESET

        try:
            self.logger.info('Iniciando DWeb -> {}'.format(CONFIG.main.str()))

            # load slaves
            self.load_slaves()

            # init status object
            STATUS.clear()

            # le arquivo .id
            STATUS.read_id_from_file(str(Path(CONFIG.rconfig.path_config.parents[0], CONFIG.main.idfile)))

            # set register state
            if CONFIG.main.register:
                STATUS.set_state(CONST.STATE_REGISTER)
            else:
                STATUS.clear_state(CONST.STATE_REGISTER)

            # init GPIOs (trigger exception if config error)
            GPIO.setwarnings(False)
            GPIO.setmode(CONFIG.main.gpiotype)

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
            self.stack = dstack.DStack()
            self.stack.start()
            time.sleep(2)  # wait for stack start ...

            # create resources dict
            self.resources['modbus'] = self.modbus
            self.resources['stack'] = self.stack

            # inicia server
            self.server = dlisten.DListen(self.resources, CONFIG.listen)
            self.resources['server'] = self.server
            self.server.start()

            # inicia local server (se configurado)
            if CONFIG.is_local_server():
                self.local_server = dlisten.DListen(self.resources, CONFIG.local_listen)
                self.resources['server'] = self.local_server
                self.local_server.start()
            else:
                self.local_server = None

            # inicia codi



            # main loop
            while self.is_running():

                # only do if registered
                if STATUS.is_state(CONST.STATE_REGISTER):
                    # send boot if true
                    if GLOBAL.boot:
                        self.send_boot(time_boot)
                        GLOBAL.boot = False
                    # create jobs at first time ...
                    if not self._jobs_started:
                        self.start_jobs()
                    # if setup changed, send message setup
                    if GLOBAL.setup:
                        self.send_setup(int(time.time()))
                        GLOBAL.setup = False
                else:
                    # stop jobs if exist
                    self.stop_jobs()

                self.wait_time(60)
                break
                # interval wait

            # check if restart
            self.soft_reset = GLOBAL.reset == DResetTypes.SOFT_RESET

            self.logger.info('Terminando com reset:{}'.format(GLOBAL.reset))

            # stop jobs if exist
            self.stop_jobs()

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

    def sigterm(self, signum, frame):
        self.logger.info('Recebido signal {}.'.format(signum))
        self.stop_main()

    def exit(self):
        super().exit()

    def load_slaves(self):
        # fill slaves
        for x in range(0, CONFIG.main.slaves + 1):
            s = DConfig_Slave(CONFIG.rconfig, x)
            if s.modelid == pextron_urp1439tu.ID['modelid']:
                rs = pextron_urp1439tu.DPextron_URP1439TU_Config(CONFIG.rconfig, x)
            elif s.modelid == schneider_sepam40.ID['modelid']:
                rs = schneider_sepam40.DSchneider_SEPAM40_Config(CONFIG.rconfig, x)
            elif s.modelid == unilojas_c001.ID['modelid']:
                rs = unilojas_c001.DUnilojas_C001_Config(CONFIG.rconfig, x)
            elif s.modelid == pextron_urpe7104_v7_18.ID['modelid']:
                rs = pextron_urpe7104_v7_18.DPextron_URPE7104_V7_18_Config(CONFIG.rconfig, x)
            else:
                raise ValueError('{} {}'.format(CONST.ERR_INVALID, CONST.MODELID))
            CONFIG.slaves[x] = rs

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def wait_time(self, timeout):
        self._stop_event.wait(timeout)

    def stop_main(self):
        self._stop_event.set()

    def stop_services(self):
        if self.local_server:
            self.local_server.stop()
        self.server.stop()
        self.stack.stop()
        self.alive.stop()
        if self.local_server:
            self.local_server.join(CONST.JOIN_TIMEOUT)
        self.server.join(CONST.JOIN_TIMEOUT)
        self.stack.join(CONST.JOIN_TIMEOUT)
        self.alive.join(CONST.JOIN_TIMEOUT)

    def start_jobs(self):
        for x in range(0, CONFIG.main.slaves + 1):
            modelid = CONFIG.slaves[x].modelid
            if modelid is not None:
                try:
                    if modelid == pextron_urp1439tu.ID['modelid']:
                        self._jobs[x] = pextron_urp1439tu.DPextron_URP1439TU_Thread(x, self.resources)
                        self._jobs[x].start()
                    elif modelid == schneider_sepam40.ID['modelid']:
                        self._jobs[x] = schneider_sepam40.DSchneider_SEPAM40_Thread(x, self.resources)
                        self._jobs[x].start()
                    elif modelid == unilojas_c001.ID['modelid']:
                        self._jobs[x] = unilojas_c001.DUnilojas_C001_Thread(x, self.resources)
                        self._jobs[x].start()
                    elif modelid == pextron_urpe7104_v7_18.ID['modelid']:
                        self._jobs[x] = pextron_urpe7104_v7_18.DPextron_URPE7104_V7_18_Thread(x, self.resources)
                        self._jobs[x].start()
                except Exception as err:
                    self.logger.info('Erro "{}" iniciando job "{}"'.format(str(err), CONFIG.slaves[x].local))
        self._jobs_started = True

    def stop_jobs(self):
        if len(self._jobs) != 0:
            for x in self._jobs.values():
                x.stop()
            for x in self._jobs.values():
                x.join()
            self._jobs.clear()
        self._jobs_started = False

    def send_boot(self, time_boot: int):
        h = dsocket.DSocketHeader_Boot.create(STATUS.id, time_boot)
        self.stack.put(0, time.time(), h)

    def send_setup(self, time_setup: int):
        h = dsocket.DSocketHeader_Setup.create(STATUS.id, time_setup)
        try:
            r = RData.read_from_file(CONST.APPNAME + '.ini')
            self.stack.put(0, time.time(), h, r)
        except Exception as err:
            self.logger.debug('Erro lendo configuracao {}'.format(str(err)))

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
    GLOBAL.path = os.path.dirname(os.path.abspath(sys.argv[0]))

    # take args to GLOBAL
    GLOBAL.args = args

    # create and load config
    rconfig = RConfig()
    CONFIG.load(rconfig, GLOBAL.path, GLOBAL.args)

    # load logger
    CONFIG.load_logger()

    # get pidfile (rais RConfigError)
    pidfile = CONFIG.config.get(CONST.MAIN, CONST.PIDFILE)

    # start dweb class
    if args.debug:
        d = DWeb(app='dweb', pid=pidfile, foreground=True, chdir=GLOBAL.path)
        d.start()
    elif args.start:
        d = DWeb(app='dweb', pid=pidfile, foreground=False, chdir=GLOBAL.path)
        getattr(d, 'start')()
    else:
        d = DWeb(app='dweb', pid=pidfile, foreground=False, chdir=GLOBAL.path)
        getattr(d, 'stop')()
