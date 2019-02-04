#!/usr/bin/python3
# -*- coding: utf-8 -*-

import threading
import logging
import socket
import time
from rlib.common import RData, get_ip_from_iface
import dlib.dsocket as dsocket
from dlib.dcommon import CONST, DEVICES_CATALOG
from dlib.dstatus import STATUS
from dlib.dconfig import DConfig_Listen, CONFIG
import dlib._dlisten as listen_function


# =============================================================================#
class DListen_Process(threading.Thread):
    def __init__(self, conn: socket, addr, resources: dict, sockettimeout):
        threading.Thread.__init__(self)
        self._conn = conn
        self._conn.settimeout(sockettimeout)
        self._addr = addr
        self._resources = resources
        self._stop_event = threading.Event()
        self.logger = logging.getLogger(__name__)
        self.logger.info('Connected to: {} : {}'.format(addr[0], addr[1]))

    def run(self):
        try:
            recv_header, recv_data = dsocket.dsocket_listen(self._conn)
            if not STATUS.is_state(CONST.STATE_REGISTER) and not self.is_register(
                    recv_header, recv_data):  # check register
                listen_function.replay_err(self._conn, CONST.RETURNCODE_NO_REGISTERED)
                self._conn.close()
            elif STATUS.is_state(CONST.STATE_LOCK):  # check lock
                listen_function.replay_err(self._conn, CONST.RETURNCODE_LOCKED)
                self._conn.close()
            else:
                # determina o modelid e cria classe correspondente
                # para acrescentar dispositivos novos adicione a lista abaixo
                if recv_header.slave == 0 and recv_header.modelid in DEVICES_CATALOG:  # slave:0
                    if CONFIG.slaves[0].modelid == 3:  # device modelid: 3
                        import dlib.devices.unilojas.c001 as unilojas_c001
                        unilojas_c001.DUnilojas_C001_Process(self._conn, self._addr, recv_header, recv_data,
                                                             self._resources).start()
                    else:  # Erro
                        listen_function.replay_err(self._conn,
                                                   CONST.RETURNCODE_CMD_INVALID)  # envia retorno erro comando
                        self._conn.close()
                elif recv_header.modelid == 1:  # modelid: Pextron URP1439TU
                    import dlib.devices.pextron.urp1439tu as pextron_urp1439tu
                    pextron_urp1439tu.DPextron_URP1439TU_Process(self._conn, self._addr, recv_header, recv_data,
                                                                 self._resources).start()
                elif recv_header.modelid == 2:  # modelid: Schneider SEPAM40
                    import dlib.devices.schneider.sepam40 as schneider_sepam40
                    schneider_sepam40.DSchneider_SEPAM40_Process(self._conn, self._addr, recv_header, recv_data,
                                                                 self._resources).start()
                elif recv_header.modelid == 4:  # modelid: Pextron URPE7104_V7_18
                    import dlib.devices.pextron.urpe7104_v7_18 as pextron_urpe7104_v7_18
                    pextron_urpe7104_v7_18.DPextron_URPE7104_V7_18_Process(self._conn, self._addr, recv_header,
                                                                           recv_data,
                                                                           self._resources).start()
                else:  # Erro
                    listen_function.replay_err(self._conn,
                                               CONST.RETURNCODE_CMD_INVALID)  # envia retorno erro comando
                    self._conn.close()
        except dsocket.DSocketHeaderException as err:
            if err.code == dsocket.DSocketHeaderException.DSOCKET_INVALID_ERR:
                e = CONST.RETURNCODE_HEADER_INVALID
            elif err.code == dsocket.DSocketHeaderException.DSOCKET_SIZE_ERR:
                e = CONST.RETURNCODE_SIZE_ERR
            elif err.code == dsocket.DSocketHeaderException.DSOCKET_CRC_ERR:
                e = CONST.RETURNCODE_CRC_ERR
            else:
                e = CONST.RETURNCODE_RECV_ERR
            listen_function.replay_err(self._conn, e)
            self._conn.close()
        except Exception as err:
            self.logger.debug('Erro inesperado {}'.format(str(err)))
            self._conn.close()

    def is_register(self, header: dsocket.DSocketHeaderBasic, data: RData) -> bool:
        if header.messagetype == dsocket.DSocketMessagesType.CMD_NOW:
            cmdtype = getattr(header, 'cmdtype', None)
            size = getattr(header, 'size', None)
            return cmdtype is not None and size is not None and \
                   cmdtype == 0 and size == 1 and data[0] == 1
        return False


# =============================================================================#
class DListen(threading.Thread):
    def __init__(self, resources: dict, config_listen: DConfig_Listen):
        threading.Thread.__init__(self)
        self._resources = resources
        self._config = config_listen
        self._iface = self._config.iface
        self._timeout = self._config.timeout
        self._port = self._config.port
        self._sockettimeout = self._config.sockettimeout
        self.logger = logging.getLogger(__name__)
        self._stop_event = threading.Event()

    def run(self):
        try:
            self.logger.info('Iniciando DListen -> {}'.format(self._config.str()))
            while self.is_running():
                self._reset = False
                host = get_ip_from_iface(self._iface)
                if host is None:
                    if not STATUS.is_state(CONST.STATE_ERRO_IP_LISTEN):
                        self.logger.debug('{} Erro obtendo IP listen.'.format(self.get_listen_name()))
                        STATUS.set_state(CONST.STATE_ERRO_IP_LISTEN)
                    time.sleep(10)
                    continue
                if STATUS.is_state(CONST.STATE_ERRO_IP_LISTEN):
                    STATUS.clear_state(CONST.STATE_ERRO_IP_LISTEN)
                self.logger.info('{} IP listen {}'.format(self.get_listen_name(), host))
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.__s:
                    self.__s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    self.__s.settimeout(self._timeout)
                    self.__s.bind((host, self._port))
                    self.__s.listen(1)
                    while self.is_running():
                        try:
                            nhost = get_ip_from_iface(self._iface)
                            if nhost != host:
                                self.logger.debug('{} Renew IP listen.'.format(self.get_listen_name()))
                                break
                            conn, addr = self.__s.accept()
                            listen_proc = DListen_Process(conn, addr, self._resources, self._sockettimeout)
                            listen_proc.start()
                        except socket.timeout:
                            if not STATUS.alive:
                                while not STATUS.alive and self.is_running():
                                    time.sleep(10)
                                break
                            if self._reset:
                                self.logger.debug('{} Reset listen.'.format(self.get_listen_name()))
                                time.sleep(10)
                                break
                        except InterruptedError:
                            break
                        except Exception as err:
                            self.logger.debug('{} Listen erro: {}'.format(self.get_listen_name(), str(err)))
                            break
            self.logger.debug('Terminada Thread DListen {}'.format(self.get_listen_name()))
        except Exception as err:
            self.logger.fatal('Thread DListen {} falha:{}'.format(self.get_listen_name(), str(err)))

    def reset(self):
        self._reset = True

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def stop(self):
        self._stop_event.set()

    def get_listen_name(self) -> str:
        return 'Listen:{}/{}'.format(self._iface, self._port)
