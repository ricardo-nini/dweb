#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import socket
import struct
import threading
import traceback
import time
from rlib.common import RConfigError, RConfigParms, RByteType as BTYPE, RData, int2ip, get_ip_from_iface
from dlib.dcommon import CONFIG, GLOBAL, CONST
from dlib.dstatus import STATUS


# =============================================================================#
class DAliveConfig(RConfigParms):
    @property
    def host(self) -> str:
        if self._config.conf.has_option(self._section, CONST.HOST):
            return self._config.conf.get(self._section, CONST.HOST)
        elif len(self._main_section) != 0 and self._config.conf.has_option(self._main_section, CONST.HOST):
            return self._config.conf.get(self._main_section, CONST.HOST)
        else:
            raise RConfigError(CONST.HOST)

    @host.setter
    def host(self, value):
        self._config.conf.set(self._section, CONST.PORT, value)

    @property
    def port(self) -> int:
        return self._config.conf.getint(self._section, CONST.PORT, fallback=9093)

    @port.setter
    def port(self, value):
        self._config.conf.set(self._section, CONST.PORT, value)

    @property
    def iface(self) -> str:
        if self._config.conf.has_option(self._section, CONST.IFACE):
            return self._config.conf.get(self._section, CONST.IFACE)
        elif len(self._main_section) != 0 and self._config.conf.has_option(self._main_section, CONST.IFACE):
            return self._config.conf.get(self._main_section, CONST.IFACE)
        else:
            raise RConfigError(CONST.IFACE)

    @iface.setter
    def iface(self, value):
        self._config.conf.set(self._section, CONST.IFACE, value)

    @property
    def timeout(self) -> int:
        return self._config.conf.getint(self._section, CONST.TIMEOUT, fallback=30)

    @timeout.setter
    def timeout(self, value):
        self._config.conf.set(self._section, CONST.TIMEOUT, value)

    @property
    def interval(self) -> int:
        return self._config.conf.getint(self._section, CONST.INTERVAL, fallback=900)

    @interval.setter
    def interval(self, value):
        self._config.conf.set(self._section, CONST.INTERVAL, value)


# =============================================================================#
class DAlive(threading.Thread):
    def __init__(self):
        super().__init__()
        self._config = DAliveConfig(CONST.ALIVE, CONFIG, CONST.MAIN)
        self._timeout = self._config.timeout
        self._host = self._config.host
        self._port = self._config.port
        self._interval = self._config.interval
        self._iface = self._config.iface
        self.STATUS = 0
        self._send_now = False
        self._stop_event = threading.Event()
        self.logger = logging.getLogger(__name__)

    def run(self):
        self.logger.info('Iniciando DAlive -> {}'.format(self._config.str()))
        try:
            real_state = 0
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.settimeout(self._timeout)
            server_address = (self._host, self._port)
            addr = (0,)
            while self.is_running():
                try:
                    data_send = RData()
                    data_send.add_byte(BTYPE.BYTE16, STATUS.id)
                    addr = self._getip()
                    data_send.add_byte(BTYPE.BYTE32, addr[0], bigendian=True)
                    data_send.add_byte(BTYPE.BYTE16, STATUS.status)
                    sent = self._sock.sendto(data_send, server_address)
                    if sent != 0 and self.is_running():
                        data, server = self._sock.recvfrom(2)
                        d = RData(data)
                        r = d.get_byte(BTYPE.BYTE16, 0)
                        if r == STATUS.id:
                            STATUS.alive = True
                            real_state = 1
                        else:
                            STATUS.alive = False
                            real_state = 2
                    else:
                        real_state = 0
                    if not self.is_running():
                        break
                except Exception:
                    STATUS.alive = False
                    real_state = 0
                finally:
                    if self.STATUS != STATUS.status:
                        self.STATUS = STATUS.status
                    if not self._send_now:
                        self.logger.debug(STATUS)
                    if real_state == 0:
                        time.sleep(1)
                    else:
                        t = 0
                        while t < int(self._interval) and self.is_running():
                            time.sleep(1)
                            t = t + 1
                            naddr = self._getip()
                            if naddr[0] != addr[0] or self._send_now:
                                if naddr[0] != addr[0]:
                                    self.logger.debug(
                                        'IP trocado de {} para {}, enviando alive.'.format(int2ip(addr[0]),
                                                                                           int2ip(naddr[0])))
                                self._send_now = False
                                break
            self.logger.debug('Terminada Thread DAlive')
        except Exception as e:
            if logging.getLogger().getEffectiveLevel() >= CONST.ERROR_LEVEL:
                self.logger.error('Thread DAlive falha: {}'.format(str(e)))
            else:
                for line in traceback.format_exc().split("\n"):
                    self.logger.error(line)
        finally:
            self._sock.close()

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def stop(self):
        self._stop_event.set()

    def send_alive_now(self):
        self._send_now = True

    def _getip(self):
        try:
            return struct.unpack('>I', socket.inet_aton(get_ip_from_iface(self._iface)))
        except:
            return (0,)
