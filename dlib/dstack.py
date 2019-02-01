#!/usr/bin/python3
# -*- coding: utf-8 -*-

import threading
import sqlite3
import os
import socket
import time
import dlib.dsocket as dsocket
import logging
from rlib.common import RData, RConfigParms, RConfigError
from dlib.dcommon import CONFIG, GLOBAL, DResetTypes, CONST
from dlib.dstatus import STATUS


# =============================================================================#
class DSendStackConfig(RConfigParms):
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
        return self._config.conf.getint(self._section, CONST.PORT, fallback=9091)

    @port.setter
    def port(self, value):
        self._config.conf.set(self._section, CONST.PORT, value)

    @property
    def timeout(self) -> int:
        return self._config.conf.getint(self._section, CONST.TIMEOUT, fallback=30)

    @timeout.setter
    def timeout(self, value):
        self._config.conf.set(self._section, CONST.TIMEOUT, value)

    @property
    def interval(self) -> int:
        return self._config.conf.getint(self._section, CONST.INTERVAL, fallback=1000)

    @interval.setter
    def interval(self, value):
        self._config.conf.set(self._section, CONST.INTERVAL, value)

    @property
    def backuptime(self) -> int:
        return self._config.conf.getint(self._section, CONST.BACKUPTIME, fallback=60)

    @backuptime.setter
    def backuptime(self, value):
        self._config.conf.set(self._section, CONST.BACKUPTIME, value)

    @property
    def backupcount(self) -> int:
        return self._config.conf.getint(self._section, CONST.BACKUPCOUNT, fallback=10)

    @backupcount.setter
    def backupcount(self, value):
        self._config.conf.set(self._section, CONST.BACKUPCOUNT, value)

    @property
    def dbfile(self) -> str:
        return self._config.conf.get(self._section, CONST.DBFILE, fallback='stack.db')

    @dbfile.setter
    def dbfile(self, value):
        self._config.conf.set(self._section, CONST.DBFILE, value)


# =============================================================================#
class DSendStack(threading.Thread):
    threadLock = threading.Lock()

    def __init__(self):
        super().__init__()
        self._config = DSendStackConfig(CONST.STACK, CONFIG, CONST.MAIN)
        self._dbfile = self._config.dbfile
        self._host = self._config.host
        self._port = self._config.port
        self._interval = self._config.interval / 1000
        self._backup = self._config.backuptime * 60
        self._timeout = self._config.timeout
        self._backupcount = self._config.backupcount
        self._count = 0
        self._stop_event = threading.Event()
        self.logger = logging.getLogger(__name__)

    def run(self):
        self.logger.info('Iniciando DStack -> {}'.format(self._config.str()))
        try:
            self._con = self._create_db(self._dbfile)
            DSendStack.threadLock.acquire()
            try:
                self._count = self._count + self._count_rows()
            finally:
                DSendStack.threadLock.release()
            self.logger.debug('Backup DB com {} mensagens.'.format(self._count))
            t = int(time.time() / 60)
            try:
                while self.is_running():
                    if self._count != 0:
                        if not STATUS.is_state(
                                CONST.DCONST.STATE_BACKUP) and self._count > self._backupcount:
                            STATUS.set_state(CONST.DCONST.STATE_BACKUP)
                        elif STATUS.is_state(
                                CONST.DCONST.STATE_BACKUP) and self._count <= self._backupcount:
                            STATUS.clear_state(CONST.DCONST.STATE_BACKUP)
                        d = self._pull_db()
                        if d:
                            f, r = self._send(d)
                            if f:
                                self._pop_db(d[0])
                                DSendStack.threadLock.acquire()
                                self._count = self._count - 1
                                DSendStack.threadLock.release()
                                if r:
                                    GLOBAL.reset = DResetTypes.HARD_RESET
                                t = time.time()
                            else:
                                STATUS.alive = False
                                if time.time() - t > self._backup:
                                    self._archive()
                                    t = int(time.time() / 60)
                    self.wait_time(self._interval)
                self.logger.debug('Terminada Thread DStack')
            except Exception:
                raise
            finally:
                self._con.close()
        except Exception as err:
            self.logger.fatal('Thread DStack falha: {}'.format(str(err)))

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def stop(self):
        self._stop_event.set()

    def wait_time(self, timeout):
        self._stop_event.wait(timeout)

    def is_empty(self):
        DSendStack.threadLock.acquire()
        con = None
        try:
            con = self._create_db(self._dbfile)
            sql = '''SELECT * FROM sendstack;'''
            c = con.execute(sql)
            d = c.fetchone() == None
            self.logger.info('Stack is empty: {}'.format(d))
            return d
        except Exception as err:
            self.logger.error('Erro obtendo dados do stack: {}'.format(str(err)))
        finally:
            if con:
                con.close()
            DSendStack.threadLock.release()

    def put(self, priority, timestamp, header, data=None):
        DSendStack.threadLock.acquire()
        con = None
        try:
            con = self._create_db(self._dbfile)
            sql = '''INSERT INTO sendstack
                  (priority, timestamp, header, data)
                  VALUES(?, ?, ?, ?);'''
            if priority > 9:
                priority = 10
            if priority < 0:
                priority = 0
            if data:
                con.execute(sql,
                            [int(priority), timestamp, sqlite3.Binary(header.exchange_data()), sqlite3.Binary(data)])
            else:
                con.execute(sql, [int(priority), timestamp, sqlite3.Binary(header.exchange_data()), None])
            con.commit()
            self._count = self._count + 1
            self.logger.debug('Mensagem {} adicionada no DB, total {}'.format(header.messagetype, self._count))
        except Exception as err:
            self.logger.error('Erro inserindo no stack: {}'.format(str(err)))
        finally:
            if con:
                con.close()
            DSendStack.threadLock.release()

    def _create_db(self, dbfile):
        db_is_new = not os.path.exists(dbfile)
        con = sqlite3.connect(dbfile)
        if db_is_new:
            self.logger.info('Criando stack {}'.format(dbfile))
            sql = ('''CREATE TABLE IF NOT EXISTS sendstack
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  priority INTEGER NOT NULL,
                  timestamp REAL NOT NULL,
                  header BLOB NOT NULL,
                  data BLOB);''')
            con.execute(sql)
        return con

    def _pull_db(self):
        try:
            sql = '''SELECT * FROM sendstack
                  ORDER BY priority, timestamp;'''
            c = self._con.execute(sql)
            return c.fetchone()
        except Exception as err:
            self.logger.error('Erro obtendo dados do stack: {}'.format(str(err)))

    def _pop_db(self, item):
        try:
            sql = '''DELETE FROM sendstack
                  WHERE id = ?;'''
            self._con.execute(sql, [item, ])
            self._con.commit()
        except Exception as err:
            self.logger.error('Erro apagando no stack: {}'.format(str(err)))

    def _count_rows(self):
        try:
            sql = '''SELECT COUNT(*) FROM sendstack;'''
            c = self._con.execute(sql)
            return c.fetchone()[0]
        except Exception as err:
            self.logger.error('Erro contando itens: {}'.format(str(err)))

    def _send(self, data) -> (bool, bool):
        boot = False
        header = dsocket.socketheader(dsocket.DSocketHeaderBasic(data[3]))
        if data[4]:
            rdata = RData(data[4])
        else:
            rdata = None
        try:
            # create socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    # configure socket
                    s.settimeout(self._timeout)
                    # connect ...
                    s.connect((self._host, self._port))
                    if getattr(header, 'size', None) is not None:
                        self.logger.debug('[Data]\n{}'.format(rdata.dump()))
                    # send
                    dsocket.dsocket_send(s, header, rdata)
                    self.logger.info('Enviado: [Header] {}'.format(header))
                    # receive
                    recv_header, recv_data = dsocket.dsocket_listen(s)
                    self.logger.info('Recebido: [Header] {}'.format(recv_header))
                    if getattr(recv_header, 'size', None) is not None:
                        self.logger.debug('[Data]\n{}'.format(recv_data.dump()))
                    # receive response data
                    if recv_header.messagetype == dsocket.DSocketMessagesType.ACK:
                        # mensagem ok
                        self.logger.debug('Recebido: {}'.format(recv_header))
                        ret = True
                        # check for recovery
                        boot = recv_header.returncode == 1
                    else:
                        # mensagem com header valido, mas nao confirmacao... grava no log e passa a diante...
                        self.logger.fatal('Recebido Erro: {}'.format(recv_header))
                        recv_size = getattr(recv_header, 'size', None)
                        # check for received data
                        if recv_size is not None:
                            recv_data = RData(s.recv(recv_size))  # get received data
                            self.logger.fatal('[Data]\n{}'.format(recv_data.dump()))
                        ret = False
                    return ret, boot
                except:
                    raise
                finally:
                    s.close()
        except Exception as err:
            self.logger.fatal('Erro transmitindo/recebendo stack: {}'.format(str(err)))
            return False, False

    def _archive(self):
        try:
            sql = '''UPDATE sendstack
            SET priority = ? WHERE priority < ?;'''
            self._con.execute(sql, [10, 10])
            self._con.commit()
        except Exception as err:
            self.logger.error('Erro obtendo dados do stack: {}'.format(str(err)))
