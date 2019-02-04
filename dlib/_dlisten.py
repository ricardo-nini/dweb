#!/usr/bin/python3
# -*- coding: utf-8 -*-

import threading
import logging
import socket
import os
import struct
from rlib.common import CONST, crc32f, RData
import dlib.dsocket as dsocket
from dlib.dstatus import STATUS

logger = logging.getLogger(__name__)


# =============================================================================#
def replay_ok(conn: socket, code=CONST.RETURNCODE_OK):
    try:
        h = dsocket.DSocketHeader_Ack.create(STATUS.id, code)
        dsocket.dsocket_send(conn, h, None)
    except Exception as err:
        logger.debug('Erro enviando ACK {}'.format(str(err)))


def replay_err(conn: socket, code=0):
    try:
        h = dsocket.DSocketHeader_Error.create(STATUS.id, code)
        dsocket.dsocket_send(conn, h, None)
    except Exception as err:
        logger.debug('Erro enviando ERR {}'.format(str(err)))


def replay_cmd_now(conn: socket, data: RData):
    h = dsocket.DSocketHeader_CmdNowResponse.create(STATUS.id)
    try:
        dsocket.dsocket_send(conn, h, data)
    except Exception as err:
        logger.debug('Erro enviando CmdNowResponse {}'.format(str(err)))


def replay_cmd_now_file(conn: socket, filename: str, buffersize=1024):
    try:
        fileback = '{}.copy'.format(filename)
        with open(fileback, 'wb') as fw:
            with open(filename, 'rb') as fr:
                crc = crc32f(fr)
                for chunk in iter(lambda: fr.read(buffersize), b''):
                    fw.write(chunk)
                v = struct.pack('<I', crc)
                fw.write(v)
                fw.flush()
        with open(fileback, 'rb') as ft:
            h = dsocket.DSocketHeader_CmdNowResponse.create(STATUS.id)
            dsocket.dsocket_send_file(conn, h, ft)
        os.remove(fileback)
    except Exception as err:
        logger.debug('Erro enviando file:{} {}'.format(filename, str(err)))


# =============================================================================#
class DListenProcess(threading.Thread):
    def __init__(self, conn: socket, addr, header: dsocket.DSocketHeaderBasic, data: RData, resources: dict):
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
