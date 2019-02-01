#!/usr/bin/python3
# -*- coding: utf-8 -*-

import rlib.common as common
from Crypto.Cipher import Blowfish


# =============================================================================#
class DStatus(object):
    def __init__(self):
        self._data = common.RData([0x00, 0x00, 0x00, 0x00])
        self._id = -1
        self.send_alive_function = None

    def clear(self):
        self._data.set_byte(common.RByteType.BYTE32, 0, 0)
        self._id = -1
        self._call_func()

    def read_id_from_file(self, idname: str):
        with open(idname, 'rb') as f:
            id_crypt = f.read()
            bs = Blowfish.block_size
            keyb = b'unilojasmille'
            iv = id_crypt[:bs]
            id_crypt = id_crypt[bs:]
            cipher = Blowfish.new(keyb, Blowfish.MODE_CBC, iv)
            msg = cipher.decrypt(id_crypt)
            last_byte = msg[-1]
            id = msg[:- (last_byte if type(last_byte) is int else ord(last_byte))]
            if int(id) < 65536:
                self._id = int(id)
            else:
                raise ValueError('ID invalido.')

    @property
    def id(self):
        return self._id

    @property
    def alive(self) -> bool:
        return bool(self._data.get_byte(common.RByteType.BYTE8, 1))

    @alive.setter
    def alive(self, value: bool):
        if self.alive != value:
            self._data.set_byte(common.RByteType.BYTE8, 1, int(value))
            self._call_func()

    @property
    def status(self):
        return self._data.get_byte(common.RByteType.BYTE16, 2, False, True)

    @status.setter
    def status(self, value):
        if self.status != value:
            self._data.set_byte(common.RByteType.BYTE16, 2, value, False, True)
            self._call_func()

    def to_send(self) -> common.RData:
        return self._data

    def is_state(self, state):
        offset, bit = self._calc_bit(state)
        return self._data.is_bit(offset, bit)

    def set_state(self, state):
        if not self.is_state(state):
            offset, bit = self._calc_bit(state)
            self._data.set_bit(offset, bit)
            self._call_func()

    def clear_state(self, state):
        if self.is_state(state):
            offset, bit = self._calc_bit(state)
            self._data.clear_bit(offset, bit)
            self._call_func()

    def toggle_state(self, state):
        offset, bit = self._calc_bit(state)
        self._data.toggle_bit(offset, bit)
        self._call_func()

    def put_state(self, state, value):
        if value:
            self.set_state(state)
        else:
            self.clear_state(state)

    def _calc_bit(self, state) -> (int, int):
        offset = (state % 8) + 16
        bit = int(state / 8)
        return offset, bit

    def _call_func(self):
        if self.send_alive_function:
            self.send_alive_function()

    def __str__(self):
        return 'Id:{0} Alive:{1} Status:{2:b}'.format(self.id, self.alive, self.status)


# =============================================================================#
STATUS = DStatus()