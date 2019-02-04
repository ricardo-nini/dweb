#!/usr/bin/python3
# -*- coding: utf-8 -*-

import socket
import enum
import os
import io
import datetime
import rlib.common as common
from rlib.common import RByteType as BTYPE


# =============================================================================#
class DSocketMessagesType(enum.Enum):
    # ask
    REPORT = 0x01
    ALARM = 0x02
    SETUP = 0x03
    BOOT = 0x04
    CODI = 0x05
    EVENT = 0x06
    CMD_NOW = 0x07
    CMD = 0x08
    # response
    CMD_NOW_RESPONSE = 0xFC
    CMD_RESPONSE = 0xFD
    ACK = 0xFE
    ERROR = 0xFF


# =============================================================================#
DSocketHeaderSizes = {
    DSocketMessagesType.REPORT: 19,
    DSocketMessagesType.ALARM: 19,
    DSocketMessagesType.SETUP: 15,
    DSocketMessagesType.BOOT: 7,
    DSocketMessagesType.CODI: 19,
    DSocketMessagesType.EVENT: 19,
    DSocketMessagesType.CMD_NOW: 11,
    DSocketMessagesType.CMD: 13,
    DSocketMessagesType.CMD_NOW_RESPONSE: 7,
    DSocketMessagesType.CMD_RESPONSE: 13,
    DSocketMessagesType.ACK: 5,
    DSocketMessagesType.ERROR: 5
}


# =============================================================================#
class DSocketHeaderException(Exception):
    DSOCKET_INVALID_ERR = 0
    DSOCKET_SIZE_ERR = 1
    DSOCKET_RECV_ERR = 2
    DSOCKET_CRC_ERR = 3

    DSocketHeaderException_MSG = (
        'Invalid socket header!',
        'Invalid socket header size!',
        'Receive error on socket header!',
        'CRC error!'
    )

    def __init__(self, code: int):
        self.code = code
        self.msg = DSocketHeaderException.DSocketHeaderException_MSG[code]


# =============================================================================#
class DSocketHeaderBasic(object):
    def __init__(self, rdata: common.RData, trunc=0):
        if trunc == 0:
            self._data = common.RData(rdata)
        else:
            self._data = common.RData(rdata[:trunc])

    @property
    def messagetype(self) -> DSocketMessagesType:
        return DSocketMessagesType(self._data.get_byte(BTYPE.BYTE8, 0))

    @messagetype.setter
    def messagetype(self, value: DSocketMessagesType):
        self._data.set_byte(BTYPE.BYTE8, 0, value.value)

    @property
    def deviceid(self):
        return self._data.get_byte(BTYPE.BYTE16, 1)

    @deviceid.setter
    def deviceid(self, value):
        self._data.set_byte(BTYPE.BYTE16, 1, value)

    @classmethod
    def recv(cls, socket: socket):
        r = bytearray(socket.recv(1))
        if r is None:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_RECV_ERR)
        try:
            remain = DSocketHeaderSizes[DSocketMessagesType(r[0])] - 1
        except:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_RECV_ERR)
        rb = bytearray(socket.recv(remain))
        if (remain != len(rb)):
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_RECV_ERR)
        r[1:] = rb
        return cls(r)

    def exchange_data(self):
        return self._data

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return "MessageType:{} DeviceID:{}".format(self.messagetype, self.deviceid)


# =============================================================================#
class __DSocketHeaderBasicUnsolicited(DSocketHeaderBasic):
    @classmethod
    def create(cls, messagetype: DSocketMessagesType, deviceid: int, timestamp: int, slave: int, modelid: int,
               fmt: int, size=0, crc=0):
        rdata = common.RData()
        rdata.add_byte(BTYPE.BYTE8, messagetype.value)
        rdata.add_byte(BTYPE.BYTE16, deviceid)
        rdata.add_byte(BTYPE.BYTE32, timestamp)
        rdata.add_byte(BTYPE.BYTE8, slave)
        rdata.add_byte(BTYPE.BYTE16, modelid)
        rdata.add_byte(BTYPE.BYTE8, fmt)
        rdata.add_byte(BTYPE.BYTE32, size)
        rdata.add_byte(BTYPE.BYTE32, crc)
        return cls(rdata)

    @property
    def timestamp(self):
        return self._data.get_byte(BTYPE.BYTE32, 3)

    @timestamp.setter
    def timestamp(self, value):
        self._data.set_byte(BTYPE.BYTE32, 3, value)

    @property
    def slave(self):
        return self._data.get_byte(BTYPE.BYTE8, 7)

    @slave.setter
    def slave(self, value):
        self._data.set_byte(BTYPE.BYTE8, 7, value)

    @property
    def modelid(self):
        return self._data.get_byte(BTYPE.BYTE16, 8)

    @modelid.setter
    def modelid(self, value):
        self._data.set_byte(BTYPE.BYTE16, 8, value)

    @property
    def fmt(self):
        return self._data.get_byte(BTYPE.BYTE8, 10)

    @fmt.setter
    def fmt(self, value):
        self._data.set_byte(BTYPE.BYTE8, 10, value)

    @property
    def size(self):
        return self._data.get_byte(BTYPE.BYTE32, 11)

    @size.setter
    def size(self, value):
        self._data.set_byte(BTYPE.BYTE32, 11, value)

    @property
    def crc(self):
        return self._data.get_byte(BTYPE.BYTE32, 15)

    @crc.setter
    def crc(self, value):
        self._data.set_byte(BTYPE.BYTE32, 15, value)

    def __repr__(self):
        return "{} Timestamp:{} Slave:{} ModelID:{} Fmt:{} Size:{} Crc32:{}".format(super().__repr__(),
                                                                                    datetime.datetime.fromtimestamp(
                                                                                        self.timestamp),
                                                                                    self.slave,
                                                                                    self.modelid,
                                                                                    self.fmt,
                                                                                    self.size,
                                                                                    self.crc)


# =============================================================================#
class DSocketHeader_Report(__DSocketHeaderBasicUnsolicited):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.REPORT]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.REPORT.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.REPORT])

    @classmethod
    def create(cls, deviceid: int, timestamp: int, slave: int, modelid: int, fmt: int, size=0, crc=0):
        return super().create(DSocketMessagesType.REPORT, deviceid, timestamp, slave, modelid, fmt, size, crc)


# =============================================================================#
class DSocketHeader_Alarm(__DSocketHeaderBasicUnsolicited):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.ALARM]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.ALARM.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.ALARM])

    @classmethod
    def create(cls, deviceid: int, timestamp: int, slave: int, modelid: int, fmt: int, size=0, crc=0):
        return super().create(DSocketMessagesType.ALARM, deviceid, timestamp, slave, modelid, fmt, size, crc)


# =============================================================================#
class DSocketHeader_Setup(DSocketHeaderBasic):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.SETUP]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.SETUP.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.SETUP])

    @classmethod
    def create(cls, deviceid: int, timestamp: int, size=0, crc=0):
        rdata = common.RData()
        rdata.add_byte(BTYPE.BYTE8, DSocketMessagesType.SETUP.value)
        rdata.add_byte(BTYPE.BYTE16, deviceid)
        rdata.add_byte(BTYPE.BYTE32, timestamp)
        rdata.add_byte(BTYPE.BYTE32, size)
        rdata.add_byte(BTYPE.BYTE32, crc)
        return cls(rdata)

    @property
    def timestamp(self):
        return self._data.get_byte(BTYPE.BYTE32, 3)

    @timestamp.setter
    def timestamp(self, value):
        self._data.set_byte(BTYPE.BYTE32, 3, value)

    @property
    def size(self):
        return self._data.get_byte(BTYPE.BYTE32, 7)

    @size.setter
    def size(self, value):
        self._data.set_byte(BTYPE.BYTE32, 7, value)

    @property
    def crc(self):
        return self._data.get_byte(BTYPE.BYTE32, 11)

    @crc.setter
    def crc(self, value):
        self._data.set_byte(BTYPE.BYTE32, 11, value)

    def __repr__(self):
        return "{} Timestamp:{} Size:{} Crc32:{}".format(super().__repr__(),
                                                         datetime.datetime.fromtimestamp(self.timestamp),
                                                         self.size,
                                                         self.crc)


# =============================================================================#
class DSocketHeader_Boot(DSocketHeaderBasic):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.BOOT]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.BOOT.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.BOOT])

    @classmethod
    def create(cls, deviceid: int, timestamp: int):
        rdata = common.RData()
        rdata.add_byte(BTYPE.BYTE8, DSocketMessagesType.BOOT.value)
        rdata.add_byte(BTYPE.BYTE16, deviceid)
        rdata.add_byte(BTYPE.BYTE32, timestamp)
        return cls(rdata)

    @property
    def timestamp(self):
        return self._data.get_byte(BTYPE.BYTE32, 3)

    @timestamp.setter
    def timestamp(self, value):
        self._data.set_byte(BTYPE.BYTE32, 3, value)

    def __repr__(self):
        return "{} Timestamp:{}".format(super().__repr__(), datetime.datetime.fromtimestamp(self.timestamp))


# =============================================================================#
class DSocketHeader_Codi(__DSocketHeaderBasicUnsolicited):
    def __init__(self, rdata: common.RData):
        super().__init__(rdata)
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.CODI]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.CODI.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.CODI])

    @classmethod
    def create(cls, deviceid: int, timestamp: int, slave: int, modelid: int, fmt: int, size=0, crc=0):
        return super().create(DSocketMessagesType.CODI, deviceid, timestamp, slave, modelid, fmt, size, crc)


# =============================================================================#
class DSocketHeader_Event(__DSocketHeaderBasicUnsolicited):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.EVENT]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.EVENT.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.EVENT])

    @classmethod
    def create(cls, deviceid: int, timestamp: int, slave: int, modelid: int, fmt: int, size=0, crc=0):
        return super().create(DSocketMessagesType.EVENT, deviceid, timestamp, slave, modelid, fmt, size, crc)


# =============================================================================#
class DSocketHeader_CmdNow(DSocketHeaderBasic):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.CMD_NOW]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.CMD_NOW.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.CMD_NOW])

    @classmethod
    def create(cls, deviceid: int, slave: int, modelid: int, cmdtype: int, size=0):
        rdata = common.RData()
        rdata.add_byte(BTYPE.BYTE8, DSocketMessagesType.CMD_NOW.value)
        rdata.add_byte(BTYPE.BYTE16, deviceid)
        rdata.add_byte(BTYPE.BYTE8, slave)
        rdata.add_byte(BTYPE.BYTE16, modelid)
        rdata.add_byte(BTYPE.BYTE8, cmdtype)
        rdata.add_byte(BTYPE.BYTE32, size)
        return cls(rdata)

    @property
    def slave(self):
        return self._data.get_byte(BTYPE.BYTE8, 3)

    @slave.setter
    def slave(self, value):
        self._data.set_byte(BTYPE.BYTE8, 3, value)

    @property
    def modelid(self):
        return self._data.get_byte(BTYPE.BYTE16, 4)

    @modelid.setter
    def modelid(self, value):
        self._data.set_byte(BTYPE.BYTE16, 4, value)

    @property
    def cmdtype(self):
        return self._data.get_byte(BTYPE.BYTE8, 6)

    @cmdtype.setter
    def cmdtype(self, value):
        self._data.set_byte(BTYPE.BYTE8, 6, value)

    @property
    def size(self):
        return self._data.get_byte(BTYPE.BYTE32, 7)

    @size.setter
    def size(self, value):
        self._data.set_byte(BTYPE.BYTE32, 7, value)

    def __repr__(self):
        return "{} Slave:{} ModelID:{} CmdType:{} Size:{}".format(super().__repr__(), self.slave, self.modelid,
                                                                  self.cmdtype, self.size)


# =============================================================================#
class DSocketHeader_Cmd(DSocketHeaderBasic):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.CMD]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.CMD.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.CMD])

    @classmethod
    def create(cls, deviceid: int, slave: int, modelid: int, cmdtype: int, messageid: int, size=0):
        rdata = common.RData()
        rdata.add_byte(BTYPE.BYTE8, DSocketMessagesType.CMD.value)
        rdata.add_byte(BTYPE.BYTE16, deviceid)
        rdata.add_byte(BTYPE.BYTE8, slave)
        rdata.add_byte(BTYPE.BYTE16, modelid)
        rdata.add_byte(BTYPE.BYTE8, cmdtype)
        rdata.add_byte(BTYPE.BYTE16, messageid)
        rdata.add_byte(BTYPE.BYTE32, size)
        return cls(rdata)

    @property
    def slave(self):
        return self._data.get_byte(BTYPE.BYTE8, 3)

    @slave.setter
    def slave(self, value):
        self._data.set_byte(BTYPE.BYTE8, 3, value)

    @property
    def modelid(self):
        return self._data.get_byte(BTYPE.BYTE16, 4)

    @modelid.setter
    def modelid(self, value):
        self._data.set_byte(BTYPE.BYTE16, 4, value)

    @property
    def cmdtype(self):
        return self._data.get_byte(BTYPE.BYTE8, 6)

    @cmdtype.setter
    def cmdtype(self, value):
        self._data.set_byte(BTYPE.BYTE8, 6, value)

    @property
    def messageid(self):
        return self._data.get_byte(BTYPE.BYTE16, 7)

    @messageid.setter
    def messageid(self, value):
        self._data.set_byte(BTYPE.BYTE16, 7, value)

    @property
    def size(self):
        return self._data.get_byte(BTYPE.BYTE32, 9)

    @size.setter
    def size(self, value):
        self._data.set_byte(BTYPE.BYTE32, 9, value)

    def __repr__(self):
        return "{} Slave:{} ModelID:{} CmdType:{} MessageID:{} Size:{}".format(super().__repr__(),
                                                                               self.slave,
                                                                               self.modelid,
                                                                               self.cmdtype,
                                                                               self.messageid,
                                                                               self.size)


# =============================================================================#
class DSocketHeader_CmdNowResponse(DSocketHeaderBasic):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.CMD_NOW_RESPONSE]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.CMD_NOW_RESPONSE.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.CMD_NOW_RESPONSE])

    @classmethod
    def create(cls, deviceid: int, size=0):
        rdata = common.RData()
        rdata.add_byte(BTYPE.BYTE8, DSocketMessagesType.CMD_NOW_RESPONSE.value)
        rdata.add_byte(BTYPE.BYTE16, deviceid)
        rdata.add_byte(BTYPE.BYTE32, size)
        return cls(rdata)

    @property
    def size(self):
        return self._data.get_byte(BTYPE.BYTE32, 3)

    @size.setter
    def size(self, value):
        self._data.set_byte(BTYPE.BYTE32, 3, value)

    def __repr__(self):
        return "{} Size:{}".format(super().__repr__(), self.size)


# =============================================================================#
class DSocketHeader_CmdResponse(DSocketHeaderBasic):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.CMD_RESPONSE]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.CMD_RESPONSE.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.CMD_RESPONSE])

    @classmethod
    def create(cls, deviceid: int, slave: int, modelid: int, cmdtype: int, messageid: int, size=0):
        rdata = common.RData()
        rdata.add_byte(BTYPE.BYTE8, DSocketMessagesType.CMD_RESPONSE.value)
        rdata.add_byte(BTYPE.BYTE16, deviceid)
        rdata.add_byte(BTYPE.BYTE8, slave)
        rdata.add_byte(BTYPE.BYTE16, modelid)
        rdata.add_byte(BTYPE.BYTE8, cmdtype)
        rdata.add_byte(BTYPE.BYTE16, messageid)
        rdata.add_byte(BTYPE.BYTE32, size)
        return cls(rdata)

    @property
    def slave(self):
        return self._data.get_byte(BTYPE.BYTE8, 3)

    @slave.setter
    def slave(self, value):
        self._data.set_byte(BTYPE.BYTE8, 3, value)

    @property
    def modelid(self):
        return self._data.get_byte(BTYPE.BYTE16, 4)

    @modelid.setter
    def modelid(self, value):
        self._data.set_byte(BTYPE.BYTE16, 4, value)

    @property
    def cmdtype(self):
        return self._data.get_byte(BTYPE.BYTE8, 6)

    @cmdtype.setter
    def cmdtype(self, value):
        self._data.set_byte(BTYPE.BYTE8, 6, value)

    @property
    def messageid(self):
        return self._data.get_byte(BTYPE.BYTE16, 7)

    @messageid.setter
    def messageid(self, value):
        self._data.set_byte(BTYPE.BYTE16, 7, value)

    @property
    def size(self):
        return self._data.get_byte(BTYPE.BYTE32, 9)

    @size.setter
    def size(self, value):
        self._data.set_byte(BTYPE.BYTE32, 9, value)

    def __repr__(self):
        return "{} Slave:{} ModelID:{} CmdType:{} MessageID:{} Size:{}".format(super().__repr__(),
                                                                               self.slave,
                                                                               self.modelid,
                                                                               self.cmdtype,
                                                                               self.messageid,
                                                                               self.size)


# =============================================================================#
class DSocketHeader_Ack(DSocketHeaderBasic):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.ACK]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.ACK.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)

        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.ACK])

    @classmethod
    def create(cls, deviceid: int, returncode: int):
        rdata = common.RData()
        rdata.add_byte(BTYPE.BYTE8, DSocketMessagesType.ACK.value)
        rdata.add_byte(BTYPE.BYTE16, deviceid)
        rdata.add_byte(BTYPE.BYTE16, returncode)
        return cls(rdata)

    @property
    def returncode(self):
        return self._data.get_byte(BTYPE.BYTE16, 3)

    @returncode.setter
    def returncode(self, value):
        self._data.set_byte(BTYPE.BYTE16, 3, value)

    def __repr__(self):
        return "{} ReturnCode:{}".format(super().__repr__(), self.returncode)


# =============================================================================#
class DSocketHeader_Error(DSocketHeaderBasic):
    def __init__(self, rdata: common.RData):
        if len(rdata) < DSocketHeaderSizes[DSocketMessagesType.ERROR]:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        if rdata[0] != DSocketMessagesType.ERROR.value:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)
        super().__init__(rdata, DSocketHeaderSizes[DSocketMessagesType.ERROR])

    @classmethod
    def create(cls, deviceid: int, returncode: int):
        rdata = common.RData()
        rdata.add_byte(BTYPE.BYTE8, DSocketMessagesType.ERROR.value)
        rdata.add_byte(BTYPE.BYTE16, deviceid)
        rdata.add_byte(BTYPE.BYTE16, returncode)
        return cls(rdata)

    @property
    def returncode(self):
        return self._data.get_byte(BTYPE.BYTE16, 3)

    @returncode.setter
    def returncode(self, value):
        self._data.set_byte(BTYPE.BYTE16, 3, value)

    def __repr__(self):
        return "{} ReturnCode:{}".format(super().__repr__(), self.returncode)


# =============================================================================#
def socketheader(header: DSocketHeaderBasic):
    if header.messagetype == DSocketMessagesType.REPORT:
        return DSocketHeader_Report(header.exchange_data())
    if header.messagetype == DSocketMessagesType.ALARM:
        return DSocketHeader_Alarm(header.exchange_data())
    if header.messagetype == DSocketMessagesType.SETUP:
        return DSocketHeader_Setup(header.exchange_data())
    if header.messagetype == DSocketMessagesType.BOOT:
        return DSocketHeader_Boot(header.exchange_data())
    if header.messagetype == DSocketMessagesType.CODI:
        return DSocketHeader_Codi(header.exchange_data())
    if header.messagetype == DSocketMessagesType.EVENT:
        return DSocketHeader_Event(header.exchange_data())
    if header.messagetype == DSocketMessagesType.CMD_NOW:
        return DSocketHeader_CmdNow(header.exchange_data())
    if header.messagetype == DSocketMessagesType.CMD:
        return DSocketHeader_Cmd(header.exchange_data())
    if header.messagetype == DSocketMessagesType.CMD_NOW_RESPONSE:
        return DSocketHeader_CmdNowResponse(header.exchange_data())
    if header.messagetype == DSocketMessagesType.CMD_RESPONSE:
        return DSocketHeader_CmdResponse(header.exchange_data())
    if header.messagetype == DSocketMessagesType.ACK:
        return DSocketHeader_Ack(header.exchange_data())
    if header.messagetype == DSocketMessagesType.ERROR:
        return DSocketHeader_Error(header.exchange_data())
    raise DSocketHeaderException(DSocketHeaderException.DSOCKET_INVALID_ERR)


# =============================================================================#
# send data between client/server
def dsocket_send(s: socket, header: DSocketHeaderBasic, rdata: common.RData):
    # sync header with data
    if getattr(header, 'size', None) is not None:
        if rdata is None:
            header.size = 0
        else:
            header.size = len(rdata)
        if getattr(header, 'crc', None) is not None:
            if rdata is None:
                header.crc = 0
            else:
                header.crc = rdata.crc32()
        # create buffer and add header data
        buff = common.RData(header.exchange_data())
        # add body data to buffer, if exist ...
        if header.size != 0:
            buff[len(buff):] = rdata
        # send
        s.sendall(buff)
    else:
        # send only header
        s.sendall(header.exchange_data())


# =============================================================================#
# send data between client/server
def dsocket_send_file(s: socket, header: DSocketHeaderBasic, f: io.FileIO, buffersize=1024):
    # sync header with data
    if getattr(header, 'size', None) is not None:
        if f is None:
            header.size = 0
        else:
            header.size = os.fstat(f.fileno()).st_size
        if getattr(header, 'crc', None) is not None:
            if f is None:
                header.crc = 0
            else:
                header.crc = common.crc32f(f)
        # create buffer and add header data
        buff = common.RData(header.exchange_data())
        # add body data to buffer, if exist ...
        if header.size != 0:
            for chunk in iter(lambda: f.read(buffersize), b''):
                buff[len(buff):] = chunk
        s.sendall(buff)
    else:
        # send only header
        s.sendall(header.exchange_data())


# =============================================================================#
# wait for response data, return header and data  (data can be None if not exist)
def dsocket_listen(s: socket, buffersize=1024) -> (DSocketHeaderBasic, common.RData):
    # get data
    recv_header = socketheader(DSocketHeaderBasic.recv(s))
    # fit to right header type
    recv_size = getattr(recv_header, 'size', None)
    # check for received data
    if recv_size is not None and recv_size != 0:
        # receive data until size ...
        buff = bytearray()
        for chunk in iter(lambda: s.recv(buffersize), b''):
            buff = buff + chunk
            if len(buff) >= recv_size:
                break
        recv_data = common.RData(buff)  # get received data from buffer
        # check received size data
        if len(buff) != recv_size:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        # if header has crc...
        recv_crc = getattr(recv_header, 'crc', None)
        if recv_crc is not None:
            # check crc
            if recv_crc != recv_data.crc32():
                raise DSocketHeaderException(DSocketHeaderException.DSOCKET_CRC_ERR)
    else:
        recv_data = None  # no data received
    return recv_header, recv_data


# =============================================================================#
# wait for response data, return header and data  (data can be None if not exist)
def dsocket_listen_file(s: socket, buffersize=1024) -> (DSocketHeaderBasic, str):
    # get data
    recv_header = socketheader(DSocketHeaderBasic.recv(s))
    # fit to right header type
    recv_size = getattr(recv_header, 'size', None)
    # check for received data
    if recv_size is not None and recv_size != 0:
        # receive data until size ...
        filename = datetime.datetime.now().strftime('TMP_%Y-%m-%d_%H:%M:%S:%f')
        with open(filename, 'wb') as f:
            file_size = 0
            for chunk in iter(lambda: s.recv(buffersize), b''):
                f.write(chunk)
                file_size = file_size + len(chunk)
                if file_size >= recv_size:
                    break
            f.flush()
        # check received size data
        if os.path.getsize(filename) != recv_size:
            raise DSocketHeaderException(DSocketHeaderException.DSOCKET_SIZE_ERR)
        # if header has crc...
        recv_crc = getattr(recv_header, 'crc', None)
        if recv_crc is not None:
            # check crc
            if recv_crc != common.crc32(filename):
                raise DSocketHeaderException(DSocketHeaderException.DSOCKET_CRC_ERR)
    else:
        filename = None
    return recv_header, filename


def print_socket(s: DSocketHeaderBasic):
    if s.messagetype == DSocketMessagesType.ALARM:
        p = DSocketHeader_Alarm(s.exchange_data())
        print(p)
    elif s.messagetype == DSocketMessagesType.BOOT:
        p = DSocketHeader_Boot(s.exchange_data())
        print(p)
