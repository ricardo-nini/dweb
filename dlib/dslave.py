#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import threading
from rlib.common import CONST
from dlib.dconfig import CONFIG


# =============================================================================#
class DRange(object):
    MIN = 0
    MAX = 1
    DELTA = 2

    def __init__(self, ranges: dict):
        self._ranges = ranges
        self.logger = logging.getLogger(__name__)

    def check(self, actual: dict, alarm: False) -> bool:
        ret = False
        for x in self._ranges:
            if x and x in actual and self._ranges[x]:
                if actual[x] > self._ranges[x][DRange.MAX] + self._ranges[x][DRange.DELTA] or actual[x] > \
                                self._ranges[x][DRange.MAX] - self._ranges[x][DRange.DELTA]:
                    if not alarm:
                        self.logger.debug(
                            'Alarm high on {}:{} limit:{:.2f}'.format(x, actual[x], self._ranges[x][DRange.MAX]))
                    ret = True
                elif actual[x] < self._ranges[x][DRange.MIN] + self._ranges[x][DRange.DELTA] or actual[x] < \
                                self._ranges[x][DRange.MIN] - self._ranges[x][DRange.DELTA]:
                    if not alarm:
                        self.logger.debug(
                            'Alarm low on {}:{} limit:{:.2f}'.format(x, actual[x], self._ranges[x][DRange.MIN]))
                    ret = True
        return ret


# =============================================================================#
class DTolerance(object):
    MIN = 0
    MAX = 1
    PERCENT = 2

    def __init__(self, tolerances: dict):
        self._tolerances = tolerances
        self.logger = logging.getLogger(__name__)

    def check(self, previous: dict, actual: dict) -> bool:
        ret = True
        for x in self._tolerances:
            if x is not None and x in previous and x in actual and self._tolerances[x]:
                if actual[x] >= previous[x] + self._htolerance(x, previous[x]):
                    self.logger.debug(
                        'Changed high on {} previous:{:.2f} to actual:{:.2f} limit:{:.2f}'.format(x, previous[x],
                                                                                                  actual[x],
                                                                                                  self._htolerance(x,
                                                                                                                   previous[
                                                                                                                       x])))
                    ret = False
                if actual[x] <= previous[x] - self._ltolerance(x, previous[x]):
                    self.logger.debug(
                        'Changed low on {} previous:{:.2f} to actual:{:.2f} limit:{:.2f}'.format(x, previous[x],
                                                                                                 actual[x],
                                                                                                 self._ltolerance(x,
                                                                                                                  previous[
                                                                                                                      x])))
                    ret = False
        return ret

    def _htolerance(self, name, value):
        if (self._tolerances[name][DTolerance.PERCENT]):
            return value * (self._tolerances[name][DTolerance.MAX] / 100)
        else:
            return self._tolerances[name][DTolerance.MAX]

    def _ltolerance(self, name, value):
        if (self._tolerances[name][DTolerance.PERCENT]):
            return value * (self._tolerances[name][DTolerance.MIN] / 100)
        else:
            return self._tolerances[name][DTolerance.MIN]


# =============================================================================#
class DSlave_Thread(threading.Thread):
    def __init__(self, slave_num: int, resources: dict):
        threading.Thread.__init__(self)
        self.slave_num = slave_num
        self.slave_name = '{}:{}'.format(CONST.SLAVE, slave_num)
        self.id = {}
        self.local = CONFIG.slaves[self.slave_num].local
        self.modbus_slave = CONFIG.slaves[self.slave_num].modbus_slave
        self._resources = resources
        self._stop_event = threading.Event()
        self.logger = logging.getLogger(__name__)

    def run(self):
        raise NotImplementedError

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()
