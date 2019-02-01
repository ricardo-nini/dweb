#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging.config
import threading
import os
import sys
import time
import rlib.rdaemon as rdaemon


class Srvtest(rdaemon.Daemonize):
    def __init__(self, app, pid, privileged_action=None,
                 user=None, group=None, verbose=False, logger=None,
                 foreground=False, chdir="/"):
        super().__init__(app, pid, privileged_action, user, group, verbose, logger,
                         foreground, chdir)
        self._event = threading.Event()
        self.logger = logging.getLogger(__name__)

    def stop(self):
        self._event.set()

    def is_running(self):
        return not self._event.is_set()

    def run(self, *args):
        while self.is_running():
            print('Teste')
            self.syslogger.warn('Entrando no sleep.')
            time.sleep(900)
            self.syslogger.warn('Saindo do sleep.')
            self._event.wait(300)


# =============================================================================#
if __name__ == '__main__':
    PATH = os.path.dirname(os.path.abspath(sys.argv[0]))
    d = Srvtest(app='srvtest', pid='/var/run/srvtest.pid', foreground=False, chdir=PATH)
    getattr(d, 'start')()
