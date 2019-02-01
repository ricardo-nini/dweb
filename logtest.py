#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging.config
import threading
import time
import rlib.common as common


def stop_teste(event:threading.Event):
    time.sleep(5)
    event.set()


# =============================================================================#
if __name__ == '__main__':
    e = threading.Event()

    logging.config.fileConfig('dweb.log.ini')
    log = logging.getLogger(__name__)
    log.debug('Teste')
    common.string2bool('true')

    p = threading.Thread(target=stop_teste, args=(e,))
    p.start()

    while not e.is_set():
        print('Passei')
        e.wait(30)
