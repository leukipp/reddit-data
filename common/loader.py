from threading import Thread, Event
from datetime import datetime, timezone

from common.sleep import Sleep


class Loader(Thread):
    def __init__(self, name):
        Thread.__init__(self, name=name)

        self._name = name
        self._runevent = Event()
        self._stopevent = Event()
        self._time = Sleep(10, immediate=False)

    def _log(self):
        return f'{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} | {self._name.ljust(9)} | '

    def log(self, msg):
        print(self._log() + msg)

    def background(self):
        return self.isAlive()

    def running(self):
        return self._runevent.is_set()

    def stopped(self):
        return self._stopevent.is_set()

    def run(self):
        raise NotImplementedError

    def stop(self, timeout=None):
        raise NotImplementedError
