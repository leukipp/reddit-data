from threading import Event


class Sleep(object):
    def __init__(self, seconds, immediate=True):
        self._sleepevent = Event()
        self._seconds = seconds
        if immediate:
            self.sleep()

    def sleep(self, seconds=None):
        self._sleepevent.clear()
        self._sleepevent.wait(timeout=self._seconds if seconds is None else seconds)

    def wake(self):
        self._sleepevent.set()
