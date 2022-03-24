from threading import Event


class Sleep(object):
    def __init__(self, seconds, immediate=True):
        self.sleepevent = Event()
        self.seconds = seconds
        if immediate:
            self.sleep()

    def sleep(self, seconds=None):
        self.sleepevent.clear()
        self.sleepevent.wait(timeout=self.seconds if seconds is None else seconds)

    def wake(self):
        self.sleepevent.set()
