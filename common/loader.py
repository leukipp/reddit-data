from threading import Thread, Event

from helper.sleep import Sleep
from common.store import Store
from common.logger import Logger


class Loader(Thread, Logger, Store):
    def __init__(self, name, root, config, subreddit):
        Thread.__init__(self, name=name)
        Logger.__init__(self, name=name, context=f'r/{subreddit}', plain=False)
        Store.__init__(self, name=name, root=root, config=config, subreddit=subreddit)

        # thread events
        self.runevent = Event()
        self.stopevent = Event()

        # time helpers
        self.time = Sleep(10, immediate=False)

    def running(self):
        return self.runevent.is_set()

    def stopped(self):
        return self.stopevent.is_set()

    def alive(self):
        return self.is_alive()

    def run(self):
        raise NotImplementedError()

    def stop(self, timeout=None):
        self.stopevent.set()
        self.time.wake()

        while self.running():
            Sleep(0.1)

        if self.alive():
            self.join(timeout)
