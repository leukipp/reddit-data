import datetime


class Timer(object):
    def __init__(self, run=True):
        self.reset(run=run)

    def start(self):
        # start timer
        self._start = Timer.stamp()

    def stop(self, run=True):
        self._stop = Timer.stamp()

        # append total time
        if self._start:
            self._total.append((self._stop - self._start).total_seconds() * 1000)

        # restart timer
        if run:
            self.start()

        # return total time from last run
        return self._total[-1] if len(self._total) else None

    def reset(self, run=True):
        self._start = None
        self._stop = None
        self._total = []

        # restart timer
        if run:
            self.start()

    @staticmethod
    def stamp():
        return datetime.datetime.now()
