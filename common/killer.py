# coding=utf-8
import signal


class Killer(object):
    def __init__(self):
        self._kill_now = False
        signal.signal(signal.SIGINT, self.graceful_exit)
        signal.signal(signal.SIGTERM, self.graceful_exit)

    def graceful_exit(self, signum, frame):
        self._kill_now = True

    @property
    def kill_now(self):
        return self._kill_now


killer = Killer()

if __name__ == "__main__":
    while True:
        print(killer.kill_now)
        import time
        time.sleep(1)
