import pyca
import datetime
import threading

def now():
  """ 
  Return string with current date and time
  """
  now = datetime.datetime.now()
  return "%04d-%02d-%02d %02d:%02d:%02d.%03d" % (now.year, now.month, now.day,
                                                 now.hour, now.minute, now.second,
                                                 int(now.microsecond/1e3))

def set_numpy(use_numpy):
    """
    The choice to use numpy as the default handling for arrays

    Parameters
    ----------
    use_numpy: bool
        True means numpy will be used
    """
    pyca.set_numpy(use_numpy)


def _check_condition(fn, condition):
    """
    Check an arbitrary condition with against an arbitrary function
    """
    def inner():
        ok = condition()
        try:
            return fn(ok)
        except TypeError:
            return ok
    return inner

def _any_condition(condition):
    """
    Check that any element in an array meets an arbitrary condition
    """
    return self._check_condition(any,condition)


def _all_condition(condition):
    """
    Check that all elements in an array meets an arbitrary condition
    """
    return self._check_condition(all,condition)



class TimeoutSem(object):
    """
    Context manager/wrapper for semaphores, with a timeout on the acquire call.
    Timeout < 0 blocks indefinitely.

    Usage:
    .. code::
    with TimeoutSem(<Semaphore or Lock>, <timeout>): 
        <code block>
    """
    def __init__(self, sem, timeout=-1):
        self.sem = sem
        self.timeout = timeout

    def __enter__(self):
        self.acq = False
        if self.timeout < 0:
            self.acq = self.sem.acquire(True)
        elif self.timeout == 0:
            self.acq = self.sem.acquire(False)
        else:
            self.tmo = threading.Timer(self.timeout, self.raise_tmo)
            self.tmo.start()
            self.acq = self.sem.acquire(True)
            self.tmo.cancel()
        if not self.acq:
            self.raise_tmo()

    def __exit__(self, type, value, traceback):
        try:
            if self.acq:
                self.sem.release()
        except threading.ThreadError:
            pass
        try:
            self.tmo.cancel()
        except AttributeError:
            pass

    def raise_tmo(self):
        raise threading.ThreadError("semaphore acquire timed out")
