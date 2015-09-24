import pyca
import threading
import sys
import numpy
import time
import datetime

"""
   Pv module

   
"""

def __printfn(s):
  print s

logprint        = __printfn
DEBUG           = 0
pv_cache        = {}
DEFAULT_TIMEOUT = 1.0

def now():
  """ returns string with current date and time (with millisecond resolution)"""
  now = datetime.datetime.now()
  return "%04d-%02d-%02d %02d:%02d:%02d.%03d" % ( now.year, now.month,now.day,
                     now.hour,now.minute,now.second,int(now.microsecond/1e3))

class Pv(pyca.capv):
  
  def __init__(self, name, **kw):
    pyca.capv.__init__(self, name)
    self.__con_sem = threading.Event()
    self.__init_sem = threading.Event()
    self.connect_cb  = self.__connection_handler
    self.monitor_cb  = self.__monitor_handler
    self.getevt_cb   = self.__getevt_handler
    self.ismonitored   = False
    self.isconnected   = False
    self.isinitialized = False
    self.monitor_append = False
    self.con_cbs = {}
    self.mon_cbs = {}
    self.cbid = 1
    self.timestamps = []
    self.values = []
    try:
      self.count = kw['count']
    except:
      self.count = None
    try:
      self.control = kw['control']
    except:
      self.control = False
    try:
      m = kw['monitor']
      if m == True:
        self.do_monitor = True
      elif m == False:
        self.do_monitor = False
      else:
        self.do_monitor = True
        self.add_monitor_callback(m)
    except:
      self.do_monitor = False
    try:
      self.do_initialize = kw['initialize']
    except:
      self.do_initialize = False
    if self.do_initialize:
      self.connect(None)

  # Channel access callbacks
  def __connection_handler(self, isconnected):
    self.isconnected = isconnected
    if isconnected:
      self.__con_sem.set()
      if self.do_initialize:
        self.get_data(self.control, -1.0, self.count)
        pyca.flush_io()
    else:
      self.__con_sem.clear()
    for (id, cb) in self.con_cbs.items():
      cb(isconnected)

  def __getevt_handler(self, e=None):
    if e == None:
      self.isinitialized = True
      self.do_initialize = False
      self.getevt_cb = None
      if self.do_monitor:
        self.monitor(pyca.DBE_VALUE | pyca.DBE_LOG | pyca.DBE_ALARM,
                     self.control, self.count)
        pyca.flush_io()
      self.__init_sem.set()

  def __monitor_handler(self, e=None):
    if not self.isinitialized:
      self.__getevt_handler(e)
    if self.monitor_append:
      self.values.append(self.value)
      self.timestamps.append(self.timestamp())
    for (id, (cb, once)) in self.mon_cbs.items():
      cb(e)
      if once and e == None:
        self.del_monitor_callback(id)
    if e == None:
      if DEBUG != 0:
        logprint("%s monitoring %s %s" % (now(), self.name, self.timestr()))
        logprint(self.value)
    else:
      logprint("%-30s %s" % (self.name, e))

  def add_connection_callback(self, cb):
    id = self.cbid
    self.cbid += 1
    self.con_cbs[id] = cb
    return id

  def del_connection_callback(self, id):
    del self.con_cbs[id]

  def add_monitor_callback(self, cb, once=False):
    id = self.cbid
    self.cbid += 1
    self.mon_cbs[id] = (cb, once)
    return id

  def del_monitor_callback(self, id):
    del self.mon_cbs[id]

  # Calls to channel access methods
  # Note that these don't call pyca.flush_io()!
  def connect(self, timeout=None):
    self.create_channel()
    if timeout != None:
      tmo = float(timeout)
      if tmo > 0:
        self.__con_sem.wait(tmo)
        if not self.__con_sem.isSet():
          raise pyca.pyexc, "connection timedout for PV %s" % self.name

  def disconnect(self):
    self.clear_channel()

  def monitor(self, mask=pyca.DBE_VALUE | pyca.DBE_LOG | pyca.DBE_ALARM,
              ctrl=None, count=None):
    if not self.isconnected:
      self.connect(DEFAULT_TIMEOUT)
      if not self.isconnected:
        raise pyca.pyexc, "monitor: connection timedout for PV %s" % self.name
    if ctrl == None:
      ctrl = self.control
    if count == None:
      count = self.count
    self.subscribe_channel(mask, ctrl, count)
    self.ismonitored = True

  def unsubscribe(self):
    self.unsubscribe_channel()
    self.ismonitored = False

  def get(self, **kw):
    if DEBUG != 0:
      logprint("caget %s: " % self.name)
    if not self.isconnected:
      self.connect(DEFAULT_TIMEOUT)
      if not self.isconnected:
        raise pyca.pyexc, "get: connection timedout for PV %s" % self.name
    try:
      ctrl = kw['ctrl']
      if ctrl == None:
        ctrl = False
    except:
      ctrl = self.control
    try:
      count = kw['count']
    except:
      count = self.count
    try:
      timeout = kw['timeout']
      if timeout != None:
        tmo = float(timeout)
      else:
        tmo = -1.0
    except:
      tmo = DEFAULT_TIMEOUT
    self.get_data(ctrl, tmo, count)
    if tmo > 0 and DEBUG != 0:
      logprint("got %s\n" % self.value.__str__())
    try:
      if kw['as_string']:
        return str(self.value)
    except:
      pass
    return self.value

  def put(self, value, **kw):
    if DEBUG != 0:
      logprint("caput %s in %s\n" % (value, self.name))
    if not self.isinitialized:
      if self.isconnected:
        self.get_data(self.control, -1.0, self.count)
        pyca.flush_io()
      else:
        self.do_initialize = True
        self.connect()
      self.wait_ready(DEFAULT_TIMEOUT * 2)
    try:
      timeout = kw['timeout']
      if timeout != None:
        tmo = float(timeout)
      else:
        tmo = -1.0
    except:
      tmo = DEFAULT_TIMEOUT
    self.put_data(value, tmo)
    return value

  def get_enum_set(self, timeout=-1.0):
    """
    Only valid for ENUM type PVs and Fields, will throw exception otherwise
    Retrieves the array of valid ENUM String values for this PV
    Array index is ENUM Integer value
    Array is stored in the 'data' member, or is available directly as Pv.enum_set
    """
    tmo = float(timeout)
    self.get_enum_strings(tmo)

  # "Higher level" methods.
  def wait_ready(self, timeout=None):
    pyca.flush_io()
    self.__init_sem.wait(timeout)
    if not self.__init_sem.isSet():
        raise pyca.pyexc, "ready timedout for PV %s" %(self.name)

  # The monitor callback used in wait_condition.
  def __wc_mon_cb(self, e, condition, sem):
    if (e == None) and condition():
      sem.set()
      
  # Returns True if successfully waited, False if timeout.
  def wait_condition(self, condition, timeout=60):
    if not self.ismonitored:
      self.monitor()
      pyca.flush_io()
    sem = threading.Event()
    id = self.add_monitor_callback(lambda e: self.__wc_mon_cb(e, condition, sem),
                                   False)
    sem.wait(timeout)
    self.del_monitor_callback(id)
    return sem.is_set()
    
  def wait_until_change(self, timeout=60):
    result = self.wait_condition(lambda: True, timeout)
    if not result:
      logprint("waiting for pv %s to change timed out" % self.name)
    return result
    
  def wait_for_value(self, value, timeout=60):
    result = self.wait_condition(lambda: self.value == value, timeout)
    if not result:
      logprint("waiting for pv %s to become %s timed out" % (self.name, value))
    return result
    
  def wait_for_range(self, low, high, timeout=60):
    result = self.wait_condition(lambda: (low <= self.value) and (self.value <= high), timeout)
    if not result:
      logprint("waiting for pv %s to become %s timed out" % (self.name, value))
    return result

  def timestamp(self):
    return (self.secs + pyca.epoch, self.nsec)

  def timestr(self):
    """ make a time string (with ns resolution) using PV time stamp """
    ts = time.localtime(self.secs+pyca.epoch)
    tstr = time.strftime("%Y-%m-%d %H:%M:%S", ts)
    tstr = tstr + ".%09d" % self.nsec
    return tstr

  # Start monitoring and/or appending.
  def monitor_start(self, monitor_append=False):
    """ start monitoring for the Pv, new values are added to the `values` 
        list if monitor_append is True """
    if not self.isinitialized:
      if self.isconnected:
        self.get_data(self.control, -1.0, self.count)
        pyca.flush_io()
      else:
        self.do_initialize = True
        self.connect()
      self.wait_ready()
    if self.ismonitored:
      if monitor_append == self.monitor_append:
        return
      if monitor_append:
        self.monitor_clear()
      self.monitor_append = monitor_append
      return
    self.monitor_append = monitor_append
    self.monitor_clear()
    self.monitor()
    pyca.flush_io()

  def monitor_stop(self):
    """ stop  monitoring for the Pv, note that this does not clear the 
        `values` list """
    if self.ismonitored:
      self.unsubscribe()

  def monitor_clear(self):
    """ clear the `values` list """
    self.values = []
    self.timestamps = []

  def monitor_get(self):
    """ retuns statistics for the current `values` list as dictionary """
    a=numpy.array(self.values[1:])
    ret = {}
    if (len(a)==0):
      ret["mean"]=ret["std"]=ret["err"]=numpy.nan
      ret["num"]=0
      if DEBUG != 0:
        logprint("No pulses.... while monitoring %s" % self.name)
      return ret
    ret["mean"]=a.mean()
    ret["std"] =a.std()
    ret["num"] =len(a)
    ret["err"] =ret["std"]/numpy.sqrt(ret["num"])
    if DEBUG != 0:
      logprint("get monitoring for %s" % self.name)
    return ret

  # Re-define getattr method to allow direct access to 'data' dictionary members
  def __getattr__(self, name):
    if self.data.has_key(name):
      return self.data[name]
    else:
      print "%s (%s) has No attribute %s" % (str(self), self.name, name)
      raise AttributeError

# Stand alone routines!

def add_pv_to_cache(pvname):
  if not pvname in pv_cache.keys():
    pv_cache[pvname] = Pv(pvname)
  return pv_cache[pvname]

def monitor_start(pvname, monitor_append=False):
  """ start monitoring for pvname, pvname is added to the cache list """
  add_pv_to_cache(pvname)
  pv_cache[pvname].monitor_start(monitor_append)
  
def monitor_stop(pvname):
  """ stop monitoring for pvname, pvname is added to the cache list """
  add_pv_to_cache(pvname)
  pv_cache[pvname].monitor_stop()
  
def monitor_clear(pvname):
  """ clear the `values` list for pvname, pvname is added to the cache list """
  add_pv_to_cache(pvname)
  pv_cache[pvname].monitor_clear()

def monitor_get(pvname):
  """ returns statistics for pvname, pvname is added to the cache list """
  add_pv_to_cache(pvname)
  return pv_cache[pvname].monitor_get()

def monitor_stop_all(clear=False):
  """ stop monitoring for all PVs defined in cache list """
  for pv in pv_cache.keys():
    pv_cache[pv].monitor_stop()
    if (clear):
      pv_cache[pv].monitor_clear()
    logprint("stopping monitoring for %s" % pv)

def get(pvname,as_string=False):
  """ returns current value for the pvname, if as_string is True values
      are converted to string """
  add_pv_to_cache(pvname)
  return pv_cache[pvname].get(as_string=as_string, timeout=DEFAULT_TIMEOUT)

def put(pvname,value):
  """ write value to pvname; returns the value itself """
  add_pv_to_cache(pvname)
  return pv_cache[pvname].put(value, timeout=DEFAULT_TIMEOUT)

def wait_until_change(pvname,timeout=60):
  """ wait until value of pvname changes (default timeout is 60 sec) """
  pv = add_pv_to_cache(pvname)
  ismon = pv.ismonitored
  if not ismon:
    pv.monitor_start(False)
  pv.wait_until_change(timeout=timeout)
  if not ismon:
    monitor_stop(pvname)

def wait_for_value(pvname,value,timeout=60):
  """ wait until pvname is exactly value (default timeout is 60 sec) """
  pv = add_pv_to_cache(pvname)
  ismon = pv.ismonitored
  if not ismon:
    pv.monitor_start(False)
  pv.wait_until_value(value,timeout=timeout)
  if not ismon:
    monitor_stop(pvname)

def wait_for_range(pvname,low,high,timeout=60):
  """ wait until pvname is exactly between low and high (default timeout is 60 sec) """
  pv = add_pv_to_cache(pvname)
  ismon = pv.ismonitored
  if not ismon:
    pv.monitor_start(False)
  pv.wait_until_range(low, high, timeout=timeout)
  if not ismon:
    monitor_stop(pvname)

def clear():
  """ stop monitoring and disconnect PV, to use as kind of reset """
  for pv in pv_cache:
    monitor_stop(pv)
    monitor_clear(pv)
    pv_cache[pv].disconnect()
  pv_cache.clear()

def what_is_monitored():
  """ print list of PVs that are currently monitored """
  for pv in pv_cache:
    if (pv_cache[pv].ismonitored):
      logprint("pv %s is currently monitored" % pv_cache[pv].name)
