#!/usr/bin/env python

import pyca
from Pv import Pv

import sys

from options import Options

if __name__ == '__main__':
  options = Options(['pvnames'], ['timeout'], [])
  try:
    options.parse()
  except Exception, msg:
    options.usage(str(msg))
    sys.exit()

  pvnames = options.pvnames.split()

  if options.timeout is not None:
    timeout = float(options.timeout)
  else:
    timeout = 1.0

  states = ["never connected", "previously connected", "connected", "closed"]
  access = ['none', 'read only', 'write only', 'read-write']

  for pvname in pvnames:
    try:
      pv = Pv(pvname)
      pv.connect(timeout)
      print pv.name
      print '  State: ', states[pv.state()]
      print '  Host:  ', pv.host()
      print '  Access:', access[pv.rwaccess()]
      print '  Type:  ', pv.type()
      print '  Count: ', pv.count()
    except pyca.pyexc, e:
      print 'pyca exception: %s' %(e)
    except pyca.caexc, e:
      print 'channel access exception: %s' %(e)
