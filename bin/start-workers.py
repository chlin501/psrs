#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyhocon import ConfigFactory
import argparse
import os
import subprocess

def get_workers(conf_path):
  if None is not conf_path:
    conf = ConfigFactory.parse_file(conf_path)
    return conf['psrs.workers']
  else:
    return None

def get_conf():
  parser = argparse.ArgumentParser(description='Find wokers.')   
  parser.add_argument("-c" , "--conf", dest="conf_path", help="get workers from the config file")
  return parser.parse_args()

def start(workers):
  for worker in workers:
    ret = subprocess.call(['./bin/start-container', str(worker)]) 
    if 0 != ret:
      print "error: fail launcing worker!"
    else: 
      print "worker %s is launched." % worker

if __name__ == '__main__':
  args = get_conf()
  if args.conf_path is not None:
    workers = get_workers(args.conf_path)
    start(workers)
  else:
    print "error: application.conf is not supplied!"
