# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
