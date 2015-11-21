#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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

from pyhocon import ConfigFactory
import argparse
import os
import subprocess
import time

def get_args():
  parser = argparse.ArgumentParser(description='Find wokers.')   
  parser.add_argument("-c" , "--conf", dest="conf_path", help="get workers from the config file")
  return parser.parse_args()

def start(workers, log_dir):
  if not os.path.exists(log_dir):
    os.makedirs(log_dir)  
  for worker in workers:
    ary = worker.split(':')
    host = str(ary[0])
    port = str(ary[1])
    cmd = 'nohup sbt "runMain psrs.Container --host {0} --port {1}" > {2}/container_{0}_{1}.log 2>&1 &'.format(host, port, log_dir)
    subprocess.Popen(cmd, shell=True) 
    time.sleep(1)

if __name__ == '__main__':
  args = get_args()
  if args.conf_path is not None:
    conf = ConfigFactory.parse_file(args.conf_path)
    workers = conf['psrs.workers']
    log_dir = conf['psrs.log-dir']
    start(workers, log_dir)
  else:
    print "error: application.conf is not supplied!"
