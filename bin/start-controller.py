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

def get_args():
  parser = argparse.ArgumentParser(description='Find controller.')   
  parser.add_argument("-c" , "--conf", dest="conf_path", help="get controller from the config file")
  return parser.parse_args()

def start(host, port, log_dir):
  if not os.path.exists(log_dir):
    os.makedirs(log_dir)  
  cmd = 'nohup sbt "runMain psrs.Controller --host {0} --port {1}" > {2}/controller_{0}_{1}.log 2>&1 &'.format(host, port, log_dir)
  subprocess.Popen(cmd, shell=True) 

if __name__ == '__main__':
  args = get_args()
  if args.conf_path is not None:
    conf = ConfigFactory.parse_file(args.conf_path)
    host = conf['akka.remote.netty.tcp.hostname'] 
    port = conf['akka.remote.netty.tcp.port'] 
    log_dir = conf['psrs.log-dir'] 
    start(host, port, log_dir)
  else:
    print "error: application.conf is not supplied!"
