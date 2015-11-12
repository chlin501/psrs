/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package psrs

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.Props
import akka.remote.RemoteScope

trait Worker extends Actor with ActorLogging {

  def unknown: Receive = {
    case msg@_ => log.warning("Unknown message: {}", msg)
  }
}

trait Protocol
case object Remote extends Protocol {
  override def toString(): String = "akka.tcp"
}
case object Local extends Protocol {
  override def toString(): String = "akka"
}

object Worker {

  def name(idx: Int) = classOf[DefaultWorker].getSimpleName + idx

  def props(systemName: String, host: String, port: Int, 
            protocol: Protocol = Remote): Props =
    Props(classOf[DefaultWorker], host, port).withDeploy(Deploy(scope =
      RemoteScope(Address(protocol.toString, systemName, host, port))
    ))
}

protected[psrs] class DefaultWorker(host: String, port: Int)
      extends Worker with Computation {

  override def execute() { }

  def read(file: String): Array[Int] = Array.empty[Int]

  def quickSort(ary: Array[Int]) { }

  def sampling(ary: Array[Int]): Array[Int] = Array.empty[Int]

  override def receive = unknown
}
