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
import akka.actor.ActorRef
import akka.actor.ActorLogging
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.Props
import akka.remote.RemoteScope

sealed trait Message
case class Initialize(refs: Seq[ActorRef]) extends Message
case object Execute extends Message

trait Worker extends Actor with ActorLogging {

  protected var peers = Seq.empty[ActorRef]

  protected def initialize(refs: Seq[ActorRef]) = peers = refs

  protected def init: Receive = {
    case Initialize(refs) => initialize(refs) 
  }

  protected def execute()  

  protected def exec: Receive = {
    case Execute => execute 
  }

  def unknown: Receive = {
    case msg@_ => log.warning("Unknown message: {}", msg)
  }

  override def receive = init orElse exec orElse unknown
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
    Props(classOf[DefaultWorker]).withDeploy(Deploy(scope =
      RemoteScope(Address(protocol.toString, systemName, host, port))
    ))
}

protected[psrs] class DefaultWorker extends Worker {

  override def execute() { 

  }

  override def receive = super.receive
}
