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

import java.io.File

import psrs.io.Reader
import psrs.util.{ ZooKeeper, Barrier }

import scala.util.Sorting

sealed trait Message
case class Initialize(refs: Seq[ActorRef], zookeepers: Seq[String]) 
      extends Message
case object Execute extends Message
case class Sample[T](data: T) extends Message

trait Worker extends Actor with ActorLogging {

  protected[psrs] val name = self.path.name

  protected[psrs] val index = name.split("_")(1).toInt

  protected var peers = Seq.empty[ActorRef]

  protected var barrier: Option[Barrier] = None

  protected def initialize(refs: Seq[ActorRef], zookeepers: Seq[String]) {
    peers = refs
    barrier = Option(Barrier.create("/barrier", peers.length,
      zookeepers.map { zk => ZooKeeper.fromString(zk) }
    ))
  }

  protected def init: Receive = {
    case Initialize(refs, zks) => initialize(refs, zks) 
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

  def name(idx: Int) = classOf[DefaultWorker].getSimpleName + "_" +idx

  def at(name: String): Int = name.split("_")(0).toInt

  def props(systemName: String, host: String, port: Int,
            protocol: Protocol = Remote): Props =
    Props(classOf[DefaultWorker]).withDeploy(Deploy(scope =
      RemoteScope(Address(protocol.toString, systemName, host, port))
    ))
}

protected[psrs] class DefaultWorker extends Worker {

  import Worker._

  protected[psrs] var reader = Reader.fromFile(
    "/tmp/input_"+host+"_"+port+".txt")

  protected[psrs] var collected = Array.empty[Int]

  protected[psrs] def host: String = self.path.address.host match {
    case Some(h) => h
    case None => "localhost"
  }

  protected[psrs] def port: Int = self.path.address.port match {
    case Some(p) => p
    case None => 20000
  }

  override def execute() { 
    val ary = reader.foldLeft(Array.empty[Int]){ (result, line) => 
      result :+ line.toInt 
    }
    Sorting.quickSort(ary)
    barrier.map(_.sync({ step => log.info("Sync at step {} ...", step) }))
    val chunk = Math.ceil(ary.length.toDouble / (peers.length.toDouble + 1d))
    val sampled = (for(idx <- 0 until ary.length) yield { 
      if(0 == idx % chunk) idx else -1
    }).filter(_ != -1)
    barrier.map(_.sync({ step => peers.map { peer => at(peer.path.name) match {
      case 0 => peer ! Sample[Array[Int]](sampled.toArray[Int])
      case _ =>
    }}}))
    if(!collected.isEmpty) Sorting.quickSort(collected)
    // find pivotal values
    barrier.map(_.sync({ step =>  // boradcast
    }))
  }

  protected def sample: Receive = {
    case Sample(data) => collected ++= data.asInstanceOf[Array[Int]]
  }

  override def receive = sample orElse super.receive
}
