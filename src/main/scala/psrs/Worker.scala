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

import com.typesafe.config.Config

import java.io.File

import psrs.io.Reader
import psrs.util.{ ZooKeeper, Barrier }

import scala.util.Sorting

sealed trait Message
case class Initialize(refs: Seq[ActorRef], zookeepers: Seq[String], 
                      config: Config) extends Message
case object Execute extends Message
case class Collect[T](data: T) extends Message
case class Broadcast[T](data: T) extends Message
case class Aggregate[T](data: T) extends Message

trait Worker extends Actor with ActorLogging {

  protected val name = self.path.name

  protected val index = name.split("_")(1).toInt

  protected var peers = Seq.empty[ActorRef]

  protected var config: Option[Config] = None

  protected var barrier: Option[Barrier] = None

  protected var reader: Option[Reader] = None

  protected var collected = Array.empty[Int]

  protected var broadcasted = Array.empty[Int]

  protected var aggregated = Array.empty[Int]

  protected def initialize(refs: Seq[ActorRef], zookeepers: Seq[String], 
                           conf: Config) {
    peers = refs
    config = Option(conf)
    barrier = Option(Barrier.create("/barrier", peers.length,
      zookeepers.map { zk => ZooKeeper.fromString(zk) }
    ))
    val inputDir = conf.getString("psrs.input-dir")
    val input = inputDir+"/"+host+"_"+port+".txt"
    log.info("Input data is from {}", input)
    reader = Option(Reader.fromFile(input))
  }

  protected def getReader: Reader = reader match {
    case Some(r) => r
    case None => throw new RuntimeException("Reader not initialized!")
  }

  protected def host: String = self.path.address.host match {
    case Some(h) => h
    case None => "localhost"
  }

  protected def port: Int = self.path.address.port match {
    case Some(p) => p
    case None => 20000
  }

  protected def init: Receive = {
    case Initialize(refs, zks, conf) => initialize(refs, zks, conf) 
  }

  protected def execute()  

  protected def exec: Receive = {
    case Execute => execute 
  }

  protected def collect: Receive = {
    case Collect(data) => collected ++= data.asInstanceOf[Array[Int]]
  }

  protected def broadcast: Receive = {
    case Broadcast(data) => broadcasted ++= data.asInstanceOf[Array[Int]]
  }

  protected def aggregate: Receive = {
    case Aggregate(data) => aggregated ++= data.asInstanceOf[Array[Int]]
  }

  def unknown: Receive = {
    case msg@_ => log.warning("Unknown message: {}", msg)
  }

  override def receive = init orElse exec orElse collect orElse broadcast orElse aggregate orElse unknown

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

  override def execute() { 
    log.info("Start reading data ...")
    val data = getReader.foldLeft(Array.empty[Int]){ (result, line) => 
      result :+ line.toInt 
    }
    Sorting.quickSort(data)
    log.info("sorted data {}", data)
    barrier.map(_.sync({ step => log.info("Sync at step {} ...", step) }))
    val chunk = Math.ceil(data.length.toDouble / (peers.length.toDouble + 1d))
    var sampled = Seq.empty[Int]
    for(idx <- 0 until data.length) if(0 == idx % chunk) sampled :+= data(idx) 
    barrier.map(_.sync({ step => peers.map { peer => at(peer.path.name) match {
      case 0 => peer ! Collect[Array[Int]](sampled.toArray[Int])
      case _ =>
    }}}))
    var pivotal = Seq.empty[Int]
    if(!collected.isEmpty) {
      Sorting.quickSort(collected)
      for(idx <- 0 until collected.length) 
        if(0 == idx % chunk) pivotal :+= collected(idx)
      pivotal = pivotal.tail
    }
    log.info("Pivotal: {}", pivotal)
    barrier.map(_.sync({ step => peers.map { peer => if(!pivotal.isEmpty)
      peer ! Broadcast[Array[Int]](pivotal.toArray[Int])
    }}))
    var result = Array.empty[Array[Int]]
    if(!broadcasted.isEmpty) {
      val pivotals = 0 +: broadcasted :+ data.last
      for(idx <- 0 until pivotals.length) if((pivotals.length - 1) != idx) 
        result :+= data.filter( e => e > broadcasted(idx) && 
          e < broadcasted(idx +1))
    }
    log.info("Result: {}", result)
    barrier.map(_.sync({ step => if(result.length == (peers.length + 1) ) {
      val all = peers.patch(index, Seq(self), 0)
      all.zipWithIndex.foreach { case (ref, idx) => if(ref.equals(self))
        self ! Aggregate[Array[Int]](result(idx)) else 
        ref ! Aggregate[Array[Int]](result(idx)) 
      }
    }}))
    Sorting.quickSort(aggregated)
    // TODO: write to local file
  }

  override def receive = super.receive
}
