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
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.Props
import akka.remote.RemoteScope
import akka.remote.AssociatedEvent

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File

import psrs.io.Reader
import psrs.io.Writer
import psrs.util.{ ZooKeeper, Barrier }

import scala.util.Sorting
import scala.collection.JavaConversions._

sealed trait Message
case class Initialize(refs: Seq[ActorRef], zookeepers: Seq[String], 
                      config: Config) extends Message
case object Execute extends Message
case class Collect[T](data: T) extends Message
case class Broadcast[T](data: T) extends Message
case class Aggregate[T](data: T) extends Message
case object Ready extends Message

trait Worker extends Actor with ActorLogging {

  protected val name = self.path.name

  protected val index = name.indexOf("_") match {
    case -1 => -1 
    case _ => name.split("_")(1).toInt
  }

  protected var peers = Seq.empty[ActorRef]

  protected var config: Option[Config] = None

  protected var barrier: Option[Barrier] = None

  protected var reader: Option[Reader] = None

  protected var writer: Option[Writer] = None

  protected var collected = Array.empty[Int]

  protected var broadcasted = Array.empty[Int]

  protected var aggregated = Array.empty[Int]

  protected def initialize(refs: Seq[ActorRef], zookeepers: Seq[String], 
                           conf: Config) {
    peers = refs
    log.info("Peers contain {}", peers)
    config = Option(conf)
    barrier = Option(Barrier.create("/barrier", peers.length,
      zookeepers.map { zk => ZooKeeper.fromString(zk) }
    ))
  }



  protected def getReader: Reader = reader match {
    case Some(r) => r
    case None => throw new RuntimeException("Reader not initialized!")
  }

  protected def getWriter: Writer = writer match {
    case Some(w) => w
    case None => throw new RuntimeException("Writer not initialized!")
  }

  protected def getBarrier: Barrier = barrier match {
    case Some(b) => b
    case None => throw new RuntimeException("Barrier not initialized!")
  }

  protected def init: Receive = {
    case Initialize(refs, zks, conf) => initialize(refs, zks, conf) 
  }

  protected def execute() { }

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

  protected def ready(from: ActorRef) { }

  protected def isReady: Receive = {
    case Ready => ready(sender)
  }

  def rest: Receive = {
    case msg@_ => log.warning("Unknown message: {}", msg)
  }

  override def receive = init orElse exec orElse collect orElse broadcast orElse aggregate orElse isReady orElse rest

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

  def props(ctrl: ActorRef, systemName: String, host: String, port: Int,
            protocol: Protocol = Remote): Props =
    Props(classOf[DefaultWorker], ctrl, host, port).withDeploy(Deploy(scope =
      RemoteScope(Address(protocol.toString, systemName, host, port))
    ))
}

protected[psrs] class DefaultWorker(ctrl: ActorRef, host: String, port: Int) 
      extends Worker {

  import Worker._

  override def initialize(refs: Seq[ActorRef], zookeepers: Seq[String], 
                           conf: Config) {
    super.initialize(refs, zookeepers, conf)
    val inputDir = conf.getString("psrs.input-dir")
    val input = inputDir+"/"+host+"_"+port+".txt"
    log.info("Input data is from {}", input)
    reader = Option(Reader.fromFile(input))
    val outDir = conf.getString("psrs.output-dir")
    val output = host+"_"+port+".txt"
    log.info("Output data to {}/{}", outDir, output)
    writer = Option(Writer.withFile(outDir, output))
  }

  override def execute() { 
    log.info("Start reading data ...")
    val data = getReader.foldLeft(Array.empty[Int]){ (result, line) => 
      result :+ line.toInt 
    }
    Sorting.quickSort(data)
    log.info("Sorted data {}", data.mkString("<", ", ", ">"))
    getBarrier.sync
    log.info("1. Current step {}", getBarrier.currentStep)
    val chunk = Math.ceil(data.length.toDouble / (peers.length.toDouble + 1d))
    var sampled = Seq.empty[Int]
    for(idx <- 0 until data.length) if(0 == idx % chunk) sampled :+= data(idx) 
    getBarrier.sync({ step => peers.map { peer => at(peer.path.name) match {
      case 0 => peer ! Collect[Array[Int]](sampled.toArray[Int])
      case _ =>
    }}})
    log.info("2. Current step {}", getBarrier.currentStep)
    var pivotal = Seq.empty[Int]
    if(!collected.isEmpty) {
      Sorting.quickSort(collected)
      for(idx <- 0 until collected.length) 
        if(0 == idx % chunk) pivotal :+= collected(idx)
      pivotal = pivotal.tail
    }
    log.info("Pivotal: {}", pivotal)
    getBarrier.sync({ step => peers.map { peer => if(!pivotal.isEmpty)
      peer ! Broadcast[Array[Int]](pivotal.toArray[Int])
    }})
    log.info("3. Current step {}", getBarrier.currentStep)
    var result = Array.empty[Array[Int]]
    if(!broadcasted.isEmpty) {
      val pivotals = 0 +: broadcasted :+ data.last
      for(idx <- 0 until pivotals.length) if((pivotals.length - 1) != idx) 
        result :+= data.filter( e => e > broadcasted(idx) && 
          e < broadcasted(idx +1))
    }
    log.info("Result: {}", result)
    getBarrier.sync({ step => if(result.length == (peers.length + 1) ) {
      val all = peers.patch(index, Seq(self), 0)
      all.zipWithIndex.foreach { case (ref, idx) => if(ref.equals(self))
        self ! Aggregate[Array[Int]](result(idx)) else 
        ref ! Aggregate[Array[Int]](result(idx)) 
      }
    }})
    log.info("4. Current step {}", getBarrier.currentStep)
    Sorting.quickSort(aggregated)
    log.info("Aggregated result: {}", aggregated)
    getWriter.write(aggregated.mkString(",")+"\n").close
  }

  override def receive = super.receive
}

object Controller {

  protected[psrs] case class Options(host: String = "localhost", 
                                     port: Int = 10000)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Options]("controller") {
      head("controller", "0.1")     
      opt[String]('h', "host") required() valueName("<host>") action { 
        (h, opts) => opts.copy(host = h) 
      } text("host of the controller") 
      opt[Int]('p', "port") required() valueName("<port>") action { 
        (p, opts) => opts.copy(port = p) 
      } text("port of the controller") 
    }    
    parser.parse(args, Options()) match {
      case Some(opts) => {
        val conf = ConfigFactory.load("controller")
        val system = ActorSystem(conf.getString("psrs.system-name"), conf)
        system.actorOf(props(conf), name)
      }
      case None => 
    }
  }

  def props(conf: Config): Props = conf match {
    case null => throw new IllegalArgumentException("Config is missing!")
    case _ => Props(classOf[DefaultController], conf)
  }

  def name: String = classOf[DefaultController].getSimpleName

}

protected[psrs] class DefaultController(conf: Config) extends Worker {

  import Controller._

  protected var workers = Seq.empty[ActorRef]

  protected def systemName: String = conf.getString("psrs.system-name")

  protected def protocol: Protocol = conf.getString("psrs.protocol") match {
    case "akka" => Local
    case "akka.tcp" => Remote
    case s@_ => throw new RuntimeException("Invalid protocol: "+s)
  }

  override def preStart() = {
    conf.getStringList("psrs.workers").zipWithIndex.map { case (e, idx) => 
      val ary = e.split(":")
      val host = ary(0)
      val port = ary(1).toInt
      log.info("Initialize worker #"+idx+" at host: "+host+" port: "+port)
      workers +:= context.actorOf (
        Worker.props(self, systemName, host, port, protocol), Worker.name(idx)
      )
    }
    log.info("Worker size: {}", workers.size)
    workers.foreach { worker => 
      log.info("Instruct {} starting execution ...", worker.path.name)
      val zookeepers = conf.getStringList("psrs.zookeepers").toSeq
      worker ! Initialize(workers diff Seq(worker), zookeepers, conf) 
      worker ! Execute
    }
  }

  override def receive = super.receive
  
}
