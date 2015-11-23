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
import akka.util.Timeout
import akka.pattern.ask

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.util.concurrent.LinkedBlockingQueue

import psrs.io.Reader
import psrs.io.Writer
import psrs.util.{ ZooKeeper, Barrier }

import scala.collection.JavaConversions._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Sorting

sealed trait Message
case class Initialize(refs: Seq[ActorRef], zookeepers: Seq[String], 
                      config: Config) extends Message
case object Execute extends Message
case class Collect[T](data: T) extends Message
case class Broadcast[T](data: T) extends Message
case class Aggregate[T](data: T) extends Message
case object GetAll extends Message
case class Messages[T](data: T) extends Message
case class Put[T](data: T) extends Message
case class Hermes(messenger: ActorRef) extends Message

trait Worker0 extends Actor with ActorLogging {

  protected val name = self.path.name

  protected var peers = Seq.empty[ActorRef]

  protected var messengers = Seq.empty[ActorRef]

  protected var config: Option[Config] = None

  protected def initialize(refs: Seq[ActorRef], zookeepers: Seq[String], 
                           conf: Config) {
    peers = refs
    config = Option(conf)
  }

  protected def init: Receive = {
    case Initialize(refs, zks, conf) => initialize(refs, zks, conf) 
  }

  protected def rest: Receive = {
    case msg@_ => log.warning("Unknown message: {}", msg)
  }

  override def receive = init orElse rest

}

object Messenger {

  def name(idx: Int) = "messenger" + "_" + idx

  def props[T]: Props = Props(classOf[Messenger[T]])
}

protected[psrs] class Messenger[T] extends Worker0 {

  protected var messages = Seq.empty[T]

  protected def reset = messages = Seq.empty[T]

  protected def msgs: Receive = {
    case GetAll => { sender ! Messages(messages); reset }
    case Put(data) => messages :+= data.asInstanceOf[T]
  }
  
  override def receive = msgs orElse super.receive
}

trait Worker[T] extends Worker0 {

  protected val index = name.indexOf("_") match {
    case -1 => -1 
    case _ => name.split("_")(1).toInt
  }

  protected val master = config match {
    case Some(found) => found.getInt("psrs.master")
    case None => 0
  }

  protected var barrier: Option[Barrier] = None

  protected var reader: Option[Reader] = None

  protected var writer: Option[Writer] = None

  protected val messenger = 
    context.actorOf(Messenger.props[T], Messenger.name(index))

  override def initialize(refs: Seq[ActorRef], zookeepers: Seq[String], 
                           conf: Config) {
    super.initialize(refs, zookeepers, conf)
    peers.foreach { peer => peer ! Hermes(messenger) }
    if(-1 != index) {
      barrier = Option(Barrier.create("/barrier", (peers.length + 1), index,
        zookeepers.map { zk => ZooKeeper.fromString(zk) }
      ))
    }
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

  protected def execute() { }

  protected def exec: Receive = {
    case Execute => execute 
  }

  protected def msgr: Receive = {
    case Hermes(hermes) => messengers :+= hermes
  }

  override def receive = exec orElse msgr orElse super.receive

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
      extends Worker[Seq[Int]] {

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

  protected def readData: Array[Int] = {
    val data = getReader.foldLeft(Array.empty[Int]){ (result, line) => 
      result :+ line.toInt 
    }
    Sorting.quickSort(data)
    data
  }

  protected def sampling(data: Array[Int]): (Double, Seq[Int]) = {
    val chunk = Math.ceil(data.length.toDouble / (peers.length.toDouble + 1d))
    var sampled = Seq.empty[Int]
    for(idx <- 0 until data.length) if(0 == idx % chunk) sampled :+= data(idx) 
    (chunk, sampled)
  }

  protected def collect(data: Seq[Int]) = messengers.find( hermes => 
    hermes.path.name.contains(master) 
  ) match {
    case Some(remote) => remote ! Put(data)  
    case None => messenger ! Put(data)
  }

  protected def waitFor: Seq[Seq[Int]] = {
    log.info("Retrieve data from messenger ...")
    implicit val timeout = Timeout(5 seconds)
    val future = ask(messenger, GetAll).mapTo[Messages[Seq[Seq[Int]]]] 
    Await.result(future, 5 second).data
  }

  protected def findPivotal(chunk: Double): Seq[Int] = {
    var pivotal = Seq.empty[Int] 
    if(index == master) {
      waitFor match {
        case data if data.isEmpty => log.warning("No data in messenger!")
        case data@_ => { 
          val flatdata = data.flatten.toArray[Int]
          Sorting.quickSort(flatdata)
          for(idx <- 0 until flatdata.length) {
            if(0 == idx % chunk) pivotal :+= flatdata(idx)
          }
          pivotal = pivotal.tail
        }
      }
    } else log.info("Non master worker {} simply goes to sync fn ...", index)
    pivotal 
  }

  protected def dispatch(pivotal: Seq[Int]) = { 
    messengers.map { messenger => if(!pivotal.isEmpty) messenger ! Put(pivotal)}
    messenger ! Put(pivotal)
  }

  override def execute() { 
    val data = readData    
    log.info("Sorted data {}", data.mkString("<", ", ", ">"))
    getBarrier.sync
    val (chunk, sampled) = sampling(data)
    log.info("Sampled: {}, chunk: {}", sampled, chunk)
    getBarrier.sync({ step => collect(sampled) })
    val pivotal = findPivotal(chunk)
    log.info("Pivotal: {}", pivotal)
    getBarrier.sync({ step => dispatch(pivotal) })
/*
    var result = Array.empty[Array[T]]
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
        self ! Aggregate[Array[T]](result(idx)) else 
        ref ! Aggregate[Array[T]](result(idx)) 
      }
    }})
    Sorting.quickSort(aggregated)
    log.info("Aggregated result: {}", aggregated)
    getWriter.write(aggregated.mkString(",")+"\n").close
*/
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

protected[psrs] class DefaultController(conf: Config) extends Worker0 {

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
