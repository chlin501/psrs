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
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object Starter {

  val log = LoggerFactory.getLogger(classOf[Starter])

  def main(args: Array[String]): Unit = create(ConfigFactory.load)

  def create(config: Config): Starter = {
    val starter = new Starter(config) 
    starter.initialize
    starter
  }
}

protected[psrs] class Starter(config: Config) {

  import Starter._

  protected[psrs] var system: Option[ActorSystem] = None

  protected[psrs] var workers = Seq.empty[ActorRef]

  protected[psrs] def initialize() = system match {
    case None => {
      system = Option(ActorSystem("PSRS", config))
      config.getStringList("psrs.workers").zipWithIndex.map { case (e, idx) => 
        val ary = e.split(":")
        val host = ary(0)
        val port = ary(1).toInt
        log.debug("Initialize worker #"+idx+" at host: "+host+" port: "+port)
        system.map { sys => workers +:= 
          sys.actorOf(WorkerImpl.props(host, port), WorkerImpl.name(idx)) 
        }
      }
      deploy
    }
    case _ => { log.error("Fail initializing the system!"); System.exit(-1) }
  }

  def deploy = workers.foreach { worker => }
}

trait Worker extends Actor with ActorLogging {

  def unknown: Receive = {
    case msg@_ => log.warning("Unknown message: {}", msg)
  }
}

object WorkerImpl {

  def name(idx: Int) = classOf[WorkerImpl].getSimpleName + idx

  def props(host: String, port: Int): Props = 
    Props(classOf[WorkerImpl], host, port)
}

protected[psrs] class WorkerImpl(host: String, port: Int) extends Worker {

  override def receive = unknown
}
