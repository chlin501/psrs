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

import akka.actor.ActorRef
import akka.actor.ActorSystem

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object Controller {

  val log = LoggerFactory.getLogger(classOf[Controller])

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load("controller")
    new Controller(conf).initialize
  }
}

protected[psrs] class Controller(config: Config) {

  import Controller._

  protected[psrs] var system: Option[ActorSystem] = None

  protected[psrs] var workers = Seq.empty[ActorRef]

  protected[psrs] def systemName: String = config.getString("psrs.system-name")

  protected[psrs] def protocol: Protocol = 
    config.getString("psrs.protocol") match {
      case "akka" => Local
      case "akka.tcp" => Remote
      case s@_ => throw new RuntimeException("Invalid protocol: "+s)
    }

  protected[psrs] def initialize() = system match {
    case None => {
      system = Option(ActorSystem(config.getString("psrs.system-name"), config))
      config.getStringList("psrs.workers").zipWithIndex.map { case (e, idx) => 
        val ary = e.split(":")
        val host = ary(0)
        val port = ary(1).toInt
        log.debug("Initialize worker #"+idx+" at host: "+host+" port: "+port)
        system.map { sys => workers +:= 
          sys.actorOf(Worker.props(systemName, host, port, protocol), 
                      Worker.name(idx))
        }
      }
    }
    case _ => log.error("Fail initializing the system!")
  }
}
