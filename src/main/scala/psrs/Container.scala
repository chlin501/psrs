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

import akka.actor.ActorSystem
import akka.actor.Address

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory

object Container {

  protected[psrs] var system: Option[ActorSystem] = None

  protected[psrs] case class Options(protocol: String = "akka.tcp", 
                                     systemName: String = "psrs", 
                                     host: String = "localhost", 
                                     port: Int = 20000)

  def address(opts: Options) = Address(opts.protocol, opts.systemName, 
    opts.host, opts.port)

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Options]("container") {
      head("container", "0.1")     
      opt[String]('p', "protocol") action { (p, opts) => 
        opts.copy(protocol = p)
      } text("protocol used by the system") 
      opt[String]('s', "system-name") action { (s, opts) => 
        opts.copy(systemName = s) } text("system name") 
      opt[String]('h', "host") required() valueName("<host>") action { 
        (h, opts) => opts.copy(host = h) 
      } text("host on which the computation will run") 
      opt[Int]('p', "port") required() valueName("<port>") action { 
        (p, opts) => opts.copy(port = p) 
      } text("port on which the computation will use") 
    }
    parser.parse(args, Options()) match {
      case Some(opts) => {
        val conf = ConfigFactory.load("container").withValue (
          "akka.remote.netty.tcp.hostname", 
          ConfigValueFactory.fromAnyRef(address(opts))
        ) 
        system match {
          case Some(sys) => 
          case None => system = 
            Option(ActorSystem(conf.getString("psrs.system-name"), conf))
        }
      }
      case None =>
    }
  }
}

protected[psrs] class Container extends Worker {

  override def receive = unknown

}
