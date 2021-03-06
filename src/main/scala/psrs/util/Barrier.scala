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
package psrs.util

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier
import org.apache.curator.retry.RetryNTimes

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.KeeperException._

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ZooKeeper {

  def fromString(value: String): ZooKeeper = {
    val ary = value.split(":")
    require(2 == ary.length, "Invalid ZooKeeper server connect string: "+value)
    ZooKeeper(ary(0), ary(1).toInt)
  }

}

protected[psrs] case class ZooKeeper(host: String = "localhost", 
                                     port: Int = 2181) {

  require(0 < port, "Invalid port value: "+port)

  override def toString(): String = host + ":" + port
}

object Barrier {

  def create(root: String, nrPeers: Int, id: Int, targets: Seq[ZooKeeper]): 
      Barrier = root match {
    case null | "" => throw new IllegalArgumentException("Invalid root path!")
    case _ => {
      require(root.startsWith("/"), "Root path not starts with '/'!") 
      val servers = targets.map(_.toString).mkString(",")
      val curator = CuratorFrameworkFactory.builder.
        sessionTimeoutMs(3*60*1000).retryPolicy(new RetryNTimes(3, 1000)).
        connectString(servers).build
      curator.start
      new DefaultBarrier(curator, root, nrPeers, id)
    }
  }

}

trait Barrier {

  def sync()

  def sync(f: (Int) => Unit)

  def currentStep: Int

  def close()

}

protected[psrs] class DefaultBarrier(curator: CuratorFramework, 
                                     root: String, 
                                     nrPeers: Int,
                                     id: Int) extends Barrier {

  val log = LoggerFactory.getLogger(classOf[DefaultBarrier])

  protected[psrs] var step: Int = 0

  override def currentStep: Int = step

  override def sync() = sync({ s => })

  protected[psrs] def retry(times: Int, path: String, f: String => Unit) {
    if(0 < times) curator.checkExists.forPath(path) match {
      case s:Stat => log.debug("Stat at path "+path+" is "+ s)
      case _ => try {
        f(path)
      } catch {
        case nee: NodeExistsException => retry( times - 1, path, { p => 
          f(path) 
        })
        case e : Exception => log.error("Unable to create znode: "+path, e)
      }
    } 
  }
  
  override def sync(f: (Int) => Unit) {
    val path = root+"/"+step
    retry(nrPeers, path, { p => 
      curator.create.creatingParentsIfNeeded.forPath(p) 
    })
    log.info("Worker "+id+" creates a double barrier instance at "+ path+
             " with total "+nrPeers+" peers.")
    val barrier = new DistributedDoubleBarrier(curator, path, nrPeers)
    barrier.enter
    f(step)
    step += 1
    barrier.leave
  }

  override def close() = curator.close
}  
