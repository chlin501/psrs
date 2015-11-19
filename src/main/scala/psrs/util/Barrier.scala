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
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier

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

  def create(parentPath: String, nrPeers: Int, targets: Seq[ZooKeeper]): 
      Barrier = {
    val servers = targets.map(_.toString).mkString(",")
    val curator = CuratorFrameworkFactory.builder.
                                          sessionTimeoutMs(3*60*1000).
                                          retryPolicy(new RetryNTimes(3, 1000)).
                                          connectString(servers).build
    new DefaultBarrier(curator, parentPath, nrPeers: Int)
  }

}

trait Barrier {

  def sync()

  def sync(f: (Int) => Unit)

}

protected[psrs] class DefaultBarrier(curator: CuratorFramework, 
                                     parentPath: String, 
                                     nrPeers: Int) extends Barrier {

  protected[psrs] var step: Int = 0

  override def sync() = sync({ step => })
  
  override def sync(f: (Int) => Unit) {
    val barrier = new DistributedDoubleBarrier(curator, parentPath+"/"+step, 
                                               nrPeers)
    barrier.enter
    f(step)
    barrier.leave
  }
}  
