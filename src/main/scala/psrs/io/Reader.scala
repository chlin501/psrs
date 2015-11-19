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
package psrs.io

import java.io.FileReader
import java.io.BufferedReader

trait Reader {

  type Line = String

  def foreach(f:(Line) => Unit)

  def foldLeft[R](r: R)(f: (R, Line) => R): R

}

object Reader {

  def fromFile(name: String): Reader = 
    new DefaultReader(new BufferedReader(new FileReader(name)))

}

protected[psrs] class DefaultReader(reader: BufferedReader) extends Reader {
  
  override def foreach(f: (Line) => Unit) {
    var line: Line = reader.readLine 
    while(null != line) {
      f(line)
      line = reader.readLine
    }
  }

  override def foldLeft[R](r: R)(f: (R, Line) => R): R = {
    var result = r 
    foreach { line => 
      result = f(r, line) 
    }
    result
  }

}  
