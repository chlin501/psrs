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

import java.io.File
import java.io.PrintWriter

trait Writer {

  type Line = String

  def write(line: Line): Writer

  def close

}

object Writer {

  def withFile(dir: String, file: String): Writer = dir match {
    case null | "" => throw new IllegalArgumentException("Invalid output dir!")
    case _ => file match {
      case null | "" => throw new IllegalArgumentException("Invalid path!")
      case _ => {
        val directory = new File(dir)
        if(!directory.exists) directory.mkdirs
        new DefaultWriter(new PrintWriter(directory+"/"+file))
      }
    }
  }

}

protected[psrs] class DefaultWriter(writer: PrintWriter) extends Writer {
  
  override def write(line: String): Writer = { writer.write(line); this }

  override def close = writer.close
  
}  
