/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.tpch.util

import java.io.File
import java.nio.charset.StandardCharsets

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

class TPCHRunner(val spark: SparkSession) {

  def runTPCHQuery(caseId: Int, roundId: Int, explain: Boolean = false): Unit = {
    val path = "tpch-queries/q" + caseId + ".sql";
    val absolute = TPCHRunner.locateResourcePath(path)
    val sql = FileUtils.readFileToString(new File(absolute), StandardCharsets.UTF_8)
    println("Running TPC-H query %d (round %d)... ".format(caseId, roundId))
    val df = spark.sql(sql)
    if (explain) {
      df.explain(extended = false)
    }
    df.show(100)
  }
}

object TPCHRunner {

  private def locateResourcePath(resource: String): String = {
    classOf[TPCHRunner].getClassLoader.getResource("")
        .getPath.concat(File.separator).concat(resource)
  }

  private def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }
}
