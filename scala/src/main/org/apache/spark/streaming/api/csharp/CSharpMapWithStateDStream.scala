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

package org.apache.spark.streaming.api.csharp

import java.nio.ByteBuffer

import org.apache.spark.api.python.{PythonBroadcast, PythonRunner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.util.{StateMap, EmptyStateMap}
import org.apache.spark.util.Utils
import org.apache.spark._

import java.io.{ObjectOutputStream, IOException, DataInputStream, DataOutputStream}
import java.util.{Collections, ArrayList => JArrayList, List => JList, Map => JMap}
import org.apache.spark.streaming.rdd.{MapWithStateRDDPartition, MapWithStateRDD, MapWithStateRDDRecord}

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

import scala.language.existentials


class CSharpMapWithStateDStream(
                                 parent: DStream[(Array[Byte], Array[Byte])],
                                 func: Array[Byte],
                                 spec: StateSpecImpl[Array[Byte], Array[Byte], Array[Byte], Array[Byte]],
                                 envVars: JMap[String, String],
                                 pythonIncludes: JList[String],
                                 preservePartitoning: Boolean,
                                 pythonExec: String,
                                 pythonVer: String,
                                 broadcastVars: JList[Broadcast[PythonBroadcast]],
                                 accumulator: Accumulator[JList[Array[Byte]]])

  extends DStream[MapWithStateRDDRecord[Array[Byte], Array[Byte], Array[Byte]]](parent.context) {

  persist(StorageLevel.MEMORY_ONLY)

  private val partitioner = spec.getPartitioner().getOrElse(new HashPartitioner(ssc.sc.defaultParallelism))

  override def slideDuration: Duration = parent.slideDuration

  override def dependencies: List[DStream[_]] = List(parent)

  override def initialize(time: Time): Unit = {
    if (checkpointDuration == null) {
      checkpointDuration = slideDuration * 10
    }
    super.initialize(time)
  }

  override def compute(validTime: Time): Option[CSharpMapWithStateRDD] = {
    // Get the previous state or create a new empty state RDD
    val prevStateRDD = getOrCompute(validTime - slideDuration) match {
      case Some(rdd) => rdd
      case None =>
        CSharpMapWithStateRDD.createFromPairRDD(
          spec.getInitialStateRDD().getOrElse(new EmptyRDD[(Array[Byte], Array[Byte])](ssc.sparkContext)),
          partitioner,
          validTime
        )
    }

    // Compute the new state RDD with previous state RDD and partitioned data RDD
    // Even if there is no data RDD, use an empty one to create a new state RDD
    val dataRDD = parent.getOrCompute(validTime).getOrElse {
      context.sparkContext.emptyRDD[(Array[Byte], Array[Byte])]
    }
    val partitionedDataRDD = dataRDD.partitionBy(partitioner)
    val timeoutThresholdTime = spec.getTimeoutInterval().map { interval =>
      (validTime - interval).milliseconds
    }

    Some(new CSharpMapWithStateRDD(
      prevStateRDD,
      partitionedDataRDD,
      func,
      validTime,
      timeoutThresholdTime,
      envVars,
      pythonIncludes,
      preservePartitoning,
      pythonExec,
      pythonVer,
      broadcastVars,
      accumulator))
  }

  /** Return a pair DStream where each RDD is the snapshot of the state of all the keys. */
  def stateSnapshots(): DStream[(Array[Byte], Array[Byte])] = {
    flatMap { _.stateMap.getAll().map { case (k, s, _) => (k, s) }.toTraversable }
  }
}

private[streaming] object CSharpMapWithStateRDD {

  def createFromPairRDD(
           pairRDD: RDD[(Array[Byte], Array[Byte])],
           partitioner: Partitioner,
           updateTime: Time): CSharpMapWithStateRDD = {

    val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions ({ iterator =>
      val stateMap = StateMap.create[Array[Byte], Array[Byte]](SparkEnv.get.conf)
      iterator.foreach { case (key, state) => stateMap.put(key, state, updateTime.milliseconds) }
      Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[Array[Byte]]))
    }, preservesPartitioning = true)

    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(Array[Byte], Array[Byte])].partitionBy(partitioner)

    // val noOpFunc = (time: Time, key: Array[Byte], value: Option[Array[Byte]], state: State[Array[Byte]]) => None

    new CSharpMapWithStateRDD(
      stateRDD,
      emptyDataRDD,
      null,
      updateTime,
      None,
      null,
      null,
      false,
      null,
      null,
      null,
      null)
  }
}

private[spark] class CSharpMapWithStateRDD (
        private var prevStateRDD: RDD[MapWithStateRDDRecord[Array[Byte], Array[Byte], Array[Byte]]],
        private var partitionedDataRDD: RDD[(Array[Byte],Array[Byte])],
        command: Array[Byte],
        batchTime: Time,
        timeoutThresholdTime: Option[Long],
        envVars: JMap[String, String],
        pythonIncludes: JList[String],
        preservePartitoning: Boolean,
        pythonExec: String,
        pythonVer: String,
        broadcastVars: JList[Broadcast[PythonBroadcast]],
        accumulator: Accumulator[JList[Array[Byte]]])
  extends RDD[MapWithStateRDDRecord[Array[Byte], Array[Byte], Array[Byte]]](prevStateRDD) {

  @volatile private var doFullScan = false

  val bufferSize = conf.getInt("spark.buffer.size", 65536)
  val reuse_worker = conf.getBoolean("spark.python.worker.reuse", true)

  override def checkpoint(): Unit = {
    super.checkpoint()
    doFullScan = true
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prevStateRDD = null
    partitionedDataRDD = null
  }

  override def compute(
      partition: Partition,
      context: TaskContext):
          Iterator[MapWithStateRDDRecord[Array[Byte], Array[Byte], Array[Byte]]] = {

    val stateRDDPartition = partition.asInstanceOf[CSharpMapWithStateRDDPartition]
    val prevStateRDDIterator = prevStateRDD.iterator(
      stateRDDPartition.previousSessionRDDPartition, context)
    val dataIterator = partitionedDataRDD.iterator(
      stateRDDPartition.partitionedDataRDDPartition, context)

    val prevRecord = if (prevStateRDDIterator.hasNext) Some(prevStateRDDIterator.next()) else None

    val newStateMap = prevRecord.map { _.stateMap.copy() }. getOrElse { new EmptyStateMap[Array[Byte], Array[Byte]]() }
    val mappedData = new ArrayBuffer[Array[Byte]]

    val runner = new PythonRunner(
      command, envVars, pythonIncludes, pythonExec, pythonVer, broadcastVars, accumulator,
      bufferSize, reuse_worker)


    runner.compute(new MapWithStateDataIterator(dataIterator, newStateMap), partition.index, context)

    Iterator(MapWithStateRDDRecord(newStateMap, mappedData))
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate(prevStateRDD.partitions.length) { i =>
      new CSharpMapWithStateRDDPartition(i, prevStateRDD, partitionedDataRDD)}
  }
}

private[streaming] class MapWithStateDataIterator(
  dataIterator: Iterator[(Array[Byte], Array[Byte])],
  stateMap: StateMap[Array[Byte], Array[Byte]])

  extends Iterator[(Array[Byte], Array[Byte])] {

  def hasNext = dataIterator.hasNext

  def next = {
    val (key, value) = dataIterator.next()
    stateMap.get(key) match {
      case Some(state) => (key, ByteBuffer.allocate(4).putInt(value.length).array()
        ++ value ++ ByteBuffer.allocate(4).putInt(state.length).array() ++ state)
      case None => (key, ByteBuffer.allocate(4).putInt(value.length).array()
        ++ value ++ ByteBuffer.allocate(4).putInt(0).array())
    }
  }
}

private[streaming] class CSharpMapWithStateRDDPartition(
           idx: Int,
           @transient private var prevStateRDD: RDD[_],
           @transient private var partitionedDataRDD: RDD[_]) extends Partition {

  private[rdd] var previousSessionRDDPartition: Partition = null
  private[rdd] var partitionedDataRDDPartition: Partition = null

  override def index: Int = idx
  override def hashCode(): Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    previousSessionRDDPartition = prevStateRDD.partitions(index)
    partitionedDataRDDPartition = partitionedDataRDD.partitions(index)
    oos.defaultWriteObject()
  }
}
