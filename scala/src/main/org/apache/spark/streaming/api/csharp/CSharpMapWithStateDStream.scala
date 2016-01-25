package org.apache.spark.streaming.api.csharp

import java.nio.ByteBuffer

import org.apache.spark.api.python.{PythonBroadcast, PythonRunner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.util.{StateMap, EmptyStateMap}
import org.apache.spark.util.Utils
import org.apache.spark._

import java.io.{ObjectOutputStream, IOException, DataInputStream, DataOutputStream}
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap, Base64, Collections}
import org.apache.spark.streaming.rdd.{MapWithStateRDDPartition, MapWithStateRDD, MapWithStateRDDRecord}

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

import scala.language.existentials


class CSharpMapWithStateDStream(
         parent: DStream[Array[Byte]],
         func: Array[Byte],
         spec: StateSpecImpl[Array[Byte], Array[Byte], Array[Byte], Array[Byte]],
         envVars: JMap[String, String],
         pythonIncludes: JList[String],
         csharpWorkerExec: String)
  extends DStream[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](parent.context) {

  persist(StorageLevel.MEMORY_ONLY)

  private val partitioner = new HashPartitioner(ssc.sc.defaultParallelism)

  override def dependencies: List[DStream[_]] = List(parent)

  override def initialize(time: Time): Unit = {
    if (checkpointDuration == null) {
      checkpointDuration = slideDuration * 10
    }
    super.initialize(time)
  }

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[CSharpMapWithStateRDD] = {
    // Get the previous state or create a new empty state RDD
    val prevStateRDD = getOrCompute(validTime - slideDuration) match {
      case Some(rdd) => if (rdd.partitioner != Some(partitioner)) {
        CSharpMapWithStateRDD.createFromRDD(
          rdd.flatMap { _.stateMap.getAll() }, partitioner, validTime, csharpWorkerExec, envVars)
        } else {
          rdd
        }
      case None =>
        new EmptyRDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](ssc.sparkContext)
    }

    def base64Key(bytes: Array[Byte]): String = {
      Base64.getEncoder.encodeToString(ByteBuffer.wrap(bytes, 4, ByteBuffer.wrap(bytes, 0, 4).getInt).array())
    }

    val dataRDD = parent.getOrCompute(validTime).getOrElse {
      context.sparkContext.emptyRDD[Array[Byte]]
    }.map(e => (base64Key(e), e))

    val timeoutThresholdTime = spec.getTimeoutInterval().map { interval =>
      (validTime - interval).milliseconds
    }

    Some(new CSharpMapWithStateRDD(
      prevStateRDD,
      dataRDD,
      func,
      validTime,
      timeoutThresholdTime,
      csharpWorkerExec,
      envVars))
  }

  /** Return a pair DStream where each RDD is the snapshot of the state of all the keys. */
  def stateSnapshots(): DStream[(String, Array[Byte])] = {
    flatMap { _.stateMap.getAll().map { case (k, s, _) => (k, s) }.toTraversable }
  }
}

private[spark] class CSharpMapWithStateRDD (
        var prevStateRDD: RDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]],
        var partitionedDataRDD: RDD[(String, Array[Byte])],
        command: Array[Byte],
        batchTime: Time,
        timeoutThresholdTime: Option[Long],
        csharpWorkerExec: String,
        envVars: JMap[String, String])
  extends RDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](prevStateRDD) {

  val bufferSize = conf.getInt("spark.buffer.size", 65536)
  val reuse_worker = conf.getBoolean("spark.python.worker.reuse", true)
  @volatile private var doFullScan = false

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
          Iterator[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]] = {

    val stateRDDPartition = partition.asInstanceOf[CSharpMapWithStateRDDPartition]
    val prevStateRDDIterator = prevStateRDD.iterator(
      stateRDDPartition.previousSessionRDDPartition, context)
    val dataIterator = partitionedDataRDD.iterator(
      stateRDDPartition.partitionedDataRDDPartition, context)

    val prevRecord = if (prevStateRDDIterator.hasNext) Some(prevStateRDDIterator.next()) else None

    val newStateMap = prevRecord.map { _.stateMap.copy() }. getOrElse { new EmptyStateMap[String, Array[Byte]]() }
    val mappedData = new ArrayBuffer[Array[Byte]]

    val runner = new PythonRunner(
      command,
      new java.util.HashMap[String, String](),
      new java.util.ArrayList[String](),
      csharpWorkerExec,
      "",
      new JArrayList[Broadcast[PythonBroadcast]](),
      null,
      bufferSize,
      reuse_worker)

    def processResult(bytes: Array[Byte]): Unit = {
      val buffer = ByteBuffer.wrap(bytes)
      // read key
      val keyLen = buffer.getInt
      val key = Base64.getEncoder().encodeToString(ByteBuffer.wrap(bytes, 4, keyLen).array())
      buffer.position(buffer.position() + keyLen)

      // read value
      mappedData ++= readBytes(buffer)

      // read state status, 1 - updated, 2 - defined, 3 - removed
      buffer.getInt match {
        case 3 => newStateMap.remove(key)
        case _ => {
          val state = readBytes(buffer).get
          newStateMap.put(key, state, batchTime.milliseconds)
        }
      }

      def readBytes(buffer: ByteBuffer): Option[Array[Byte]] = {
        val offset = buffer.position()
        val valueLen = buffer.getInt

        valueLen match {
          case 0 => None
          case _ => {
            val bytes = Some(ByteBuffer.wrap(bytes, offset, buffer.getInt).array())
            buffer.position(buffer.position() + valueLen)
            bytes
          }
        }
      }
    }

    runner.compute(new MapWithStateDataIterator(dataIterator, newStateMap, batchTime), partition.index, context).foreach(processResult(_))
    if (timeoutThresholdTime.isDefined) {
      newStateMap.getByTime(timeoutThresholdTime.get).foreach { case (key, state, _) =>
        // TODO apply command to these timeout keys
        newStateMap.remove(key)
      }
    }

    Iterator(MapWithStateRDDRecord(newStateMap, mappedData))
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate(prevStateRDD.partitions.length) { i =>
      new CSharpMapWithStateRDDPartition(i, prevStateRDD, partitionedDataRDD)}
  }
}

private [streaming] object CSharpMapWithStateRDD {

  def createFromRDD(
               rdd: RDD[(String, Array[Byte], Long)],
               partitioner: Partitioner,
               updateTime: Time,
               csharpWorkerExec: String,
               envVars: JMap[String, String]): CSharpMapWithStateRDD = {

    val pairRDD = rdd.map { x => (x._1, (x._2, x._3)) }
    val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions({ iterator =>
      val stateMap = StateMap.create[String, Array[Byte]](SparkEnv.get.conf)
      iterator.foreach { case (key, (state, updateTime)) =>
        stateMap.put(key, state, updateTime)
      }
      Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[Array[Byte]]))
    }, preservesPartitioning = true)

    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(String, Array[Byte])].partitionBy(partitioner)

    // FIXME: command should not be a zero-length array
    new CSharpMapWithStateRDD(stateRDD, emptyDataRDD, new Array[Byte](0), updateTime, None, csharpWorkerExec, envVars)
  }
}

private[streaming] class MapWithStateDataIterator(
  dataIterator: Iterator[(String, Array[Byte])],
  stateMap: StateMap[String, Array[Byte]],
  batchTime: Time)
  extends Iterator[(Array[Byte])] {

  def hasNext = dataIterator.hasNext

  def next = {
    val (key, pairBytes) = dataIterator.next()

    stateMap.get(key) match {
      case Some(stateBytes) =>
        pairBytes ++ ByteBuffer.allocate(4).putInt(stateBytes.length).array() ++ stateBytes ++
          ByteBuffer.allocate(4).putLong(batchTime.milliseconds * 1000).array()

      case None =>
        pairBytes ++ ByteBuffer.allocate(4).putInt(0).array() ++
          ByteBuffer.allocate(8).putLong(batchTime.milliseconds * 1000).array()
    }
  }
}

private[streaming] class CSharpMapWithStateRDDPartition(
           idx: Int,
           @transient var prevStateRDD: RDD[_],
           @transient var partitionedDataRDD: RDD[_]) extends Partition {

  private[rdd] var previousSessionRDDPartition: Partition = null
  private[rdd] var partitionedDataRDDPartition: Partition = null

  override def hashCode(): Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    previousSessionRDDPartition = prevStateRDD.partitions(index)
    partitionedDataRDDPartition = partitionedDataRDD.partitions(index)
    oos.defaultWriteObject()
  }

  override def index: Int = idx
}
