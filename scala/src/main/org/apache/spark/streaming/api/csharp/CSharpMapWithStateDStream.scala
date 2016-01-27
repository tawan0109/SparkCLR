package org.apache.spark.streaming.api.csharp

import java.nio.ByteBuffer

import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang.StringUtils
import org.apache.spark.api.python.{PythonBroadcast, PythonRunner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.api.java.JavaDStream
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


private[streaming] class CSharpMapWithStateDStream(
                            parent: DStream[Array[Byte]],
                            func: Array[Byte],
                            timeoutIntervalInMillis: Long,
                            csharpWorkerExec: String)
  extends DStream[Array[Byte]](parent.context) {

  private val internalStream =
    new InternalCSharpMapWithStateDStream(parent, func, timeoutIntervalInMillis, csharpWorkerExec)

  def slideDuration: Duration = internalStream.slideDuration

  def dependencies: List[DStream[_]] = List(internalStream)

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    internalStream.getOrCompute(validTime).map {
      _.flatMap[Array[Byte]] {
        _.mappedData
      }
    }
  }
}

private[streaming] class InternalCSharpMapWithStateDStream(
                                       parent: DStream[Array[Byte]],
                                       func: Array[Byte],
                                       timeoutIntervalInMillis: Long,
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

  override def compute(validTime: Time): Option[RDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]]] = {
    // Get the previous state or create a new empty state RDD
    val prevStateRDD = getOrCompute(validTime - slideDuration) match {
      case Some(rdd) => if (rdd.partitioner != Some(partitioner)) {
        CSharpMapWithStateRDD.createFromRDD(
          rdd.flatMap { _.stateMap.getAll() }, partitioner, validTime, csharpWorkerExec)
        } else {
          rdd
        }
      case None =>
        new EmptyRDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](ssc.sparkContext)
    }

    val dataRDD = parent.getOrCompute(validTime).getOrElse {
      context.sparkContext.emptyRDD[Array[Byte]]
    }.map(e => {
      if(e == null){
        println("bytes is null.")
        ("", e)
      }else {
        /*
        println("bytes length in scala:" + e.length)
        println("bytes in scala:")
        println(new String(Hex.encodeHex(e)))
        (Base64.getEncoder.encodeToString(ByteBuffer.wrap(e, 0, 4).array()), e)
        */
        val len = ByteBuffer.wrap(e, 0, 4).getInt
        println("key len in scala:" + len)
        (Base64.getEncoder.encodeToString(ByteBuffer.wrap(e, 4, len).array()), e)
      }
    })

    // dataRDD.take(10)

    // Some(new EmptyRDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](ssc.sparkContext))
    // Some(dataRDD.sparkContext.emptyRDD)

    val timeoutThresholdTime = Some(validTime.milliseconds - timeoutIntervalInMillis)

    val mapWithStateRDD = new CSharpMapWithStateRDD(
      prevStateRDD,
      dataRDD,
      func,
      validTime,
      timeoutThresholdTime,
      csharpWorkerExec)

    mapWithStateRDD.take(10)

    Some(mapWithStateRDD)
  }
}

private[spark] class CSharpMapWithStateRDD (
        var prevStateRDD: RDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]],
        var partitionedDataRDD: RDD[(String, Array[Byte])],
        command: Array[Byte],
        batchTime: Time,
        timeoutThresholdTime: Option[Long],
        csharpWorkerExec: String)
  extends RDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](
    partitionedDataRDD.sparkContext,
    List(
      new OneToOneDependency[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](prevStateRDD),
      new OneToOneDependency(partitionedDataRDD))) {

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

    println("Start to compute for CSharpMapWithStateRDD.")
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
      println("Start to process result.")
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
            val retBytes = Some(ByteBuffer.wrap(bytes, offset, buffer.getInt).array())
            buffer.position(buffer.position() + valueLen)
            retBytes
          }
        }
      }
    }

    println("Start python runner.")
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
               csharpWorkerExec: String): CSharpMapWithStateRDD = {

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
    new CSharpMapWithStateRDD(stateRDD, emptyDataRDD, new Array[Byte](0), updateTime, None, csharpWorkerExec)
  }
}

private[streaming] class MapWithStateDataIterator(
  dataIterator: Iterator[(String, Array[Byte])],
  stateMap: StateMap[String, Array[Byte]],
  batchTime: Time)
  extends Iterator[(Array[Byte])] {

  def hasNext = dataIterator.hasNext

  def next = {
    println("Start to read next element in dataIterator.")
    val (key, pairBytes) = dataIterator.next()
    println("Finish to read next element in dataIterator.")
    stateMap.get(key) match {
      case Some(stateBytes) =>
        pairBytes ++ ByteBuffer.allocate(4).putInt(stateBytes.length).array() ++ stateBytes ++
          ByteBuffer.allocate(8).putLong(batchTime.milliseconds).array()

      case None =>
        pairBytes ++ ByteBuffer.allocate(4).putInt(0).array() ++
          ByteBuffer.allocate(8).putLong(batchTime.milliseconds).array()
    }
  }
}

private[streaming] class CSharpMapWithStateRDDPartition(
           idx: Int,
           @transient var prevStateRDD: RDD[_],
           @transient var partitionedDataRDD: RDD[_]) extends Partition {

  private[streaming] var previousSessionRDDPartition: Partition = null
  private[streaming] var partitionedDataRDDPartition: Partition = null

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
