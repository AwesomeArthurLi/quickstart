package local.sleety.flink.quickstart.kafka

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.source.hybrid.HybridSource
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineFormat
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._

import java.io.File
import java.time.Duration

/** 
 *  Hybrid Source
 *  Scala: 2.12.14
 *  Flink: 1.14.4
 *  JDK  : 11.0.12
 */

case class SensorReading(id: String, timestamp: Long, temperature: Double)


object HelloHybrid {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafka =
      KafkaSource.builder[String]()
        // adjust it 
        .setBootstrapServers("localhost:9092")
        // adjust it 
        .setTopics("lab-flink-sensor-iot")
        .setGroupId("sensor-iot-group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build()

    // adjust it 
    val sensorDataFile = "/Users/arthur/Workspace/flink-summer/src/main/resources/sensor.csv"
    val fileData = FileSource.forRecordStreamFormat(
      new TextLineFormat(),
      Path.fromLocalFile(new File(sensorDataFile)))
      .build()

    val hybridSrc = HybridSource.builder(fileData).addSource(kafka).build()

    env.fromSource(hybridSrc,
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)),
      "kafka & file hybrid source demo")
      .map(data => {
        val arr = data.split(",").map(_.trim)
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .print("hybrid ")

    env.execute("Hello kafka & file hybrid source")
  }
}


