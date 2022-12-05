package tutorial

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, Materializer}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.singlebandGeoTiffReader
import geotrellis.raster.{ByteConstantNoDataCellType, Tile}
import geotrellis.spark.io.file.cog.{FileCOGCollectionLayerReader, FileCOGLayerReader, FileCOGValueReader}
import geotrellis.spark.io.hadoop.cog.{HadoopCOGCollectionLayerReader, HadoopCOGLayerReader, HadoopCOGValueReader}
import geotrellis.spark.io.{AttributeStore, SpaceTimeKeyFormat, SpatialKeyFormat}
import geotrellis.spark.{LayerId, SpaceTimeKey}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.ExecutionContextExecutor

object NewServe extends App with TheService{
  val conf: SparkConf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Tiler")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
  implicit val sc: SparkContext = new SparkContext(conf)
  val layerName = "cloud_free_sca"
  val catalogPath = "/Volumes/jimDisk/Tibet-SCA/catalog"
  val hadoopPath = new Path("file://"+ catalogPath)
  val hadoopColReader = HadoopCOGCollectionLayerReader(hadoopPath)
  val hadoopValReader = HadoopCOGValueReader(hadoopPath)
  val collectionReader = FileCOGCollectionLayerReader(catalogPath)
  val valueReader = FileCOGValueReader(catalogPath)


  implicit val system: ActorSystem = ActorSystem("cloud-free-snow-watch-system")
  implicit val executor: ExecutionContextExecutor = system.dispatcher
  implicit val materializer: Materializer = ActorMaterializer()
  val logger = Logging(system, getClass)

  Http().bindAndHandle(root, "0.0.0.0", 8889)

//  override def main(args: Array[String]): Unit = {
//    val valRd = valReader.reader[SpaceTimeKey, Tile](LayerId(layerName, 5))
////    val value = layerReader.query(LayerId(layerName, 3))
//
//  }

}
