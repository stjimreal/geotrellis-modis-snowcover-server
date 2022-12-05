package tutorial

import geotrellis.proj4.WebMercator
import geotrellis.raster.Tile
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.resample.{NearestNeighbor, PointResampleMethod}
import geotrellis.spark.io.SpaceTimeKeyFormat
import geotrellis.spark.io.cog.COGLayer
import geotrellis.spark.io.file.cog.FileCOGLayerWriter
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.io.index.hilbert.HilbertSpaceTimeKeyIndex
import geotrellis.spark.io.index.{HilbertKeyIndexMethod, ZCurveKeyIndexMethod}
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutLevel, Tiler, ZoomedLayoutScheme}
import geotrellis.spark.{Metadata, SpaceTimeKey, TemporalProjectedExtent, TileLayerMetadata, TileLayerRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import java.io.File
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import scala.io.StdIn


/*
 *    from HadoopGeoTiffRDD.scala: 45 line:
 *        final val GEOTIFF_TIME_TAG_DEFAULT = "TIFFTAG_DATETIME"
 *        final val GEOTIFF_TIME_FORMAT_DEFAULT = "yyyy:MM:dd HH:mm:ss"
 *
 *
 */
object HilbertIngest {

  val srcPath = "/Volumes/jimDisk/cog_test/cvt_01/"
  //  val dstPath = "/Volumes/jimDisk/Tibet-SCA/catalog_3_5/"
  val dstPath = "/Volumes/jimDisk/Tibet-SCA/catalog_test_hilbert/"
  val taskFileList = "/Users/jimlau/Documents/project/geotrellis-landsat-tutorial/test_ingest_list.txt"
  val maxZoom = 7
  val resampleMethod: PointResampleMethod = NearestNeighbor


  def iter(srcPath: String):Option[List[File]] = {
    val d = new File(srcPath)
    if (d.exists() && d.isDirectory) {
      val l = d.listFiles.filter((a: File) => {
        a.getName.endsWith("tif")
      }
      ).toList
      Some(l)
    } else {
      None
    }
  }

  def run(implicit sc: SparkContext, srcPath: String): Unit = {
    val inputRdd: RDD[(TemporalProjectedExtent, Tile)] =
      sc.hadoopTemporalGeoTiffRDD(srcPath)
    val layoutScheme = FloatingLayoutScheme(512)
    val (_, rasterMetaData: TileLayerMetadata[SpaceTimeKey]) =
      inputRdd.collectMetadata[SpaceTimeKey](layoutScheme)
    val tilerOptions =
      Tiler.Options(
        resampleMethod = resampleMethod,
        partitioner = Some(new HashPartitioner(inputRdd.partitions.length))
      )

    //生成SpaceTimeKey RDD
    val tiledRdd: RDD[(SpaceTimeKey, Tile)] with
      Metadata[TileLayerMetadata[SpaceTimeKey]] =
      inputRdd.tileToLayout[SpaceTimeKey](rasterMetaData, tilerOptions)


    //切片大小
    val tarlayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    val LayoutLevel(_, layoutDefinition) = tarlayoutScheme.levelForZoom(maxZoom)
    val (_, reprojected): (Int, RDD[(SpaceTimeKey, Tile)] with
      Metadata[TileLayerMetadata[SpaceTimeKey]]) =
      TileLayerRDD(tiledRdd, rasterMetaData)
        .reproject(WebMercator, layoutDefinition,
          options = Options(
            method = resampleMethod,
            targetCellSize = Some(layoutDefinition.cellSize)))

    val writer = FileCOGLayerWriter(dstPath)

    val layerName = "cloud_free_sca"

    val cogLayer =
      COGLayer.fromLayerRDD(
        reprojected,
        maxZoom)
    val keyIndexes =
      cogLayer.metadata.zoomRangeInfos.
//        map { case (zr, bounds) => zr -> HilbertSpaceTimeKeyIndex(bounds, ) }.
        map {case (zr, bounds) => zr -> ZCurveKeyIndexMethod.byDay().createIndex(bounds)}.
        toMap
    writer.writeCOGLayer(layerName, cogLayer, keyIndexes)
  }

  def ingestOnce(implicit sc: SparkContext, srcPath: String): Unit = {
    val inputRdd: RDD[(TemporalProjectedExtent, Tile)] =
      sc.hadoopTemporalGeoTiffRDD(srcPath)
    val layoutScheme = FloatingLayoutScheme(512)
    val (_, rasterMetaData: TileLayerMetadata[SpaceTimeKey]) =
      inputRdd.collectMetadata[SpaceTimeKey](layoutScheme)
    val tilerOptions =
      Tiler.Options(
        resampleMethod = resampleMethod,
        partitioner = Some(new HashPartitioner(inputRdd.partitions.length))
      )

    //生成SpaceTimeKey RDD
    val tiledRdd: RDD[(SpaceTimeKey, Tile)] with
      Metadata[TileLayerMetadata[SpaceTimeKey]] =
      inputRdd.tileToLayout[SpaceTimeKey](rasterMetaData, tilerOptions)


    //切片大小
    val tarlayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    val LayoutLevel(_, layoutDefinition) = tarlayoutScheme.levelForZoom(maxZoom)
    val (_, reprojected): (Int, RDD[(SpaceTimeKey, Tile)] with
      Metadata[TileLayerMetadata[SpaceTimeKey]]) =
      TileLayerRDD(tiledRdd, rasterMetaData)
        .reproject(WebMercator, layoutDefinition,
          options = Options(
            method = resampleMethod,
            targetCellSize = Some(layoutDefinition.cellSize)))

    val writer = FileCOGLayerWriter(dstPath)

    val layerName = "cloud_free_sca"

    val cogLayer =
      COGLayer.fromLayerRDD(
        reprojected,
        maxZoom)
    val keyIndexes =
      cogLayer.metadata.zoomRangeInfos.
        map { case (zr, bounds) => zr -> ZCurveKeyIndexMethod.byDay().createIndex(bounds) }.
        toMap
    writer.writeCOGLayer(layerName, cogLayer, keyIndexes)
  }


  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    new ConcurrentHashMap()
    val sc = new SparkContext(conf)
    try {
      //      val Some(ls) =  iter()
      //      ls.foreach(f => run(sc, f.getAbsolutePath))
      val start_time = new Date().getTime

      //      val Some(ls) = iter(srcPath)
      //      ls.foreach(srcPath => run(sc, srcPath.getAbsolutePath))
//      val ls = sc.textFile(taskFileList).collect
//      ls.foreach(srcPath => run(sc, srcPath))
      ingestOnce(sc, srcPath)

      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
      val end_time = new Date().getTime
      println("Cost Time: %d ms".format(end_time-start_time))
      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }

  }
}
