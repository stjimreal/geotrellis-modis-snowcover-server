package tutorial

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._
import geotrellis.proj4.WebMercator
import geotrellis.raster.Tile
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.resample.{NearestNeighbor, PointResampleMethod}
import geotrellis.spark.SpaceTimeKey.Boundable
import geotrellis.spark.io.SpaceTimeKeyFormat
import geotrellis.spark.io.cog.COGLayer
import geotrellis.spark.io.file.cog.FileCOGLayerWriter
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.io.hadoop.cog.HadoopCOGLayerWriter
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutLevel, Tiler, ZoomedLayoutScheme}
import geotrellis.spark.{Metadata, SpaceTimeKey, TemporalProjectedExtent, TileLayerMetadata, TileLayerRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.hadoop.fs.Path
import tutorial.NewTimeIngest.maxZoom

import java.io.File
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import scala.io.StdIn
import scala.io.StdIn
import java.io.File
import java.util.concurrent.ConcurrentHashMap

object IngestImage {
  //  val inputPath: String = "file://" + new File("/Volumes/jimDisk/geotrellis-landsat-tutorial/modis/r-g-nir/A2021244").getAbsolutePath
  val inputPath: String = "file://" + new File("/Volumes/jimDisk/cog_test/test_ingest/").getAbsolutePath
  //  val outputPath = "/Volumes/jimDisk/geotrellis-landsat-tutorial/modis/re-catalog"
  val outputPath = "/Volumes/jimDisk/Tibet-SCA/catalog_pyramid/"

  def main(args: Array[String]): Unit = {
    // Setup Spark to use Kryo serializer.
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    new ConcurrentHashMap()
    val sc = new SparkContext(conf)
    try {
      val start_time = new Date().getTime
      run(sc)
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

  def fullPath(path: String): String = new java.io.File(path).getAbsolutePath

  def run(implicit sc: SparkContext):Unit = {
    // Read the geotiff in as a single image RDD,
    // using a method implicitly added to SparkContext by
    // an implicit class available via the
    // "import geotrellis.spark.io.hadoop._ " statement.
    val inputRdd: RDD[(TemporalProjectedExtent, Tile)] =
    sc.hadoopTemporalGeoTiffRDD(inputPath)
    //      sc.hadoopMultibandGeoTiffRDD(inputPath)

    // Use the "TileLayerMetadata.fromRdd" call to find the zoom
    // level that the closest match to the resolution of our source image,
    // and derive information such as the full bounding box and data type.
    //    val (_, rasterMetaData) =
    //      TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(256))
    val layoutScheme = FloatingLayoutScheme(512)
    val (_, rasterMetaData: TileLayerMetadata[SpaceTimeKey]) =
      inputRdd.collectMetadata[SpaceTimeKey](layoutScheme)

    val tilerOptions =
      Tiler.Options(
        resampleMethod = NearestNeighbor,
        partitioner = Some(new HashPartitioner(inputRdd.partitions.length))
      )

    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
    // We'll repartition it so that there are more partitions to work with, since spark
    // likes to work with more, smaller partitions (to a point) over few and large partitions.
    //    val tiled: RDD[(SpaceTimeKey, Tile)] =
    //      inputRdd
    //        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
    //        .repartition(100)
    val tiledRdd: RDD[(SpaceTimeKey, Tile)] with
      Metadata[TileLayerMetadata[SpaceTimeKey]] =
    inputRdd.tileToLayout[SpaceTimeKey](rasterMetaData, tilerOptions)

    //切片大小
    val tarlayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    // We'll be tiling the images using a zoomed layout scheme
    // in the web mercator format (which fits the slippy map tile specification).
    // We'll be creating 256 x 256 tiles.
    //    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    val LayoutLevel(_, layoutDefinition) = tarlayoutScheme.levelForZoom(maxZoom)
    // We need to reproject the tiles to WebMercator
    val (zoom, reprojected): (Int, RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]) =
     TileLayerRDD(tiledRdd, rasterMetaData)
        .reproject(WebMercator, tarlayoutScheme, NearestNeighbor)

    // Create the attributes store that will tell us information about our catalog.
        val attributeStore = FileAttributeStore(outputPath)

    // Create the writer that we will use to store the tiles in the local catalog.
        val writer = FileLayerWriter(attributeStore)

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(reprojected, tarlayoutScheme, maxZoom, NearestNeighbor) { (rdd, z) =>
      val layerId = LayerId("modis", z)
      //      // If the layer exists already, delete it out before writing
      if (attributeStore.layerExists(layerId)) {
        new FileLayerManager(attributeStore).delete(layerId)
      }
      writer.write(layerId, rdd, ZCurveKeyIndexMethod.byDay().createIndex(rdd.metadata.bounds.get))
    }
  }
}

