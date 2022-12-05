package tutorial


import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.SpatialKeyFormat
import geotrellis.spark.io.cog.COGLayer
import geotrellis.spark.io.file.cog.{FileCOGCollectionLayerReader, FileCOGLayerWriter}
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerManager, FileLayerWriter}
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, Metadata, MultibandTileLayerRDD, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata, withTilerMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap
import scala.io.StdIn

object COGIngest {
  val srcPath = "/Volumes/jimDisk/cog_test/cog_tiff/"
  val dstPath = "/Volumes/jimDisk/Tibet-SCA/catalog/"
  def maskedPath(src:String, file:String) = s"$src/r-g-nir/$file.r-g-nir.tif"
  //constants to differentiate which bands to use
  val R_BAND = 0
  val G_BAND = 1
  val NIR_BAND = 2
  // Path to our landsat band geotiffs.
  def bandPath(src:String, file:String) = s"$src/$file/$file.%s.tif"

  def run(implicit sc:SparkContext, srcPath: String): Unit = {

    // Read the geotiff in as a single image RDD,
    // using a method implicitly added to SparkContext by
    // an implicit class available via the
    // "import geotrellis.spark.io.hadoop._ " statement.
    val inputRdd: RDD[(ProjectedExtent, Tile)] = {
    sc.hadoopGeoTiffRDD(srcPath)
    }
//    val reader = FileCOGCollectionLayerReader(srcPath)

    //Example:
    //import org.joda.time.DateTime
    //
    //val time1: DateTime = ???
    //val time2: DateTime = ???
    //
    //val rdd: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] =
    //  reader
    //    .query[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](LayerId("Climate_CCSM4-RCP45-Temperature-Max", 8))
    //    .where(Intersects(Extent(-85.32,41.27,-80.79,43.42)))
    //    .where(Between(time1, time2))
    //    .result

    // Use the "TileLayerMetadata.fromRdd" call to find the zoom
    // level that the closest match to the resolution of our source image,
    // and derive information such as the full bounding box and data type.
    val (_, rasterMetaData) = {
    TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(256))
    }

//    val temporalKey = TemporalKey()
    val timeFormat = new SimpleDateFormat("yyyyDDD")
    val bcTimeFormat = sc.broadcast(timeFormat)

    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
    // We'll repartition it so that there are more partitions to work with, since spark
    // likes to work with more, smaller partitions (to a point) over few and large partitions.
    val tiled: RDD[(SpatialKey, Tile)] = {
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)
      //        .collectMetadata() .map((k:SpatialKey, tile:Tile) => {
      //        val timeKey = TemporalKey()
      //      }
      //      SpaceTimeKey()
    }

    // We'll be tiling the images using a zoomed layout scheme
    // in the web mercator format (which fits the slippy map tile specification).
    // We'll be creating 256 x 256 tiles.
    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    // We need to reproject the tiles to WebMercator 3857 WebMercator
//    val (zoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
//      MultibandTileLayerRDD(tiled, rasterMetaData)
//        .reproject(WebMercator, layoutScheme, Bilinear)
//
//    println("zoom: ", zoom)

    // Create the attributes store that will tell us information about our catalog.
//    val attributeStore = FileAttributeStore(dstPath)

    // Create the writer that we will use to store the tiles in the local catalog.
//    val writer = FileLayerWriter(attributeStore)
//    val cogWriter = FileCOGLayerWriter(dstPath)
//    val layerName = "cloud_free_sca"
//
//    val cogLayer = COGLayer.fromLayerRDD(reprojected,
//      zoom,
//    )
//
//    val keyIndex = cogLayer.metadata.zoomRangeInfos.map {
//      case (zr, bounds) => zr -> ZCurveKeyIndexMethod.byDay().createIndex(bounds)
//    }.toMap
//
//    cogWriter.writeCOGLayer(layerName, cogLayer, keyIndex)

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
//    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
//      val layerId = LayerId("modis", z)
//      // If the layer exists already, delete it out before writing
//      if(attributeStore.layerExists(layerId)) {
//        new FileLayerManager(attributeStore).delete(layerId)
//      }
//      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
//    }
  }

  def iter():Option[List[File]] = {
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


  def write(list: List[File]):Unit = {
    if (new File(maskedPath(srcPath, list.head.getParentFile.getName)).exists()) {
      return
    }
    list.foreach(println)
    val multiband12 = MultibandGeoTiff(list.filter(_.getName.contains("EV_250_Aggr500_RefSB")).head.getAbsolutePath)
    val multiband34567= MultibandGeoTiff(list.filter(_.getName.contains("EV_500_RefSB")).head.getAbsolutePath)

    // Read in the red band
    println("Reading in the red band...")
    val rTile = multiband12.tile.band(0)

    // Read in the green band
    println("Reading in green band 4...")
    val gTile = multiband34567.tile.band(1)

    // Read in the near infrared band
    println("Reading in the NIR band...")
    val nirTile = multiband12.tile.band(1)

    // Read in the QA band
    //    println("Reading in the QA band...")
    //    val qaGeoTiff = SinglebandGeoTiff(bandPath("BQA"))

    // GeoTiffs have more information we need; just grab the Tile out of them.
    //    val (rTile, gTile, nirTile, qaTile) = (rGeoTiff.tile.band(1), gGeoTiff.tile, nirGeoTiff.tile, qaGeoTiff.tile)

    // This function will set anything that is potentially a cloud to NODATA
    //    def maskClouds(tile: Tile): Tile =
    //      tile.combine(qaTile) { (v: Int, qa: Int) =>
    //        val isCloud = qa & 0x8000
    //        val isCirrus = qa & 0x2000
    //        if(isCloud > 0 || isCirrus > 0) { NODATA }
    //        else { v }
    //      }

    // Mask our red, green and near infrared bands using the qa band
    println("Masking clouds in the red band...")
    //    val rMasked = maskClouds(rTile)
    println("Masking clouds in the green band...")
    //    val gMasked = maskClouds(gTile)
    println("Masking clouds in the NIR band...")
    //    val nirMasked = maskClouds(nirTile)

    // Create a multiband tile with our two masked red and infrared bands.
    val mb = ArrayMultibandTile(rTile, gTile, nirTile).convert(IntConstantNoDataCellType)

    // Create a multiband geotiff from our tile, using the same extent and CRS as the original geotiffs.
    println("Writing out the multiband R + G + NIR tile...")

    MultibandGeoTiff(mb, multiband12.extent, multiband12.crs).write(
      maskedPath(srcPath, list.head.getParentFile.getName))
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
      run(sc, srcPath)

      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }

  }
}
