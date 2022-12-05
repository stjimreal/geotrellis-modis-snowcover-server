package tutorial

import com.typesafe.config.ConfigFactory
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.{ByteConstantNoDataCellType, Tile}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.singlebandGeoTiffReader
import geotrellis.raster.render.{ColorMap, Png}
import geotrellis.spark.TileLayerMetadata.toLayoutDefinition
import geotrellis.spark.io.LayerHeader.LayeHeaderFormat
import geotrellis.spark.io.cog.{COGLayerMetadata, COGLayerStorageMetadata, ZoomRange}
import geotrellis.spark.io.cog.COGLayerMetadata.cogLayerMetadataFormat
import geotrellis.spark.io.file.cog.{FileCOGCollectionLayerReader, FileCOGLayerReader, FileCOGValueReader}
import geotrellis.spark.io.hadoop.cog.{HadoopCOGCollectionLayerReader, HadoopCOGValueReader}
import geotrellis.spark.io.{At, AttributeStore, Between, COGLayerAttributes, Intersects, SpaceTimeKeyFormat, SpatialKeyFormat, ValueNotFoundError, tileLayerMetadataFormat}
import geotrellis.spark.mapalgebra.local.temporal.TemporalWindowHelper.UnitMonths
import geotrellis.spark.tiling.{LayoutDefinition, LayoutLevel, MapKeyTransform}
import geotrellis.spark.{Boundable, LayerId, SpaceTimeKey, TileLayerMetadata}
import geotrellis.vector.{Extent, Geometry, MultiPolygon, Point, Polygon}
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollectionMap
import org.apache.hadoop.fs.{Path => HadoopReadPath}
import org.apache.spark.{SparkConf, SparkContext}
import akka.http.scaladsl.model.{HttpResponse, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Directives.respondWithHeaders
import spray.json.DefaultJsonProtocol.{StringJsonFormat, mapFormat}
import akka.http.impl.model.parser
import akka.http.scaladsl.model.headers.RawHeader

// import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.io.Source
import spray.json._

import scala.tools.nsc.io.Path



object ReaderSpaceTimeKey {
  val conf: SparkConf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Tiler")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
  implicit val sc: SparkContext = new SparkContext(conf)
  val catalogPath = "/Volumes/jimDisk/Tibet-SCA/catalog"
  val hadoopPath = new HadoopReadPath("file://"+ catalogPath)
  val layerName = "cloud_free_sca"
  val calc_zoom = 5
  val colorMap: ColorMap = ColorMap.fromStringDouble(ConfigFactory.load().getString("tutorial.ndwiColormap")).get
  val collectReader: FileCOGCollectionLayerReader = FileCOGCollectionLayerReader(catalogPath)
  val layerReader: FileCOGLayerReader = FileCOGLayerReader(catalogPath)

//  val corsResponseHeaders = List(
//    `Access-Control-Allow-Origin`.*,
//    `Access-Control-Allow-Credentials`(true),
//    `Access-Control-Allow-Headers`("Authorization",
//      "Content-Type", "X-Requested-With"),
//    `Access-Control-Max-Age`(1.day.toMillis)//Tell browser to cache OPTIONS requests
//  )

  val begTime: Long = new SimpleDateFormat("yyyy-MM-dd").parse("2020-04-01").getTime
  val endTime: Long = new SimpleDateFormat("yyyy-MM-dd").parse("2020-04-03").getTime
  val zonedBeg: ZonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(begTime), ZoneId.of("GMT"))
  val zonedEnd: ZonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(endTime), ZoneId.of("GMT"))

  def test4(shape: MultiPolygon): Unit = {
    val meta = collectReader.attributeStore.readMetadata[COGLayerStorageMetadata[SpaceTimeKey]](LayerId(layerName, 0))
    val layout = LayoutLevel(3, meta.metadata.layoutForZoom(3))
    val crs = meta.metadata.crs
    crs
//    collectReader.q
    val transformer = MapKeyTransform(crs, layout)
//    transformer.keyToExtent()
    val bounds = transformer.apply(shape.envelope)
    print(bounds.colMax, bounds.colMax, bounds.rowMax, bounds.rowMin)
  }

  def test3(shape: MultiPolygon, instant: Long): Unit= {
    val meta = collectReader.attributeStore.readMetadata[COGLayerStorageMetadata[SpaceTimeKey]](LayerId(layerName, 0))
//    val query = collectReader.query[SpaceTimeKey, Tile](LayerId(layerName, 4))
//    shape.reproject(CRS.fromEpsgCode(4326))
    val value = collectReader.query[SpaceTimeKey, Tile](LayerId(layerName, 6))
      .where(Intersects(shape.envelope))
//      .where(At(zonedEnd))
      .result
    val histCalc = value.polygonalHistogram(shape)
    val Some(result) = histCalc.minMaxValues()
    var landRatio = 0.0
    var snowRatio = 0.0
    val histRes = histCalc.binCounts()
    histRes.foreach{
      case(a, b) => if (a == 0) landRatio = b
      else if (a == 1) snowRatio = b
    }
    val bitsSum = histRes.map(_._2).sum
    landRatio = landRatio / bitsSum
    snowRatio = snowRatio / bitsSum
    val landArea:Long = (landRatio * shape.area).toLong
    val snowArea:Long = (snowRatio * shape.area).toLong


    println(landArea, snowArea, snowRatio, bitsSum)
    println(histRes)
    //    val value_sum = value.toSp{case (a, b) => a localAdd b}
    //    val geoRaster =  value_sum.stitch()
    //      geoRaster.data.renderPng().write("sum_this.png")
    //    val see = value.collect()

    val calc_mean = value.temporalMean(1, UnitMonths, zonedBeg, zonedEnd)

    val resLen = value.metadata.bounds.mkString
    val keys = calc_mean.map{case (key, tile) =>
      val extent = value.metadata.mapTransform(key.spatialKey)
      val res = tile.map(a => if (a > 1) 0 else a)
      val out = res.polygonalMean(extent, shape)
      val sum = res.polygonalSum(extent, shape)
      println(out, sum)
      if (out.isNaN ) 0 else out
    }
    print(keys.sum/keys.length)
  }

  def test2(): Unit = {

    val extented = Source.fromFile("src/main/resources/" + "highAsiaTest.json")
    val source = Source.fromFile("src/main/resources/" +
      "CN_city_scala.geojson")
    val strs = source.mkString
    val geoStr = extented.mkString

//    access-control-allow-credentials

//    HttpResponse(StatusCodes.OK, List(SimpleHeaders))
    val crsSt = "{\"crs\": { \"type\": \"name\", \"properties\": { \"name\": \"urn:ogc:def:crs:OGC:1.3:CRS84\" } } }".parseJson
    val jsValue = geoStr.parseJson
    val mapVal = jsValue.convertTo[Map[String, String]]
    println(mapVal.get("type"))
    val jsonCollectionMap = strs.parseGeoJson[JsonFeatureCollectionMap]()
    val map =  jsonCollectionMap.getAllMultiPolygons()
    val Some(s) = map.get("阿图什市")
    val polygon = s.reproject(CRS.fromEpsgCode(4326), WebMercator)

    test3(polygon, endTime)
//    val Some(oriCrs) = crsFormat.read(crsSt).toCRS
//    s.reproject(CRS.fromEpsgCode(4326), LatLng)
//    test3(s, begTime)
//    test3(map.getOrElse("喀什", ), begTime)
//    val timeList = new Vector[Long]()
//    val collectRes = new Map[String, Vector[Double]]

//    val shape = map.convertTo[Polygon] match {
//      case p: Polygon => {
//        MultiPolygon(p.reproject(LatLng, WebMercator))
//      }
//      case _ => throw new Exception("Invalid shape")
//    }
    source.close()

  }

  def test1(): Unit = {
    //    val reader = HadoopCOGLayerReader(hadoopPath)


    val valReader = FileCOGValueReader(catalogPath)
    val rd = valReader.reader[SpaceTimeKey, Tile](LayerId(layerName, 5))
    //    val query = new LayerQuery[SpaceTimeKey, TileLayerMetadata[SpaceTimeKey]].where(Intersects(extent))
    //      .where(Between(new ZonedDateTime()(2015,1,1,0,0,0, DateTimeZone.UTC),new DateTime(2015,12,12,0,0,0, DateTimeZone.UTC)))

    //    reader.read[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId,query)



    val dates = Seq("2020-03-23", "2020-05-23", "2020-07-23", "2021-02-23", "2017-05-23")
    for (dateStr<- dates) {
      val date = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr).getTime
      val datess = 224
      for ( x <- Range(0, 30)) {
        for (y <- Range(0, 30)) {
          try {
            val tile = rd.read(SpaceTimeKey(x, y, date))
//            val raster = Raster(tile, metadata.extent)
//            tile.polygonalSummary(metadata.extent, )

            val png:Png = tile.renderPng(colorMap)
            png.write("/Users/jimlau/%d_%d_%s.png".format(x, y, dateStr))
          } catch {
            case _: ValueNotFoundError => None
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    test2()
  }

}
