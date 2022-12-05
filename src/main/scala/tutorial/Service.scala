package tutorial

import akka.Done
import akka.actor._
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import akka.stream.{ActorMaterializer, Materializer}

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.{ByteConstantNoDataCellType, Tile}
import geotrellis.raster.render.{ColorMap, Png}
import geotrellis.spark.{LayerId, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.file.cog.{FileCOGCollectionLayerReader, FileCOGLayerReader, FileCOGValueReader}
import geotrellis.spark.io.cog.COGLayerMetadata
import geotrellis.spark.io.{At, Intersects, SpaceTimeKeyFormat, tileLayerMetadataFormat, CollectionLayerReader, ValueReader, ValueNotFoundError}
import geotrellis.vector.io.{GeometryFormat, PolygonFormat, json}
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import geotrellis.vector.{Geometry, MultiPolygon, Polygon}
import spray.json._

import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId, ZonedDateTime}

trait TheService {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContextExecutor
  implicit val materializer: Materializer

  def valueReader: FileCOGValueReader
  def collectionReader: FileCOGCollectionLayerReader

  val logger: LoggingAdapter
  val colorMap: ColorMap = ColorMap.fromStringDouble(ConfigFactory.load().getString("tutorial.ndwiColormap")).get
  val layerName: String
  val catalogPath: String
  val layerId = LayerId(layerName, 5)

  /** raster transformation to perform at request time */
  def rasterFunction(): Tile => Tile = {
    tile: Tile => tile.convert(ByteConstantNoDataCellType)
  }

  def filterOutNoValue(): Tile => Tile = {
    tile: Tile => tile.map(a => if (a > 1) 0 else a)
  }

  def pngAsHttpResponse(png: Png): HttpResponse =
    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))

  val zxyRoute:Route = pathPrefix("tiles") {
    pathPrefix(Segment / IntNumber) { (dateStr, zoom) =>
      // val fn: MultibandTile => Tile = NewServe.rasterFunction(render)
      val instant = new SimpleDateFormat("yyyyMMdd").parse(dateStr).getTime
      // val fn: Tile => Tile = rasterFunction()
      // ZXY route:
      pathPrefix(IntNumber / IntNumber) { (x, y) =>
        complete {
          Future {
            // Read in the tile at the given z/x/y coordinates.
            val tileOpt: Option[Tile] =
              try {
                val reader = valueReader.reader[SpaceTimeKey, Tile](LayerId(layerName, zoom))
                Some(reader.read(SpaceTimeKey(x, y, instant)))
              } catch {
                case _: ValueNotFoundError =>
                  None
              }

            for (tile <- tileOpt) yield {
              val product: Tile = tile
              // fn(tile)
              val png: Png = product.renderPng(colorMap)
              pngAsHttpResponse(png)
            }
          }
        }
      } ~
        // Static content routes:
        pathEndOrSingleSlash {
          getFromFile("static/index.html")
        } ~
        pathPrefix("") {
          getFromDirectory("static")
        }
    }
  }

  val apiRoute: Route = pathPrefix("api") {
    pathPrefix(Segment) {(dateStr) =>
        val instant = new SimpleDateFormat("yyyyMMdd").parse(dateStr).getTime
        pathEndOrSingleSlash {
          post {
            entity(as[String]) { geoJson => {
              // val poly = geoJson.parseGeoJson[Polygon]()
              val poly = geoJson.parseGeoJson[MultiPolygon]()
              val id: LayerId = LayerId(layerName, 6)

              // Leaflet produces polygon in LatLng, we need to reproject it to layer CRS
              // val layerMetadata = collectReader.attributeStore.readMetadata[COGLayerStorageMetadata[SpaceTimeKey]](LayerId(layerName, 0))
              val queryPoly = poly.reproject(LatLng, WebMercator)

              val queryTime: ZonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(instant), ZoneId.of("GMT"))
              val fn: Tile => Tile = filterOutNoValue()
              // Query all tiles that intersect the polygon and build histogram
              val queryHist = collectionReader
                .query[SpaceTimeKey, Tile](id)
                .where(Intersects(queryPoly))
                .where(At(queryTime))
                .result // all intersecting tiles have been fetched at this point
                .withContext(_.mapValues(fn))
                .polygonalHistogramDouble(queryPoly)

              val histRes = queryHist.binCounts()
              var snowRatio = 0.0
              histRes.foreach{
                case(a, b) =>
                  if (a == 1) snowRatio = b
              }
              val bitsSum = histRes.map(_._2).sum
              val snowRatioRes = snowRatio / bitsSum

              import spray.json.DefaultJsonProtocol._
              respondWithHeaders(RawHeader("access-control-allow-origin", "*"),
              RawHeader("access-control-allow-methods", "GET, POST"),
              RawHeader("access-control-allow-headers", "Origin, Content-Type, X-Auth-Token")
              ){
                complete(StatusCodes.OK -> snowRatioRes.toString)
                }
              }
            }
          }
        }
    }
  }

  def root = {
    concat(
      zxyRoute,
      apiRoute,
      path("ping") {
        entity(as[String]) { _ =>
          complete {
            Future {
              "pong"
            }
          }
        }
      },
      // Static content routes:
      pathEndOrSingleSlash {
        getFromFile("static/index.html")
      },
      pathPrefix("") {
        getFromDirectory("static")
      }
    )
  }
}
