package tutorial

import geotrellis.proj4.{ConusAlbers, LatLng}
import geotrellis.raster.Tile
import geotrellis.spark.io.file.cog.{FileCOGCollectionLayerReader, FileCOGLayerReader, FileCOGValueReader}
import geotrellis.spark.io.{BoundLayerQuery, CollectionLayerReader, Intersects, SpaceTimeKeyFormat, ValueReader, tileLayerMetadataFormat}
import geotrellis.spark.{LayerId, Metadata, SpaceTimeKey, TileLayerCollection, TileLayerMetadata}
import geotrellis.vector.io.{GeometryFormat, PolygonFormat, json}
import geotrellis.vector.{Geometry, MultiPolygon, Polygon}
import spray.json._
import spray.json.DefaultJsonProtocol._

trait Utils {

  def valReader: FileCOGValueReader
  def layerReader: FileCOGLayerReader
  def collectReader: FileCOGCollectionLayerReader
  def layerName: String
  def calc_zoom: Int

  def fetchByPolygon(shape: MultiPolygon): BoundLayerQuery[SpaceTimeKey, TileLayerMetadata[SpaceTimeKey], Seq[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] =
    collectReader
      .query[SpaceTimeKey, Tile](LayerId(layerName, calc_zoom))
      .where(Intersects(shape))

//  def fetchByTime(begTime: Long, endTime: Long) =


  def createAOIFromInput(polygon: String): MultiPolygon = parseGeometry(polygon)

  def parseGeometry(geoJson: String): MultiPolygon = {
    geoJson.parseJson.convertTo[Geometry] match {
      case p: Polygon => MultiPolygon(p.reproject(LatLng, ConusAlbers))
      case _ => throw new Exception("Invalid shape")
    }
  }
}
