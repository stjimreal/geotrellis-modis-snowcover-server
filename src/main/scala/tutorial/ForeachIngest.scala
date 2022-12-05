package tutorial

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import com.typesafe.config.ConfigFactory

import java.io.File


object ForeachIngest {
  val srcPath = "/Volumes/jimDisk/geotrellis-landsat-tutorial/modis/"
  def maskedPath(src:String, file:String) = s"$src/r-g-nir/$file.r-g-nir.tif"
  //constants to differentiate which bands to use
  val R_BAND = 0
  val G_BAND = 1
  val NIR_BAND = 2
  // Path to our landsat band geotiffs.
  def bandPath(src:String, file:String) = s"$src/$file/$file.%s.tif"

  def iter():Unit = {
    val d = new File(srcPath)
    if (d.exists() && d.isDirectory) {
      d.listFiles.filter((a:File)=>a.isDirectory && a.getName.contains("061"))
        .foreach((a:File)=> {
        println(a.getName)
        val list = a.listFiles.filter(
          (a: File) => {
            a.getName.endsWith("tif") &&
              (a.getName.contains("EV_500_RefSB") ||
                a.getName.contains("EV_250_Aggr500_RefSB"))
          }
        ).toList
        write(list)
      })
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
    iter()

  }
}