package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    var tangle_point = queryRectangle.split(",")
    var pointxy= pointString.split(",")
    var lowerX = 0.0
    var upperX = 0.0
    var lowerY = 0.0
    var upperY = 0.0

    if (tangle_point(0).toDouble < tangle_point(2).toDouble){
      lowerX = tangle_point(0).toDouble
      upperX = tangle_point(2).toDouble
    } else {
      lowerX = tangle_point(2).toDouble
      upperX = tangle_point(0).toDouble
    }

    if (tangle_point(1).toDouble < tangle_point(3).toDouble){
      lowerY = tangle_point(1).toDouble
      upperY = tangle_point(3).toDouble
    } else {
      lowerY = tangle_point(3).toDouble
      upperY = tangle_point(1).toDouble
    }

    if (lowerX <= pointxy(0).toDouble && upperX >= pointxy(0).toDouble){
      if (lowerY <= pointxy(1).toDouble && upperY >= pointxy(1).toDouble) {
        return true
      }
    }

    return false

  }

}
