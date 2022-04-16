import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


def compute(trips : Dataset[Row], dist : Double) : Dataset[Row] = {
    import spark.implicits._

    val intervalSeconds = 28800 // number of seconds in 8 h.
    val earthRadius = 6371.0 // in Km
    val degOfDist = dist/111111.0; // Convert the distance from meters to degrees
    
    // Calculate the distance between 2 points in a map given their coordinates
    // Assume that the earth is a sphere with radius 6371km.
    val makeDistExpr = (lat1 : Double, lon1 : Double, lat2 : Double, lon2 : Double) => {
        import scala.math._
        val dLat = toRadians(abs(lat2 - lat1))
        val dLon = toRadians(abs(lon2 - lon1))
        val hav = pow(sin(dLat*0.5),2) + pow(sin(dLon*0.5),2) * cos(toRadians(lat1)) * cos(toRadians(lat2))
        abs(earthRadius * 1000 * 2 * asin(sqrt(hav)))
    }

    val round = udf(math.floor _)
    val distance = udf(makeDistExpr(_:Double,_:Double,_:Double,_:Double))
    

    // Transforming time into seconds
    val tripsTransformed = trips.withColumn("pickup_time", unix_timestamp($"tpep_pickup_datetime"))
        .withColumn("dropoff_time", unix_timestamp($"tpep_dropoff_datetime"))

    // Select necessary columns only
    val tripsNec = tripsTransformed.select('pickup_time, 'dropoff_time, 'pickup_longitude, 'pickup_latitude,
                                'dropoff_longitude, 'dropoff_latitude)

    // Define buckets
    val tripsBuck = tripsNec.withColumn("pt_bucket", round($"pickup_time"/intervalSeconds))
                            .withColumn("dt_bucket", round($"dropoff_time"/intervalSeconds))
                            .withColumn("pLat_bucket", round($"pickup_latitude"/degOfDist))
                            .withColumn("dLat_bucket", round($"dropoff_latitude"/degOfDist)).cache()

    // Neighbors buckects
    val tripsBuckNeighbors = tripsBuck.withColumn("pt_bucket", explode(array($"pt_bucket" - 1, $"pt_bucket")))
                                        .withColumn("pLat_bucket", explode(array($"pLat_bucket" - 1, $"pLat_bucket", $"pLat_bucket" + 1)))
                                        .withColumn("dLat_bucket", explode(array($"dLat_bucket" - 1, $"dLat_bucket", $"dLat_bucket" + 1)))
    val tripsInterResult = tripsBuck.as("a").join(tripsBuckNeighbors.as("b"), ($"b.pt_bucket" === $"a.dt_bucket")
                                                                        && ($"b.pLat_bucket" === $"a.dLat_bucket")
                                                                        && ($"a.pLat_bucket" === $"b.dLat_bucket"))
    val tripsResult = tripsInterResult.filter((distance($"b.dropoff_latitude", $"b.dropoff_longitude", $"a.pickup_latitude", $"a.pickup_longitude") < dist)
        && (distance($"a.dropoff_latitude", $"a.dropoff_longitude", $"b.pickup_latitude", $"b.pickup_longitude") < dist)
    ).filter(($"a.dropoff_time" < $"b.pickup_time")
        && ($"a.dropoff_time" + intervalSeconds > $"b.pickup_time"))

    tripsResult
}

// Define the data for testing
val schema = StructType(Array(
    StructField("VendorID", DataTypes.StringType,false),
    StructField("tpep_pickup_datetime", DataTypes.TimestampType,false),
    StructField("tpep_dropoff_datetime", DataTypes.TimestampType,false),
    StructField("passenger_count", DataTypes.IntegerType,false),
    StructField("trip_distance", DataTypes.DoubleType,false),
    StructField("pickup_longitude", DataTypes.DoubleType,false),
    StructField("pickup_latitude", DataTypes.DoubleType,false),
    StructField("RatecodeID", DataTypes.IntegerType,false),
    StructField("store_and_fwd_flag", DataTypes.StringType,false),
    StructField("dropoff_longitude", DataTypes.DoubleType,false),
    StructField("dropoff_latitude", DataTypes.DoubleType,false),
    StructField("payment_type", DataTypes.IntegerType,false),
    StructField("fare_amount", DataTypes.DoubleType,false),
    StructField("extra", DataTypes.DoubleType,false),
    StructField("mta_tax", DataTypes.DoubleType,false),
    StructField("tip_amount", DataTypes.DoubleType,false),
    StructField("tolls_amount", DataTypes.DoubleType,false),
    StructField("improvement_surcharge", DataTypes.DoubleType,false),
    StructField("total_amount", DataTypes.DoubleType, false)
))

val tripsDF = spark.read.schema(schema).option("header", true).csv("data/yellow_tripdata_2016-01.sample.csv")
val trips = tripsDF.where($"pickup_longitude" =!= 0 && $"pickup_latitude" =!= 0 && $"dropoff_longitude" =!= 0 && $"dropoff_latitude" =!= 0)

def time[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc // call the code
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000 + " microsecs")
    res
}

var dist = 100 // get dist in meters from driver
var result = time(compute(trips, dist).agg(count("*")).first.getLong(0))
println(result)
// Result should be 3107