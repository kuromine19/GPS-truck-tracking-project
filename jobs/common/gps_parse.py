"""Parse + clean điểm GPS, dùng chung cho batch và streaming."""
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

POINT_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("VehicleCode", T.StringType()),
    T.StructField("GPSTime", T.StringType()),
    T.StructField("CreatedTime", T.StringType()),
    T.StructField("Lat", T.DoubleType()),
    T.StructField("Lng", T.DoubleType()),
    T.StructField("SpeedMax", T.DoubleType()),
    T.StructField("Heading", T.DoubleType()),
    T.StructField("KM", T.DoubleType()),
    T.StructField("Status", T.LongType()),
    T.StructField("Note", T.StringType()),
    T.StructField("CreatedBy", T.StringType()),
]))

# bbox VN
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = 8.0, 23.5, 102.0, 110.0


def pg_array_to_json(col: Column) -> Column:
    # PG array -> JSON array. Tách phần tử trước rồi mới unescape
    s = F.regexp_replace(col, r'^\{"', "[")
    s = F.regexp_replace(s, r'"\}$', "]")
    s = F.regexp_replace(s, r'\}","\{', "},{")
    s = F.regexp_replace(s, r"\\(.)", "$1")
    return F.when(F.trim(col) == "{}", F.lit("[]")).otherwise(s)


def parse_points(col: Column) -> Column:
    return F.from_json(pg_array_to_json(col), POINT_SCHEMA)


def local_ts(col: Column) -> Column:
    # giờ VN, bỏ hậu tố múi giờ
    return F.to_timestamp(F.substring(col, 1, 19), "yyyy-MM-dd'T'HH:mm:ss")


def provider_of(col: Column) -> Column:
    # "...-EupfinProcessor-XXX" -> "Eupfin"
    p = F.regexp_extract(col, r"-([A-Za-z0-9]+?)Processor", 1)
    return F.when(p != "", p).when(col.startswith("TMS."), F.regexp_replace(col, r"^TMS\.", "")) \
            .otherwise(col)


def address_of(col: Column) -> Column:
    return F.when(col.startswith("{"), F.get_json_object(col, "$.Address"))


def vehicle_key(salt: str, code: Column) -> Column:
    return F.xxhash64(F.lit(salt), code)


def flatten_point(p: str = "p") -> list:
    return [
        local_ts(F.col(f"{p}.GPSTime")).alias("device_time"),
        local_ts(F.col(f"{p}.CreatedTime")).alias("recv_time"),
        F.col(f"{p}.Lat").alias("lat"),
        F.col(f"{p}.Lng").alias("lon"),
        F.col(f"{p}.SpeedMax").alias("speed"),
        F.col(f"{p}.KM").alias("odometer"),
        F.col(f"{p}.Status").alias("status"),
        F.col(f"{p}.Heading").cast("int").alias("heading"),
        provider_of(F.col(f"{p}.CreatedBy")).alias("provider"),
        address_of(F.col(f"{p}.Note")).alias("address"),
    ]


def reject_reason(max_speed: float, max_late_hours: float) -> Column:
    # null = hợp lệ. stale_gps: thiết bị gửi lặp vị trí cũ (từ 2018-2020...)
    lat, lon, spd = F.col("lat"), F.col("lon"), F.col("speed")
    delay_s = F.col("recv_time").cast("long") - F.col("device_time").cast("long")
    return (F.when(F.col("gps_time").isNull(), "missing_time")
             .when(delay_s > max_late_hours * 3600, "stale_gps")
             .when(lat.isNull() | lon.isNull(), "missing_coord")
             .when((lat == 0) & (lon == 0), "zero_coord")
             .when(~lat.between(LAT_MIN, LAT_MAX) | ~lon.between(LON_MIN, LON_MAX), "outside_vietnam")
             .when((spd < 0) | (spd > max_speed), "bad_speed"))


def fix_clock(max_future_min: float) -> tuple:
    # giờ thiết bị null hoặc ở tương lai -> dùng giờ server
    dev, recv = F.col("device_time"), F.col("recv_time")
    delay_s = recv.cast("long") - dev.cast("long")
    bad = dev.isNull() | (delay_s < -max_future_min * 60)
    fixed = bad & recv.isNotNull()
    return fixed.alias("clock_fixed"), F.when(bad, recv).otherwise(dev).alias("gps_time")


def haversine_km(lat1, lon1, lat2, lon2) -> Column:
    r = F.radians
    dlat, dlon = r(lat2) - r(lat1), r(lon2) - r(lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(r(lat1)) * F.cos(r(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * 6371.0 * F.asin(F.sqrt(a))
