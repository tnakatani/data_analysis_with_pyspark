# Full schema from scratch
import pyspark.sql.types as T

# episode links
episode_links_schema = T.StructType(
    [T.StructField("self", T.StructType([T.StructField("href", T.StringType())]))]
)

# episode image
episode_image_schema = T.StructType(
    [
        T.StructField("medium", T.StringType()),
        T.StructField("original", T.StringType()),
    ]
)

# episode metadata
episode_schema = T.StructType(
    [
        T.StructField("_links", episode_links_schema),
        T.StructField("airdate", T.DateType()),
        T.StructField("airstamp", T.TimestampType()),
        T.StructField("airtime", T.StringType()),
        T.StructField("id", T.StringType()),
        T.StructField("image", episode_image_schema),
        T.StructField("name", T.StringType()),
        T.StructField("number", T.LongType()),
        T.StructField("runtime", T.LongType()),
        T.StructField("season", T.LongType()),
        T.StructField("summary", T.StringType()),
        T.StructField("url", T.StringType()),
    ]
)

# set top level array
embedded_schema = T.StructType([T.StructField("episodes", T.ArrayType(episode_schema))])

# network
network_schema = T.StructType(
    [
        T.StructField(
            "country",
            T.StructType(
                [
                    T.StructField("code", T.StringType()),
                    T.StructField("name", T.StringType()),
                    T.StructField("timezone", T.StringType()),
                ]
            ),
        ),
        T.StructField("id", T.LongType()),
        T.StructField("name", T.StringType()),
    ]
)

# shows (with embedded_schema and network_schema)
shows_schema = T.StructType(
    [
        T.StructField("_embedded", embedded_schema),
        T.StructField("language", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("network", network_schema),
        T.StructField("officialSite", T.StringType()),
        T.StructField("premiered", T.StringType()),
        T.StructField(
            "rating", T.StructType([T.StructField("average", T.DoubleType())])
        ),
        T.StructField("runtime", T.LongType()),
        T.StructField(
            "schedule",
            T.StructType(
                [
                    T.StructField("days", T.ArrayType(T.StringType())),
                    T.StructField("time", T.StringType()),
                ]
            ),
        ),
        T.StructField("status", T.StringType()),
        T.StructField("summary", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("updated", T.LongType()),
        T.StructField("url", T.StringType()),
        T.StructField("webChannel", T.StringType()),
        T.StructField("weight", T.LongType()),
        T.StructField("id", T.LongType()),
    ]
)