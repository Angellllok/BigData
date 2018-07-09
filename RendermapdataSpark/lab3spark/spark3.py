import numpy as np
import gmplot
from pyspark.sql import SparkSession


rt1_columns = ["rt", "version", "tlid", "side1", "source", "fedirp", "fename", "fetype", "fedirs", "cfcc",
               "fraddl", "toaddl", "fraddr", "toaddr", "friaddl", "toiaddl", "friaddr", "toiaddr", "zipl", "zipr",
               "aianhhfpl", "aianhhfpr", "aihhtlil", "aihhtlir", "census1", "census2", "statel", "stater",
               "countyl", "countyr", "cousubl", "cousubr", "submcdl", "submcdr", "placel", "placer",
               "tractl", "tractr", "blockl", "blockr", "frlong", "frlat", "tolong", "tolat"]
rt2_columns = ["rt", "version", "tlid", "rtsq", "longitudes", "latitudes"]


def rt1_parser(s):
    rt = s[0].strip()                      # 0 - rt, str
    version = int(s[1:5].strip())          # 1 - version, int
    tlid = int(s[5:15].strip())            # 2 - TLID, int
    side1 = int(s[15].strip() or 0)        # 3 - SIDE1, int
    source = s[16].strip()                 # 4 - SOURCE, str
    fedirp = s[17:19].strip()              # 5 - FEDIRP, str
    fename = s[19:49].strip()              # 6 - FENAME, str
    fetype = s[49:53].strip()              # 7 - FETYPE, str
    fedirs = s[53:55].strip()              # 8 - Fedirs, str
    cfcc = s[55:58].strip()                # 9 - CFCC, str
    fraddl = s[58:69].strip()              # 10 - FRADDL, str
    toaddl = s[69:80].strip()              # 11 - TOADDL, str
    fraddr = s[80:91].strip()              # 12 - FRADDR, str
    toaddr = s[91:102].strip()             # 13 - TOADDR, str
    friaddl = s[102].strip()               # 14 - FRIADDL, str
    toiaddl = s[103].strip()               # 15 - TOIADDL, str
    friaddr = s[104].strip()               # 16 - FRIADDR, str
    toiaddr = s[105].strip()               # 17 - TOIADDR, str
    zipl = int(s[106:111].strip() or 0)    # 18 - ZIPL, int
    zipr = int(s[111:116].strip() or 0)    # 19 - ZIPR, int
    aianhhfpl = int(s[116:121].strip() or 0)# 20 - AIANHHFPL, int
    aianhhfpr = int(s[121:126].strip() or 0)# 21 - AIANHHFPR, int

    aihhtlil = s[126].strip()              # 22 - AIHHTLIL, str
    aihhtlir = s[127].strip()              # 23 - AIHHTLIR, str
    census1 = s[128].strip()               # 24 - CENSUS1, str
    census2 = s[129].strip()               # 25 - CENSUS2, str
    statel = int(s[130:132].strip() or 0)  # 26 - STATEL, str
    stater = int(s[132:134].strip() or 0)  # 27 - STATER, str
    countyl = int(s[134:137].strip() or 0) # 28 - COUNTYL, str
    countyr = int(s[137:140].strip() or 0) # 29 - COUNTYR, str
    cousubl = int(s[140:145].strip() or 0) # 30 - COUSUBL, str
    cousubr = int(s[145:150].strip() or 0) # 31 - COUSUBR, str

    submcdl = int(s[150:155].strip() or 0) # 32 - SUBMCDL, int
    submcdr = int(s[155:160].strip() or 0) # 33 - SUBMCDR, int
    placel = int(s[160:165].strip() or 0)  # 34 - PLACEL, int
    placer = int(s[165:170].strip() or 0)  # 35 - PLACER, int
    tractl = int(s[170:176].strip() or 0)  # 36 - TRACTL, int
    tractr = int(s[176:182].strip() or 0)  # 37 - TRACTR, int
    blockl = int(s[182:186].strip() or 0)  # 38 - BLOCKL, int
    blockr = int(s[186:190].strip() or 0)  # 39 - BLOCKR, int
    frlong = int(s[190:200].strip() or 0)  # 40 - FRLONG, float
    frlat = int(s[200:209].strip() or 0)   # 41 - FRLAT, float
    tolong = int(s[209:219].strip() or 0)  # 42 - TOLONG, float
    tolat = int(s[219:228].strip() or 0)   # 43 - TOLAT, float

    return (rt, version, tlid, side1, source, fedirp, fename, fetype, fedirs, cfcc, fraddl, toaddl,
            fraddr, toaddr, friaddl, toiaddl, friaddr, toiaddr, zipl, zipr, aianhhfpl, aianhhfpr,
            aihhtlil, aihhtlir, census1, census2, statel, stater, countyl, countyr, cousubl, cousubr,
            submcdl, submcdr, placel, placer, tractl, tractr, blockl, blockr, frlong, frlat, tolong, tolat)


def rt2_parser(s):
    rt = s[0].strip()                      # 0 - rt, str
    version = int(s[1:5].strip())          # 1 - version, int
    tlid = int(s[5:15].strip())            # 2 - TLID, int

    rtsq = int(s[15:18].strip() or 0)      # 3 - RTSQ, int
    long1 = int(s[18:28].strip() or 0)     # 4 - LONG1, int
    lat1 = int(s[28:37].strip() or 0)      # 5 - LAT1, int
    long2 = int(s[37:47].strip() or 0)     # 6 - LONG2, int
    lat2 = int(s[47:56].strip() or 0)      # 7 - LAT2, int
    long3 = int(s[56:66].strip() or 0)     # 8 - LONG3, int
    lat3 = int(s[66:75].strip() or 0)      # 9 - LAT3, int
    long4 = int(s[75:85].strip() or 0)     # 10 - LONG4, int
    lat4 = int(s[85:94].strip() or 0)      # 11 - LAT4, int
    long5 = int(s[94:104].strip() or 0)    # 12 - LONG5, int
    lat5 = int(s[104:113].strip() or 0)    # 13 - LAT5, int
    long6 = int(s[113:123].strip() or 0)   # 14 - LONG6, int
    lat6 = int(s[123:132].strip() or 0)    # 15 - LAT6, int
    long7 = int(s[132:142].strip() or 0)   # 16 - LONG7, int
    lat7 = int(s[142:151].strip() or 0)    # 17 - LAT7, int
    long8 = int(s[151:161].strip() or 0)   # 18 - LONG8, int
    lat8 = int(s[161:170].strip() or 0)    # 19 - LAT8, int
    long9 = int(s[170:180].strip() or 0)   # 20 - LONG9, int
    lat9 = int(s[180:189].strip() or 0)    # 21 - LAT9, int
    long10 = int(s[189:199].strip() or 0)  # 22 - LONG10, int
    lat10 = int(s[199:208].strip() or 0)   # 23 - LAT10, int

    return (rt, version, tlid, rtsq,
            [x for x in [long1, long2, long3, long4, long5, long6, long7, long8, long9, long10] if x],
            [x for x in [lat1, lat2, lat3, lat4, lat5, lat6, lat7, lat8, lat9, lat10] if x])


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PythonTIGER").getOrCreate()

    df1 = spark.read.text("TGR53033/TGR53033.RT1").rdd.map(lambda r: rt1_parser(r[0])).toDF(rt1_columns)
    df2 = spark.read.text("TGR53033/TGR53033.RT2").rdd.map(lambda r: rt2_parser(r[0])).toDF(rt2_columns)
    joined_df = df1.join(df2, df1.tlid == df2.tlid).collect()
    spark.stop()

    locations = ((np.column_stack((x.latitudes, x.longitudes)), x.fename, x.fetype) for x in joined_df)

    gmap = gmplot.GoogleMapPlotter(47.5, -121.65, 16)
    for location in locations:
        latlon = location[0] / 1000000
        gmap.plot(latlon[:, 0], latlon[:, 1], "red", edge_width=2)
        if location[1] or location[2]:
            gmap.marker(latlon[:, 0].mean(), latlon[:, 1].mean(), title="{location[1]}, {location[2]}")
    gmap.draw("result.html")
