from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MostPlayedHeroesRDD").getOrCreate()

match_file = "file:///Users/andrei/Documents/UCD_BigData/COMP30770-gamedata-proj/data/match_player_item_v2.parquet"
heroes_file = "file:///Users/andrei/Documents/UCD_BigData/COMP30770-gamedata-proj/data/heroes.parquet"

df = spark.read.parquet(match_file).select("hero_id").limit(10_000_000)

hero_counts = (
    df.rdd.map(lambda row: (row["hero_id"], 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: -x[1])
    .collect()
)

heroes_df = spark.read.parquet(heroes_file)
hero_dict = {row["hero_id"]: row["hero_name"] for row in heroes_df.collect()}

print("\nTop 10 Popular Heroes:")
for hero_id, count in hero_counts[:10]:
    name = hero_dict.get(hero_id, "Unknown")
    print(f"{name} (ID: {hero_id}) — {count} games")

spark.stop()
