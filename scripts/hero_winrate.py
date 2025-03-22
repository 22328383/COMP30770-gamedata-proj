import os
import polars as pl

script_dir = os.path.dirname(os.path.abspath(__file__))

match_file_path = os.path.join(script_dir, "../data/match_player_item_v2.parquet")
heroes_file_path = os.path.join(script_dir, "../data/heroes.parquet")

# Which heroes have the highest win rates?
# -match_player_item_v2 is large so we aggregate before filtering to avoid reads.
# -Group matches by hero_id -> count total matches - > count total wins per hero -> sum won
# column -> compute win rate -> (total wins / total games) * 100 -> sort heroes by win rate.

df = pl.read_parquet(match_file_path, columns=["hero_id", "won"])

heroes = pl.read_parquet(heroes_file_path)
hero_stats = df.group_by("hero_id").agg([
    pl.len().alias("Total"),
    pl.sum("won").alias("Wins")
]).fill_null(0)

heroes = heroes.join(hero_stats, on="hero_id", how="left")

heroes = heroes.with_columns(
    (pl.col("Wins") / pl.col("Total") * 100).alias("Win_Percentage")
).filter(pl.col("Total") > 0)

heroes_sorted = heroes.sort("Win_Percentage", descending=True)

pl.Config.set_tbl_rows(60)
print(heroes_sorted)

    