import os
import polars as pl

script_dir = os.path.dirname(os.path.abspath(__file__))

match_file_path = os.path.join(script_dir, "../data/match_player_item_v2.parquet")
heroes_file_path = os.path.join(script_dir, "../data/heroes.parquet")

df = pl.read_parquet(match_file_path, columns=["hero_id"], n_rows=10_000_000)

hero_counts = (
    df.group_by("hero_id")
    .agg(pl.len().alias("times_played"))
    .sort("times_played", descending=True)
)

heroes = pl.read_parquet(heroes_file_path)
result = hero_counts.join(heroes, on="hero_id", how="left")
print(result)
