import os
import polars as pl

script_dir = os.path.dirname(os.path.abspath(__file__))
match_file = os.path.join(script_dir, "../data/match_player_item_v2.parquet")
mmr_file = os.path.join(script_dir, "../data/mmr_history.parquet")
heroes_file = os.path.join(script_dir, "../data/heroes.parquet")

df_match = pl.read_parquet(match_file,columns=["account_id", "match_id", "hero_id"],n_rows=10_000_000)
df_mmr = pl.read_parquet(mmr_file,columns=["account_id", "match_id", "player_score"]).filter(pl.col("player_score") >= 2100)

df_mmr = df_mmr.with_columns([pl.col("account_id").cast(pl.UInt32),pl.col("match_id").cast(pl.UInt64)])

df_joined = df_match.join(df_mmr, on=["account_id", "match_id"], how="inner")

hero_popularity = df_joined.group_by("hero_id").agg(pl.len().alias("times_picked"))

heroes = pl.read_parquet(heroes_file)
result = hero_popularity.join(heroes, on="hero_id", how="left")
result = result.sort("times_picked", descending=True)

print("\nTop 10 Heroes for MMR above 2100:")
print(result.head(10))
