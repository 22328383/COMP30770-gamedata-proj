import os
import polars as pl

script_dir = os.path.dirname(os.path.abspath(__file__))

match_file = os.path.join(script_dir, "../data/match_player_item_v2.parquet")
heroes_file = os.path.join(script_dir, "../data/heroes.parquet")
mmr_file = os.path.join(script_dir, "../data/mmr_history.parquet")


# Do certain heroes perform better at different skill levels?
# -mmr_history (250MB) needs to be joined with match_player_item_v2 (1.6GB) so we’ll filter MMR
# first.
# -Join match_player_item_v2 with mmr_history using account_id -> MMR brackets -> group by
# hero_id and MMR bracket -> win rate per bracket

#so i need to link each game with their account_id to their mmr
#i need to join match player to mmr

#remove n_rows if you want to analyse full dataset
df_match = pl.read_parquet(match_file, columns=["hero_id", "account_id", "match_id", "won"], n_rows=100_000_000) 
df_heroes = pl.read_parquet(heroes_file)
df_mmr = pl.read_parquet(mmr_file, columns=["account_id", "match_id", "player_score"])
df_mmr = df_mmr.with_columns([pl.col("account_id").cast(pl.UInt32)])

df_joined = df_match.join(df_mmr, on=["account_id", "match_id"], how="inner")


df_joined = df_joined.unique()

#creating mmr brackets

df_joined = df_joined.with_columns([
    pl.when(pl.col("player_score") < 300).then(pl.lit("<300"))
     .when(pl.col("player_score") < 600).then(pl.lit("300–599"))
     .when(pl.col("player_score") < 900).then(pl.lit("600–899"))
     .when(pl.col("player_score") < 1200).then(pl.lit("900–1199"))
     .when(pl.col("player_score") < 1500).then(pl.lit("1200–1499"))
     .when(pl.col("player_score") < 1800).then(pl.lit("1500–1799"))
     .when(pl.col("player_score") < 2100).then(pl.lit("1800–2099"))
     .when(pl.col("player_score") < 2400).then(pl.lit("2100–2399"))
     .when(pl.col("player_score") < 2700).then(pl.lit("2400–2699"))
     .when(pl.col("player_score") < 3000).then(pl.lit("2700–2999"))
     .otherwise(pl.lit("3000+"))
     .alias("mmr_bracket")
])


hero_winrates = df_joined.group_by(["hero_id", "mmr_bracket"]).agg([
    pl.len().alias("games_played"),
    pl.sum("won").alias("wins")
])

hero_winrates = hero_winrates.with_columns(
    (pl.col("wins") / pl.col("games_played") * 100).round(2).alias("win_rate_percent")
)

hero_winrates = hero_winrates.join(df_heroes, on="hero_id", how="left")

hero_winrates = hero_winrates.with_columns([
    pl.when(pl.col("mmr_bracket") == "<300").then(0)
     .when(pl.col("mmr_bracket") == "300–599").then(1)
     .when(pl.col("mmr_bracket") == "600–899").then(2)
     .when(pl.col("mmr_bracket") == "900–1199").then(3)
     .when(pl.col("mmr_bracket") == "1200–1499").then(4)
     .when(pl.col("mmr_bracket") == "1500–1799").then(5)
     .when(pl.col("mmr_bracket") == "1800–2099").then(6)
     .when(pl.col("mmr_bracket") == "2100–2399").then(7)
     .when(pl.col("mmr_bracket") == "2400–2699").then(8)
     .when(pl.col("mmr_bracket") == "2700–2999").then(9)
     .when(pl.col("mmr_bracket") == "3000+").then(10)
     .alias("bracket_index")
])
hero_winrates = hero_winrates.sort(["hero_id", "bracket_index"])
hero_winrates=hero_winrates.drop("bracket_index")


pl.Config.set_tbl_rows(6000)
print(hero_winrates)






