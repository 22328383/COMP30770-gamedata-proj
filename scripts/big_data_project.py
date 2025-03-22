import pandas as pd

df_heroes = pd.read_parquet('data/heroes.parquet')

df_mmr = pd.read_parquet('data/mmr_history.parquet')
df_mmr_sorted = df_mmr.sort_values('player_score')

columns_to_load = ['account_id', 'hero_id', 'won']
df_match_player_item_v2 = pd.read_parquet('data/match_player_item_v2.parquet', columns=columns_to_load)

print(df_mmr_sorted.tail())