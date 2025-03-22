import os
import polars as pl

script_dir = os.path.dirname(os.path.abspath(__file__))

files = {
    "match_player_item_v2": os.path.join(script_dir, "../data/match_player_item_v2.parquet"),
    "mmr_history": os.path.join(script_dir, "../data/mmr_history.parquet"),
    "heroes": os.path.join(script_dir, "../data/heroes.parquet")
}

for name, path in files.items():
    print(f"\n🔹 Random 10 rows of {name}.parquet:\n")
    try:
        df = pl.read_parquet(path, n_rows=5000)

        sample = df.sample(n=10, with_replacement=False)
        print(sample)

    except Exception as e:
        print(f"❌ Failed to read {name}: {e}")
