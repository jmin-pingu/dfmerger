import polars as pl
import os
import typing
from collections.abc import Callable

class Level:
    def __init__(self, threshold: int, scorer: Callable[[str, str], float], sieve: pl.Expr):
        self.threshold = threshold
        self.sieve = sieve
        self.scorer = scorer

    def set_threshold(self, threshold: int):
        self.threshold = threshold

    def set_sieve(self, sieve: pl.Expr):
        self.sieve = sieve

    def set_scorer(self, scorer: Callable[[str, str], float]):
        self.scorer = scorer

class MergerConfig:
    def __init__(self, cache: True, parallel: True, nshards: int):
        self.cache = cache
        self.parallel = parallel
        self.nshards = nshards
        pass

# Sort then split into shards
class Merger:
    def __init__(self, df: pl.DataFrame):
        self.dataframe = df
        self.hierarchy = []
        # NOTE: Make the common case fast

    def config(self):
        pass  

    def add_level(self, level: Level):
        pass  

    def remove_level(self, level: Level):
        pass  

    def set_level(self, level: Level):
        pass  

    def show_levels(self, level: Level):
        pass  
        
    def merge(self, other, left_on: str, right_on: str, how: str, suffix: str = ""):
        if len(self.hierarchy) == 0:
            # Default join. 
            return self.dataframe.join(other, left_on=left_on, right_on=right_on, suffix=suffix, how=how)
        else: 
            # Do
            # NOTE: the cache should be implemented as a shared data structure
            df.rows(named=True)
            pass

def main():
    cwd = os.getcwd()
    df2 = pl.read_csv(cwd + "/data/programming_languages.csv").head()
    print(df2)
    df = pl.read_csv(cwd + "/data/stack_overflow_entries.csv")

    df = pl.DataFrame(
        {
            "foo": [1, 2, 3],
            "bar": [6.0, 7.0, 8.0],
            "ham": ["a", "b", "c"],
        }
    )
    other_df = pl.DataFrame(
        {
            "apple": ["x", "y", "z"],
            "ham": ["a", "b", "d"],
        }
    )

    merger = Merger(df)
    print(merger.merge(other_df, left_on="ham", right_on="ham", how="right"))
    
if __name__ == "__main__":
    main()
