from typing import Iterator
import pandas as pd
import numpy as np


class BatchGenerator:
    """Models a simple batch generator that make chunks out of an input DataFrame."""

    def __init__(self, batch_size: int = 10) -> None:
        self.batch_size = batch_size

    def to_batches(self, df: pd.DataFrame) -> Iterator[pd.DataFrame]:
        """Makes chunks out of an input DataFrame."""
        splits = self.splits_num(df.shape[0])
        if splits <= 1:
            yield df
        else:
            for chunk in np.array_split(df, splits):
                yield chunk

    def splits_num(self, elements: int) -> int:
        """Determines how many chunks DataFrame contians."""
        return round(elements / self.batch_size)

    __call__ = to_batches
