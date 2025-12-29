import polars as pl
import numpy as np
from typing import List, Dict, Any


def transform_user_data(rows: List[tuple]) -> List[Dict[str, Any]]:
    """
    Pure transformation logic using Polars.
    Input: List of tuples (id, email)
    Output: List of dicts with transformed data and embeddings
    """
    if not rows:
        return []

    # 1. Load into Polars
    df = pl.DataFrame(rows, schema=["id", "email"], orient="row")

    # 2. Transform: Lowercase, mask email, and generate dummy embedding
    transformed = df.with_columns(
        [
            pl.col("email")
            .str.to_lowercase()
            .str.replace(r"@.*", "@masked-replica.com")
            .alias("transformed_email"),
            # Generating a dummy 3D vector
            pl.col("email")
            .map_elements(
                lambda x: np.random.rand(3).tolist(), return_dtype=pl.List(pl.Float64)
            )
            .alias("embedding"),
        ]
    ).select(["id", "transformed_email", "embedding"])

    return transformed.to_dicts()
