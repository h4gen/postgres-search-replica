import polars as pl
from typing import List, Dict, Any
from src.config import settings
from src.vectorizers import VECTORIZER_REGISTRY


def transform_data(rows: List[tuple]) -> List[Dict[str, Any]]:
    """
    Pure transformation logic using Polars.
    Input: List of tuples based on settings.publication_columns
    Output: List of dicts with transformed data and embeddings
    """
    if not rows:
        return []

    # 1. Load into Polars using configured columns
    df = pl.DataFrame(rows, schema=settings.publication_columns, orient="row")

    # Get the vectorizer function from the registry
    vectorizer_fn = VECTORIZER_REGISTRY.get(settings.vectorizer_type)
    if not vectorizer_fn:
        raise ValueError(
            f"Vectorizer '{settings.vectorizer_type}' not found in registry."
        )

    # 2. Extract texts for batch vectorization
    texts = df[settings.content_column].to_list()
    embeddings = vectorizer_fn(texts, settings.embedding_dimension)

    # 3. Transform: Lowercase and integrate batch embeddings
    transformed = df.with_columns(
        [
            pl.col(settings.content_column)
            .str.to_lowercase()
            .alias(settings.target_content_column),
            pl.Series(name=settings.embedding_column, values=embeddings),
        ]
    ).select(
        [
            settings.id_column,
            settings.target_content_column,
            settings.embedding_column,
        ]
    )

    return transformed.to_dicts()
