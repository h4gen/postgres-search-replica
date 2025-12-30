from src.transformer import transform_data
from src.config import settings


def test_transform_data_empty():
    assert transform_data([]) == []


def test_transform_data_logic():
    # Use configured column names for input
    # Assuming input rows match publication_columns which are ["id", "email"] by default
    input_rows = [(1, "ALICE@EXAMPLE.COM"), (2, "bob@Work.org")]
    result = transform_data(input_rows)

    assert len(result) == 2

    # Check Alice
    alice = next(r for r in result if r[settings.id_column] == 1)
    assert alice[settings.target_content_column] == "alice@example.com"
    assert len(alice[settings.embedding_column]) == settings.embedding_dimension

    # Check Bob
    bob = next(r for r in result if r[settings.id_column] == 2)
    assert bob[settings.target_content_column] == "bob@work.org"
    assert len(bob[settings.embedding_column]) == settings.embedding_dimension


def test_transform_data_with_nulls():
    # Test how the transformer handles NULL/None values
    input_rows = [(1, "ALICE@EXAMPLE.COM"), (2, None)]

    # We expect this not to crash.
    # Note: If Polars fails on .str.to_lowercase() for None, we'll need to fix src/transformer.py
    result = transform_data(input_rows)

    assert len(result) == 2
    alice = next(r for r in result if r[settings.id_column] == 1)
    assert alice[settings.target_content_column] == "alice@example.com"

    bob = next(r for r in result if r[settings.id_column] == 2)
    assert bob[settings.target_content_column] is None
    # Depending on how the dummy vectorizer handles None, this might be a random vector or empty
    assert len(bob[settings.embedding_column]) == settings.embedding_dimension
