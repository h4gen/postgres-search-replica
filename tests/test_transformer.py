import pytest
from src.transformer import transform_user_data

def test_transform_user_data_empty():
    assert transform_user_data([]) == []

def test_transform_user_data_logic():
    input_rows = [(1, "ALICE@EXAMPLE.COM"), (2, "bob@Work.org")]
    result = transform_user_data(input_rows)
    
    assert len(result) == 2
    
    # Check Alice
    alice = next(r for r in result if r["id"] == 1)
    assert alice["transformed_email"] == "alice@masked-replica.com"
    assert len(alice["embedding"]) == 3
    
    # Check Bob
    bob = next(r for r in result if r["id"] == 2)
    assert bob["transformed_email"] == "bob@masked-replica.com"
    assert len(bob["embedding"]) == 3

