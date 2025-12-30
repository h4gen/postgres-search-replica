import hashlib
import numpy as np
from typing import List, Callable


def dummy_vectorizer(texts: List[str], dim: int) -> List[List[float]]:
    """Generate deterministic 'dummy' vectors from a list of texts."""
    results = []
    for text in texts:
        # Use MD5 hash to seed a local random generator
        # Handle None values by using an empty string or a special token
        safe_text = text if text is not None else ""
        seed = int(hashlib.md5(safe_text.encode()).hexdigest(), 16) % (2**32)
        rng = np.random.default_rng(seed)
        results.append(rng.random(dim).tolist())
    return results


# Registry mapping vectorizer names to their implementation functions
VECTORIZER_REGISTRY: dict[
    str, Callable[[List[str], int], List[List[float]]]
] = {
    "dummy": dummy_vectorizer,
}
