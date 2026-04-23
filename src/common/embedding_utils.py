"""
Utility functions for text preparation and embedding generation.

Purpose:
- Clean review text before embedding generation
- Build a single text field from summary + review_text
- Load a Sentence-Transformers model
- Generate embeddings in batches
- Optionally normalize embeddings for cosine similarity workflows

This module is intentionally lightweight and independent from Spark.
It is designed to be reused by the embeddings pipeline scripts.
"""

import html
import re
from typing import List, Optional, Sequence

import numpy as np
from sentence_transformers import SentenceTransformer


def clean_text(text: Optional[str]) -> Optional[str]:
    """
    Clean a single text value for embedding generation.

    Steps:
    - Return None if the input is None
    - Decode HTML entities (for example: &quot; -> ")
    - Normalize repeated whitespace to a single space
    - Strip leading and trailing whitespace
    - Return None if the final text is empty
    """
    if text is None:
        return None

    cleaned = html.unescape(text)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.strip()

    return cleaned if cleaned else None


def build_text_for_embedding(
    summary: Optional[str],
    review_text: Optional[str],
) -> Optional[str]:
    """
    Build a single text field used as model input.

    Rules:
    - Clean both summary and review_text
    - If both are present, join them as: "summary. review_text"
    - If only one is present, return that one
    - If both are missing/empty, return None
    """
    cleaned_summary = clean_text(summary)
    cleaned_review_text = clean_text(review_text)

    if cleaned_summary and cleaned_review_text:
        return f"{cleaned_summary}. {cleaned_review_text}"

    if cleaned_summary:
        return cleaned_summary

    if cleaned_review_text:
        return cleaned_review_text

    return None


def load_embedding_model(model_name: str) -> SentenceTransformer:
    """
    Load and return a Sentence-Transformers model.

    Example:
    - all-MiniLM-L6-v2
    """
    return SentenceTransformer(model_name)


def generate_embeddings(
    texts: List[str],
    model: SentenceTransformer,
    batch_size: int = 32,
) -> List[List[float]]:
    """
    Generate embeddings for a list of texts in batches.

    Notes:
    - Input texts must already be cleaned and non-empty
    - Output is returned as a standard Python list of float lists
    - convert_to_numpy=True makes the output easier to normalize later
    """
    if not texts:
        return []

    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=False,
        convert_to_numpy=True,
    )

    return embeddings.tolist()


def normalize_embeddings(vectors: List[List[float]]) -> List[List[float]]:
    """
    Apply L2 normalization to embedding vectors.

    This is useful when cosine similarity is used downstream,
    for example in pgvector-based retrieval.

    Zero vectors are returned unchanged to avoid division by zero.
    """
    if not vectors:
        return []

    array = np.array(vectors, dtype=np.float32)
    norms = np.linalg.norm(array, axis=1, keepdims=True)

    # Avoid division by zero for any unexpected zero vectors
    norms = np.where(norms == 0, 1.0, norms)

    normalized = array / norms
    return normalized.tolist()