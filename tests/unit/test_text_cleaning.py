from src.common.embedding_utils import build_text_for_embedding, clean_text


def test_clean_text_decodes_html_and_normalizes_whitespace():
    """
    Test that clean_text decodes HTML entities and normalizes whitespace.
    """
    text = 'Great &quot;toy&quot;&nbsp;for   kids.\nWorks\twell.'

    result = clean_text(text)

    assert result == 'Great "toy" for kids. Works well.'


def test_build_text_for_embedding_joins_summary_and_review_text():
    """
    Test that summary and review text are cleaned and joined correctly.
    """
    summary = 'Great &quot;toy&quot;'
    review_text = 'My child&nbsp;loved it.   Works\nwell.'

    result = build_text_for_embedding(summary, review_text)

    expected = 'Great "toy". My child loved it. Works well.'

    assert result == expected


def test_build_text_for_embedding_returns_none_for_empty_values():
    """
    Test that empty or missing text values return None.
    """
    result = build_text_for_embedding("   ", None)

    assert result is None