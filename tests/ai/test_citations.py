"""Tests for the citations formatter (app/ai/citations.py)."""

from app.ai.citations import format_all_citations


def test_empty_results_returns_empty_string():
    """No tool results means no citation block."""
    assert format_all_citations([]) == ""


def test_results_without_citation_key():
    """Results lacking _citation key produce no output."""
    results = [{"data": [1, 2, 3]}, {"value": "ok"}]
    assert format_all_citations(results) == ""


def test_single_citation():
    """A single result with _citation produces a numbered citation block."""
    results = [{"_citation": "World Bank WDI"}]
    out = format_all_citations(results)
    assert "Sources:" in out
    assert "[1] World Bank WDI" in out


def test_multiple_citations():
    """Multiple distinct citations are numbered sequentially."""
    results = [
        {"_citation": "FRED"},
        {"_citation": "WDI"},
        {"_citation": "IMF WEO"},
    ]
    out = format_all_citations(results)
    assert "[1] FRED" in out
    assert "[2] WDI" in out
    assert "[3] IMF WEO" in out


def test_duplicate_citations_deduplicated():
    """Duplicate _citation values appear only once in the output."""
    results = [
        {"_citation": "FRED"},
        {"_citation": "FRED"},
        {"_citation": "WDI"},
    ]
    out = format_all_citations(results)
    # FRED must appear exactly once
    assert out.count("FRED") == 1
    assert "[1] FRED" in out
    assert "[2] WDI" in out


def test_none_citation_skipped():
    """A result with _citation=None must be skipped gracefully."""
    results = [{"_citation": None}, {"_citation": "BACI"}]
    out = format_all_citations(results)
    assert "[1] BACI" in out
    assert "None" not in out


def test_mixed_citation_and_no_citation():
    """Mix of results with and without _citation only counts those with it."""
    results = [
        {"data": "x"},
        {"_citation": "ILO"},
        {"other": True},
        {"_citation": "FAOSTAT"},
    ]
    out = format_all_citations(results)
    assert "[1] ILO" in out
    assert "[2] FAOSTAT" in out


def test_output_format_starts_with_newline():
    """Citation block starts with an empty line before 'Sources:'."""
    results = [{"_citation": "FRED"}]
    out = format_all_citations(results)
    assert out.startswith("\n")


def test_output_is_string():
    """format_all_citations always returns a string."""
    assert isinstance(format_all_citations([]), str)
    assert isinstance(format_all_citations([{"_citation": "X"}]), str)
