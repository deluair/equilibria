"""Citation formatting for AI tool results."""


def format_all_citations(results: list[dict]) -> str:
    """Format citation lines from tool results.

    Each tool result dict may contain a _citation key with source info.
    Returns a numbered citation block suitable for appending to AI responses.
    """
    citations = []
    seen = set()
    for r in results:
        cite = r.get("_citation")
        if not cite or cite in seen:
            continue
        seen.add(cite)
        citations.append(cite)
    if not citations:
        return ""
    lines = ["", "Sources:"]
    for i, c in enumerate(citations, 1):
        lines.append(f"[{i}] {c}")
    return "\n".join(lines)
