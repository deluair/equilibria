"""Claude-powered agentic analysis loop for Equilibria."""

from __future__ import annotations

import json
import logging

import anthropic

from app.ai.citations import format_all_citations
from app.ai.tools import execute_tool, get_tool_definitions
from app.config import settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are Equilibria, an AI applied economics analyst with access to tools "
    "covering trade, macro, labor, development, and agricultural economics. "
    "Always use tools to get data before making claims. Cite sources. "
    "Be direct and analytical."
)

MODEL = "claude-sonnet-4-6"
MAX_TOOL_ROUNDS = 10


async def analyze(
    question: str,
    conversation_history: list[dict] | None = None,
) -> dict:
    """Run the agentic analysis loop.

    Sends the question to Claude with tool definitions. If Claude requests
    tool use, executes the tools and feeds results back, looping up to
    MAX_TOOL_ROUNDS times. Returns the final response with citations and
    usage stats.
    """
    client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    tools = get_tool_definitions()

    messages = list(conversation_history or [])
    messages.append({"role": "user", "content": question})

    all_tool_results: list[dict] = []
    tool_calls_log: list[dict] = []
    total_input_tokens = 0
    total_output_tokens = 0

    for _round in range(MAX_TOOL_ROUNDS):
        response = await client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=tools,
            messages=messages,
        )

        total_input_tokens += response.usage.input_tokens
        total_output_tokens += response.usage.output_tokens

        if response.stop_reason != "tool_use":
            # Final text response
            text_parts = [
                block.text for block in response.content if block.type == "text"
            ]
            final_text = "\n".join(text_parts)
            citation_block = format_all_citations(all_tool_results)
            if citation_block:
                final_text += citation_block

            return {
                "response": final_text,
                "citations": [
                    r.get("_citation") for r in all_tool_results if r.get("_citation")
                ],
                "tool_calls": tool_calls_log,
                "token_usage": {
                    "input_tokens": total_input_tokens,
                    "output_tokens": total_output_tokens,
                    "total_tokens": total_input_tokens + total_output_tokens,
                },
            }

        # Process tool use blocks
        assistant_content = response.content
        messages.append({"role": "assistant", "content": assistant_content})

        tool_results_for_message = []
        for block in assistant_content:
            if block.type != "tool_use":
                continue

            tool_name = block.name
            tool_input = block.input
            tool_use_id = block.id

            logger.info("Tool call [round %d]: %s(%s)", _round + 1, tool_name, json.dumps(tool_input, default=str)[:200])
            tool_calls_log.append({"tool": tool_name, "input": tool_input, "round": _round + 1})

            result = await execute_tool(tool_name, tool_input)
            all_tool_results.append(result)

            tool_results_for_message.append({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": json.dumps(result, default=str),
            })

        messages.append({"role": "user", "content": tool_results_for_message})

    # Exhausted all rounds without a final text response
    logger.warning("Exhausted %d tool rounds without final response", MAX_TOOL_ROUNDS)
    return {
        "response": "Analysis could not be completed within the maximum number of tool rounds. Please try a more specific question.",
        "citations": [r.get("_citation") for r in all_tool_results if r.get("_citation")],
        "tool_calls": tool_calls_log,
        "token_usage": {
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "total_tokens": total_input_tokens + total_output_tokens,
        },
    }
