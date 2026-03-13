import datetime
import operator
from enum import Enum
from enum import auto
from itertools import count
from typing import Any


class TrieResult(Enum):
    FAILED = auto()
    PREFIX = auto()
    EXISTS = auto()


def new_trie(keywords: Any, trie: dict | None = None) -> dict:
    trie = {} if trie is None else trie
    for key in keywords:
        current = trie
        for char in key:
            current = current.setdefault(char, {})
        current[0] = True
    return trie


def in_trie(trie: dict, key: str) -> tuple[TrieResult, dict]:
    if not key:
        return (TrieResult.FAILED, trie)
    current = trie
    for char in key:
        if char not in current:
            return (TrieResult.FAILED, current)
        current = current[char]
    if 0 in current:
        return (TrieResult.EXISTS, current)
    return (TrieResult.PREFIX, current)


def format_time(
    string: str, mapping: dict[str, str], trie: dict[str, Any] | None = None
) -> str | None:
    if not string:
        return None

    start = 0
    end = 1
    size = len(string)
    trie = trie or new_trie(mapping)
    current = trie
    chunks: list[str] = []
    sym: str | None = None

    while end <= size:
        chars = string[start:end]
        result, current = in_trie(current, chars[-1])

        if result == TrieResult.FAILED:
            if sym:
                end -= 1
                chars = sym
                sym = None
            else:
                chars = chars[0]
                end = start + 1

            start += len(chars)
            chunks.append(chars)
            current = trie
        elif result == TrieResult.EXISTS:
            sym = chars

        end += 1

        if result != TrieResult.FAILED and end > size:
            chunks.append(chars)

    return "".join(mapping.get(chars, chars) for chars in chunks)


_MILLISECOND_PRECISION = 3
_MICROSECOND_PRECISION = 6


def subsecond_precision(timestamp_literal: str) -> int:
    try:
        parsed = datetime.datetime.fromisoformat(timestamp_literal)
    except ValueError:
        return 0
    subsecond_digit_count = len(str(parsed.microsecond).rstrip("0"))
    if subsecond_digit_count > _MILLISECOND_PRECISION:
        return _MICROSECOND_PRECISION
    if subsecond_digit_count > 0:
        return _MILLISECOND_PRECISION
    return 0


def tsort(dag: dict[Any, set[Any]]) -> list[Any]:
    result: list[Any] = []
    for _node, deps in tuple(dag.items()):
        for dep in deps:
            if dep not in dag:
                dag[dep] = set()
    while dag:
        current = {node for node, deps in dag.items() if not deps}
        if not current:
            msg = "Cycle error"
            raise ValueError(msg)
        for node in current:
            dag.pop(node)
        for key in dag:
            dag[key] -= current
        result.extend(sorted(current))
    return result


def name_sequence(prefix: str):
    sequence = count()
    return lambda: f"{prefix}{next(sequence)}"


def merge_ranges(ranges: list[tuple[Any, Any]]) -> list[tuple[Any, Any]]:
    if not ranges:
        return []
    ranges = sorted(ranges)
    merged = [ranges[0]]
    for start, end in ranges[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


ANSI_UNDERLINE = "\033[4m"
ANSI_RESET = "\033[0m"
ERROR_MESSAGE_CONTEXT_DEFAULT = 100


def highlight_sql(
    sql: str,
    positions: list[tuple[int, int]],
    context_length: int = ERROR_MESSAGE_CONTEXT_DEFAULT,
) -> tuple[str, str, str, str]:
    if not positions:
        msg = "positions must contain at least one (start, end) tuple"
        raise ValueError(msg)

    start_context = ""
    end_context = ""
    first_highlight_start = 0
    formatted_parts: list[str] = []
    previous_part_end = 0
    sorted_positions = sorted(positions, key=operator.itemgetter(0))

    if sorted_positions[0][0] > 0:
        first_highlight_start = sorted_positions[0][0]
        start_context = sql[
            max(0, first_highlight_start - context_length) : first_highlight_start
        ]
        formatted_parts.append(start_context)
        previous_part_end = first_highlight_start

    for start, end in sorted_positions:
        highlight_start = max(start, previous_part_end)
        highlight_end = end + 1
        if highlight_start >= highlight_end:
            continue
        if highlight_start > previous_part_end:
            formatted_parts.append(sql[previous_part_end:highlight_start])
        formatted_parts.append(
            f"{ANSI_UNDERLINE}{sql[highlight_start:highlight_end]}{ANSI_RESET}"
        )
        previous_part_end = highlight_end

    if previous_part_end < len(sql):
        end_context = sql[previous_part_end : previous_part_end + context_length]
        formatted_parts.append(end_context)

    formatted_sql = "".join(formatted_parts)
    highlight = sql[first_highlight_start:previous_part_end]

    return formatted_sql, start_context, highlight, end_context
