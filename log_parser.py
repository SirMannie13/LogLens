import json
import re
from dataclasses import dataclass
from typing import Optional

# Common log patterns:
# 1) 2026-03-04 12:34:56,789 INFO Something happened
# 2) 2026-03-04T12:34:56Z - ERROR - module: msg
# 3) [2026-03-04 12:34:56] [WARN] [module] msg
ISO_LINE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}"
    r"(?:[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{3,6})?)?"
    r"(?:Z|[+-]\d{2}:\d{2})?)"
    r"\s*(?:[-|]\s*)?"
    r"(?P<level>[A-Z]{3,10})"
    r"\s*(?:[-|]\s*)?"
    r"(?P<rest>.*)$"
)

BRACKET_LINE = re.compile(
    r"^\[(?P<ts>[^\]]+)\]\s*\[(?P<level>[A-Z]{3,10})\]\s*(?:\[(?P<src>[^\]]+)\]\s*)?(?P<msg>.*)$"
)

SRC_PREFIX = re.compile(r"^(?P<src>[\w.\-/#]+)\s*[:\-]\s*(?P<msg>.*)$")


@dataclass
class ParsedLog:
    ts: Optional[str]
    level: Optional[str]
    source: Optional[str]
    message: str
    raw: str


def _norm_level(level: Optional[str]) -> Optional[str]:
    if not level:
        return None
    up = level.strip().upper()
    # normalize common variants
    if up == "WARN":
        return "WARNING"
    return up


def parse_line(line: str) -> ParsedLog:
    raw = line.rstrip("\n")
    s = raw.strip()

    if not s:
        return ParsedLog(ts=None, level=None, source=None, message="", raw=raw)

    # JSON lines: {"timestamp": "...", "level": "...", "message": "...", "logger": "..."}
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
            ts = obj.get("timestamp") or obj.get("time") or obj.get("@timestamp")
            level = _norm_level(obj.get("level") or obj.get("severity"))
            source = obj.get("logger") or obj.get("source") or obj.get("module")
            message = obj.get("message") or obj.get("msg") or s
            return ParsedLog(ts=ts, level=level, source=source, message=str(message), raw=raw)
        except Exception:
            pass

    m = BRACKET_LINE.match(s)
    if m:
        ts = m.group("ts")
        level = _norm_level(m.group("level"))
        source = m.group("src")
        message = m.group("msg") or ""
        # Try to split source: message
        sm = SRC_PREFIX.match(message)
        if sm and not source:
            source = sm.group("src")
            message = sm.group("msg")
        return ParsedLog(ts=ts, level=level, source=source, message=message, raw=raw)

    m = ISO_LINE.match(s)
    if m:
        ts = m.group("ts")
        level = _norm_level(m.group("level"))
        rest = m.group("rest") or ""
        source = None
        message = rest

        sm = SRC_PREFIX.match(rest)
        if sm:
            source = sm.group("src")
            message = sm.group("msg")

        return ParsedLog(ts=ts, level=level, source=source, message=message, raw=raw)

    # fallback: no parse
    return ParsedLog(ts=None, level=None, source=None, message=s, raw=raw)