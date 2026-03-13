from typing import Any
from typing import ClassVar

from compat.bridge import TSBridge
from compat.errors import TokenError


class TokenTypeEnum:
    _members: ClassVar[dict[str, "TokenTypeEnum"]] = {}

    def __init__(self, name: str):
        self.name = name
        self.value = name

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TokenTypeEnum):
            return self.name == other.name
        if isinstance(other, str):
            return self.name == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"TokenType.{self.name}"

    def __str__(self) -> str:
        return self.name

    @classmethod
    def get_or_create(cls, name: str) -> "TokenTypeEnum":
        if name not in cls._members:
            member = cls(name)
            cls._members[name] = member
        return cls._members[name]


class TokenTypeMeta(type):
    def __getattr__(cls, name: str) -> TokenTypeEnum:
        if name.startswith("_"):
            raise AttributeError(name)
        return TokenTypeEnum.get_or_create(name)


class TokenType(metaclass=TokenTypeMeta):
    pass


class Token:
    def __init__(self, data: dict[str, Any]):
        self._data = data

    @property
    def token_type(self) -> TokenTypeEnum:
        return TokenTypeEnum.get_or_create(self._data["tokenType"])

    @property
    def text(self) -> str:
        return self._data["text"]

    @property
    def line(self) -> int:
        return self._data["line"]

    @property
    def col(self) -> int:
        return self._data["col"]

    @property
    def start(self) -> int:
        return self._data["start"]

    @property
    def end(self) -> int:
        return self._data["end"]

    @property
    def comments(self) -> list[str]:
        return self._data.get("comments", [])

    def __repr__(self) -> str:
        d = self._data
        tt = self.token_type.name
        return (
            f"<Token token_type: TokenType.{tt},"
            + f" text: {d['text']},"
            + f" line: {d['line']}, col: {d['col']},"
            + f" start: {d['start']}, end: {d['end']},"
            + f" comments: {d.get('comments', [])!r}>"
        )


class Tokenizer:
    def __init__(self, dialect: str | None = None):
        self._dialect = dialect

    def tokenize(self, sql: str) -> list[Token]:
        bridge = TSBridge.get()
        kwargs: dict[str, Any] = {"sql": sql}
        if self._dialect:
            kwargs["dialect"] = self._dialect
        result = bridge.call("tokenize", **kwargs)
        if not result["ok"]:
            msg = result.get("error", "Tokenize failed")
            raise TokenError(msg)
        return [Token(t) for t in result["tokens"] if t["tokenType"] != "EOF"]
