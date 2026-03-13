import json
import logging
import select
import subprocess  # noqa: S404
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_sqlglot_logger = logging.getLogger("sqlglot")


class TSBridge:
    _instance: "TSBridge | None" = None

    def __init__(self) -> None:
        self.proc = subprocess.Popen(
            ["node", "tools/compat/ts_bridge.mjs"],  # noqa: S607
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=PROJECT_ROOT,
        )

    @classmethod
    def get(cls) -> "TSBridge":
        if cls._instance is None:
            cls._instance = TSBridge()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        if cls._instance is not None:
            cls._instance.close()
            cls._instance = None

    def call(self, method: str, timeout: float = 30.0, **kwargs: Any) -> dict:
        cmd = {"method": method, **kwargs}
        if self.proc.stdin is None:
            msg = "Bridge stdin is unavailable"
            raise RuntimeError(msg)
        if self.proc.stdout is None:
            msg = "Bridge stdout is unavailable"
            raise RuntimeError(msg)
        self.proc.stdin.write(json.dumps(cmd) + "\n")
        self.proc.stdin.flush()

        ready, _, _ = select.select([self.proc.stdout], [], [], timeout)
        if not ready:
            self.proc.kill()
            msg = f"Bridge call '{method}' timed out after {timeout}s"
            raise TimeoutError(msg)

        line = self.proc.stdout.readline()
        if not line:
            if self.proc.stderr is None:
                msg = "Bridge died without stderr output"
                raise RuntimeError(msg)
            stderr = self.proc.stderr.read()
            msg = f"Bridge died: {stderr}"
            raise RuntimeError(msg)
        return json.loads(line)

    def close(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        self.proc.wait()


def emit_bridge_logs(result: dict) -> None:
    for msg in result.get("logs", []):
        if msg.startswith("WARNING:sqlglot:"):
            _sqlglot_logger.warning(msg[len("WARNING:sqlglot:") :])
        else:
            _sqlglot_logger.info(msg)
