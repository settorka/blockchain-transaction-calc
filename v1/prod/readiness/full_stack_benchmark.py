from __future__ import annotations

import pathlib
import subprocess
import sys


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
COMPOSE_DIR = REPO_ROOT / "prod" / "deploy"


def main() -> int:
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "orchestrator",
        "sh",
        "-lc",
        "exec sbt -Dsbt.color=false -batch 'runMain com.kofiska.solana.orchestrator.BenchmarkMain'",
    ]
    proc = subprocess.run(cmd, cwd=COMPOSE_DIR, text=True, capture_output=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
