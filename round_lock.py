"""
Lock de arquivo exclusivo para evitar rodadas de scraping simultâneas (manual + scheduler).
Compatível Windows e Unix, sem dependências extras.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import TracebackType
from typing import Optional

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl


class RoundLock:
    """Lock não bloqueante; mantenha o arquivo aberto enquanto a rodada executa."""

    def __init__(self, lock_path: Path):
        self.lock_path = Path(lock_path)
        self._fp: Optional[object] = None

    def acquire(self, *, blocking: bool = False) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = open(self.lock_path, "a+b")
        self._fp.seek(0, 2)
        if self._fp.tell() == 0:
            self._fp.write(b"\0")
            self._fp.flush()
        self._fp.seek(0)
        try:
            if sys.platform == "win32":
                mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
                msvcrt.locking(self._fp.fileno(), mode, 1)
            else:
                op = fcntl.LOCK_EX
                if not blocking:
                    op |= fcntl.LOCK_NB
                fcntl.flock(self._fp.fileno(), op)
        except OSError:
            self._fp.close()
            self._fp = None
            return False
        return True

    def release(self) -> None:
        if not self._fp:
            return
        try:
            if sys.platform == "win32":
                self._fp.seek(0)
                try:
                    msvcrt.locking(self._fp.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            else:
                try:
                    fcntl.flock(self._fp.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
        finally:
            self._fp.close()
            self._fp = None

    def __enter__(self) -> "RoundLock":
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        self.release()


def try_acquire_round_lock(lock_path: Path) -> Optional[RoundLock]:
    lock = RoundLock(lock_path)
    if lock.acquire(blocking=False):
        return lock
    return None
