"""Accelerate a single owner's waits without changing the real event loop."""

import asyncio
from types import SimpleNamespace


class OwnerAsyncio:
    def __init__(self, *, advance_clock=False):
        self.elapsed = 0.0
        self.advance_clock = advance_clock

    def __getattr__(self, name):
        return getattr(asyncio, name)

    def get_running_loop(self):
        loop = asyncio.get_running_loop()
        if self.advance_clock:
            return SimpleNamespace(time=lambda: loop.time() + self.elapsed)
        return loop

    async def sleep(self, delay):
        if self.advance_clock:
            self.elapsed += delay
            await asyncio.sleep(0)
        else:
            # Keep actual elapsed-time deadlines, but observe modeled external
            # state promptly. Other modules retain their real asyncio module.
            await asyncio.sleep(min(delay, 0.01))


def accelerate_owner(monkeypatch, module, *, advance_clock=False):
    clock = OwnerAsyncio(advance_clock=advance_clock)
    monkeypatch.setattr(module, "asyncio", clock)
    return clock
