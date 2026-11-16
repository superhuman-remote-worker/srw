"""Debug Cockpit MCP Server.

Exposes cockpit API endpoints to LLMs via the Model Context Protocol.
"""

import os

__version__ = "0.1.0"

# FastMCP's OAuth proxy keeps its state in files written through aiofile,
# whose caio uses io_uring when the kernel offers it. A container's seccomp
# profile may refuse io_uring and locked memory is small, and either way a
# registration or a token answer fails with a 500. A few small files need no
# io_uring: a thread pool does. caio reads this once, on import, and the
# package is imported before the OAuth bridge.
os.environ.setdefault("CAIO_IMPL", "thread")
