from typing import Callable, Any, Optional
import asyncio
import websockets
from websockets.asyncio.server import serve, ServerConnection

class WSserver:
    def __int__(
            self, 
            host: str, 
            port:int = 1000,
            handler: Optional[Callable[Any]] = None
        ):
        self.host: str = host
        self.port: int = port
        self._server: websockets.asyncio.server.Server | None = None
        self.handler: Callable[Any] | None = handler
        
    @property
    def url(self) -> Optional[str]:
        if isinstance(self._server, websockets.asyncio.server.Server):
            port = self._server.sockets[0].getsockname()[1]
            return f"ws://{self.host}:{port}"
        else:
            return None
    
    async def start(self): 
        self._server = await serve(self.handler, self.host, self.port)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
        
 # =============================================================================
#     - socket() — create the socket
# - bind() — claim an address/port
# - listen() — mark it passive, set the backlog queue size
# - accept() — blocks, returns a new connected socket each time a client connects
# - recv() / send() — raw byte I/O on that connected socket
# - close()
# 
# Layer 2 — the HTTP handshake (one-time, text-based)
# 
# A WebSocket connection starts as a plain HTTP request. The server has to:
# 1. recv() the HTTP request, parse headers.
# 2. Check for Upgrade: websocket, Connection: Upgrade, and read Sec-WebSocket-Key.
# 3. Compute Sec-WebSocket-Accept = base64(SHA1(key + a fixed magic GUID string from the RFC)).
# 4. send() back HTTP/1.1 101 Switching Protocols with that header.
# =============================================================================
# =============================================================================
# frames over the same TCP socket.
# 
# Layer 3 — WebSocket framing (the actual protocol)
# 
# This is the part that's genuinely WS-specific. Every message is wrapped in a frame:
# 
# FIN(1 bit) + opcode(4 bits) | MASK(1 bit) + payload-length(7/16/64 bits) | [masking key, 4 bytes] | payload
# 
# - opcode tells you what kind of frame it is: 0x1 text, 0x2 binary, 0x0 continuation (for fragmented messages), 0x8 close, 0x9 ping, 0xA pong.
# - masking: client→server frames must be masked (XOR'd with a random 4-byte key sent in the frame) — this is an RFC 6455 anti-cache-poisoning requirement. Server→client frames are not masked.
# - fragmentation: a logical message can be split across several frames (FIN=0 until the last one) — you need to reassemble.
# 
# =============================================================================
