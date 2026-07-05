"""
relay_server.py
----------------
A minimal "blind" relay server. It matches two clients together in a
"room" and forwards raw messages between them -- nothing more.

Critically, it NEVER sees:
  - the shared 10-digit code (clients only send a one-way hash of it)
  - any plaintext (clients only send already-encrypted ciphertext)
  - the encryption key (that never leaves the clients)

So even a fully compromised or malicious server operator only ever
sees: two anonymous connections exchanging random-looking blobs. That's
the whole point -- you don't have to trust this server with anything.

Requirements:
    pip install websockets

Run:
    python relay_server.py                # listens on 0.0.0.0:8765
    python relay_server.py --port 9000

To make it reachable over the internet for free, deploy this file to a
free tier on Render, Fly.io, or Railway (any of them can run a simple
Python websockets service for free at small scale).
"""

import argparse
import asyncio

import websockets

# room_id (str) -> list of connected websocket clients (max 2)
rooms: dict[str, list] = {}


async def handler(ws):
    room_id = None
    try:
        # first message from any client MUST be "JOIN:<room_id>"
        first_msg = await ws.recv()
        if not first_msg.startswith("JOIN:"):
            await ws.close(reason="expected JOIN first")
            return
        room_id = first_msg[len("JOIN:"):]

        peers = rooms.setdefault(room_id, [])
        if len(peers) >= 2:
            await ws.send("ERROR:room full")
            await ws.close(reason="room full")
            return

        role = "INITIATOR" if len(peers) == 0 else "PEER"
        peers.append(ws)
        await ws.send(f"ROLE:{role}")
        print(f"[+] client joined room {room_id[:8]}... as {role} "
              f"({len(peers)}/2 connected)")

        async for message in ws:
            # blind forward: whatever one peer sends, the other peer receives.
            # the server does not parse, log, or store message contents.
            for peer in peers:
                if peer is not ws:
                    await peer.send(message)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if room_id and room_id in rooms:
            rooms[room_id] = [p for p in rooms[room_id] if p is not ws]
            if not rooms[room_id]:
                del rooms[room_id]
            print(f"[-] client left room {room_id[:8]}...")


async def main():
    parser = argparse.ArgumentParser(description="Blind relay server for the secure chat app")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    async with websockets.serve(handler, args.host, args.port):
        print(f"Relay server listening on {args.host}:{args.port}")
        print("This server only ever sees ciphertext -- it has no way to read your messages.")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
