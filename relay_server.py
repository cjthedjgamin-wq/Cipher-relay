import os
import asyncio
import websockets

rooms = {}

async def handler(ws):
    room_id = None
    try:
        first_msg = await ws.recv()

        if not first_msg.startswith("JOIN:"):
            await ws.close()
            return

        room_id = first_msg[5:]
        peers = rooms.setdefault(room_id, [])

        if len(peers) >= 2:
            await ws.send("ERROR:room full")
            await ws.close()
            return

        role = "INITIATOR" if len(peers) == 0 else "PEER"
        peers.append(ws)

        await ws.send(f"ROLE:{role}")

        async for message in ws:
            for peer in peers:
                if peer != ws:
                    await peer.send(message)

    except websockets.exceptions.ConnectionClosed:
        pass

    finally:
        if room_id and room_id in rooms:
            rooms[room_id] = [p for p in rooms[room_id] if p != ws]
            if not rooms[room_id]:
                del rooms[room_id]


async def main():
    port = int(os.environ.get("PORT", 8765))

    async with websockets.serve(handler, "0.0.0.0", port):
        print("Relay running on", port)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
