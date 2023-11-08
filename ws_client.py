import websockets
import asyncio


async def send(msg):
    url = "ws://127.0.0.1:7890"
    # Connect to the server
    async with websockets.connect(url) as ws:
        # Send a greeting message
        await ws.send(msg)


# The main function that will handle connection and communication
# with the server
async def listen(callback, loop):
    if loop:
        asyncio.set_event_loop(loop)

    url = "ws://127.0.0.1:7890"
    # Connect to the server
    async with websockets.connect(url) as ws:
        # Send a greeting message
        # await ws.send("Hello Server!")
        # Stay alive forever, listening to incoming msgs
        while True:
            msg = await ws.recv()
            # print(msg)
            callback(msg)


def start_listen(callback, loop):
    asyncio.run(listen(callback, loop))

# Start the connection

# asyncio.run(listen())
# asyncio.run(send("TEEssssssssssssssst"))

# asyncio.get_event_loop().run_until_complete(listen())
