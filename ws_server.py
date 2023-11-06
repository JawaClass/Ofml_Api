import websockets
import asyncio
from websockets.legacy.server import WebSocketServerProtocol

# Server data
PORT = 7890
print("Server listening on Port " + str(PORT))

# A set of connected ws clients
connected: set[WebSocketServerProtocol] = set()


# The main behavior function for this server
async def echo(websocket, path):
    print("A client just connected")
    # Store a copy of the connected client
    print('add ws', type(websocket))
    connected.add(websocket)
    # Handle incoming messages
    try:
        async for message in websocket:
            print("Received message from client: " + message)
            # Send a response to all connected clients except sender
            for conn in connected:
                if conn != websocket:
                    await conn.send("Someone said: " + message)
    # Handle disconnecting clients
    except websockets.exceptions.ConnectionClosed as e:
        print("A client just disconnected")
    finally:
        connected.remove(websocket)


# Function to send a message to all connected clients
async def send_message_to_clients():
    while True:
        await asyncio.sleep(5)  # Wait for 5 seconds
        message = "This is a periodic message"
        for conn in connected:
            await conn.send(message)


# Start the server and the periodic message sending task
start_server = websockets.serve(echo, "localhost", PORT)
periodic_task = asyncio.ensure_future(send_message_to_clients())

# Run the event loop
asyncio.get_event_loop().run_until_complete(asyncio.gather(start_server, periodic_task))
asyncio.get_event_loop().run_forever()
