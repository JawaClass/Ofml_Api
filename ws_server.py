import threading

import websockets
import asyncio
from websockets.legacy.server import WebSocketServerProtocol

# Server data
PORT = 7890
print("Server listening on Port " + str(PORT))

# A set of connected ws clients
connected: set[WebSocketServerProtocol] = set()
_lock = threading.RLock()


def get_lock(lock_msg):
    print(lock_msg)
    return _lock


async def notify_clients(message):
    # print('notify_clients', message, connected)

    with get_lock(f'use lock [notify_clients] {id(_lock)}'):
        for conn in connected:
            try:
                await conn.send(message)
            except websockets.exceptions.ConnectionClosed as e:
                print("Shouldnt happen !!! A client connection not active anymore.")
            finally:
                print('Shouldnt happen !!! Remove client connection.')
                # connected.remove(conn)
    print('release lock [notify_clients]', id(_lock))


# The main behavior function for this server
async def echo(websocket: WebSocketServerProtocol, path, is_client=True):
    # print("A client just connected.", 'is_client=', websocket.is_client, 'side=', websocket.side, 'path=', path,
    #       'is_client::', is_client)
    # # Store a copy of the connected client
    # print('add ws', type(websocket))

    if is_client:
        with get_lock(f'use lock [echo] {id(_lock)}'):
            connected.add(websocket)
        print('release lock [echo]', id(_lock))

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
        with get_lock(f'use lock [echo] {id(_lock)}'):
            connected.remove(websocket)
        print('release lock [echo]', id(_lock))


# # Function to send a message to all connected clients
# async def send_message_to_clients():
#     while True:
#         return
#         await asyncio.sleep(5)  # Wait for 5 seconds
#         message = "This is a periodic message"
#         print('server says:', message)
#         for conn in connected:
#             await conn.send(message)
#         # return


def start(loop):
    asyncio.set_event_loop(loop)
    # Start the server and the periodic message sending task
    start_server = websockets.serve(echo, "localhost", PORT)
    # periodic_task = asyncio.ensure_future(send_message_to_clients())

    # print('start_server', type(start_server))
    # print('periodic_task', type(send_message_to_clients))
    # Run the event loop

    # loop.run_until_complete(start_server)

    print('..........')

    # asyncio.get_event_loop().run_forever()


def start_server_in_thread(loop):
    import threading

    t = threading.Thread(target=start, args=(loop,))
    t.start()
    return t


if __name__ == '__main__':
    print('main:::')
    start_server_in_thread()
    print('Done...')
    while 1:
        pass
