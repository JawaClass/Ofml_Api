import asyncio

from websockets import ConnectionClosedOK
from websockets.server import serve
from websockets.legacy.server import WebSocketServerProtocol
import json
from json.decoder import JSONDecodeError
import websockets

CONNECTIONS: set[WebSocketServerProtocol] = set()

SERVER: WebSocketServerProtocol = None

_event_format = {
    'who': None,
    'payload': None,
}


class ServerAlreadyDefinedException(Exception):
    pass


class MessageException(Exception):
    pass


def set_server(websocket: WebSocketServerProtocol):
    global SERVER
    if SERVER is not None:
        raise ServerAlreadyDefinedException("Server is supposed to be only defined once!")
    SERVER = websocket


def get_clients():
    return [_ for _ in CONNECTIONS if _ is not SERVER]


def parse_event(message):
    try:
        event: dict = json.loads(message)
    except JSONDecodeError:
        print('No valid JSON message', message)
        return None

    global _event_format
    if not _event_format.keys() == event.keys():
        print('JSON has wrong format', event)
        return None

    return event


async def handler(websocket: WebSocketServerProtocol):
    """
    Handle a connection and dispatch it according to who is connecting.
    1 connection is server
    other connections is clients
    server sends to clients
    """

    # new client/server connected!
    CONNECTIONS.add(websocket)
    print('handler. WS connected.', websocket, websocket.is_client)

    # Receive and parse the "init" event from the UI.
    while True:
        try:
            message = await websocket.recv()
        except ConnectionClosedOK:
            # no message anymore on this websocket
            CONNECTIONS.remove(websocket)
            continue

        event = parse_event(message)

        is_event_valid = event is not None

        if not is_event_valid:
            continue

        if event['who'] == 'server':
            if event['payload'] == 'init':
                set_server(websocket)
                continue
        else:
            raise MessageException('We only expect messages from Server')

        print(f"Server is forwarding message to all clients n={len(get_clients())}:", message)

        websockets.broadcast(
            get_clients(),
            json.dumps(event['payload'])
        )


async def main():
    async with serve(handler, "localhost", 8765):
        await asyncio.Future()  # run forever


asyncio.run(main())
