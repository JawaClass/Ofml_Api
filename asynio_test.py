import asyncio

#
# async def greet_every_two_seconds():
#     while True:
#         print('Hello World')
#         await asyncio.sleep(2)
#
# def loop_in_thread(loop):
#     asyncio.set_event_loop(loop)
#     loop.run_until_complete(greet_every_two_seconds())
#
#
# loop = asyncio.get_event_loop()
# import threading
# t = threading.Thread(target=loop_in_thread, args=(loop,))
# t.start()
# print('end')
#

import threading

from ws_client import start_listen


def on_callback(msg):
    print('___on_callback', msg)

t = threading.Thread(target=start_listen, args=(on_callback, asyncio.get_event_loop(),))
t.start()
while 1:
    pass