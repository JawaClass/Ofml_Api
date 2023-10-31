import threading
import time
from functools import wraps

from flask import Flask, jsonify

GLOBAL_VALUE = None
_lock = threading.Lock()

app = Flask(__name__)


def read_data():
    with open('data.txt') as f:
        return ' '.join(f.readlines())


def is_updating():
    return _lock.locked()


def update_data():
    global GLOBAL_VALUE
    with _lock:
        print('update data...')
        time.sleep(5)
        GLOBAL_VALUE = read_data()


print('init ')


# Function to be executed in the worker thread
def background_task():
    while True:
        update_data()

        time.sleep(15)  # Simulating a task that takes 5 seconds to complete


# Start the worker thread when the Flask app starts
worker_thread = threading.Thread(target=background_task)
worker_thread.start()


# Custom decorator to check if the server is updating
def updating_check(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if is_updating():
            return jsonify(message='Server is updating...'), 503  # HTTP 503 Service Unavailable
        return f(*args, **kwargs)

    return decorated_function


@app.route('/')
@updating_check
def hello_world():
    return f'Server is running. Data = {GLOBAL_VALUE}'


if __name__ == '__main__':
    GLOBAL_VALUE = read_data()
    app.run()
