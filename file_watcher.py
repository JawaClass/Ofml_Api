import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        print('on_modified', event)

        if event.is_directory:
            return
        print(f'File {event.src_path} has been modified.')

    def on_any_event(self, event):
        print('on_any', event)


folder_path = './test_files'
event_handler = MyHandler()

observer = Observer()
observer.schedule(event_handler, path=folder_path, recursive=False)
observer.start()

while 1:
    pass