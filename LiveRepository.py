from pathlib import Path

from watchdog.events import FileSystemEventHandler, RegexMatchingEventHandler
from watchdog.observers import Observer

from repository import Repository


class LiveRepository(Repository, RegexMatchingEventHandler):

    def __init__(self, root: Path, **kwargs):
        super().__init__(root, **kwargs)

        self._observer = Observer()
        self._observer.schedule(self, path=self.root.__str__(), recursive=True)
        self._observer.start()

    def on_any_event(self, event):
        print('LiveRepository :: on_any', event)


live_repo = LiveRepository(root=...)
