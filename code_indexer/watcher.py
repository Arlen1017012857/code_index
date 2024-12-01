"""File system watcher for code indexing.

This module provides real-time file system monitoring and triggers index updates
when files are modified.
"""

import os
import time
from typing import Set, Optional
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import signal
import threading
import atexit

from .hybrid_search import HybridCodeSearch
from .constants import EXTENSION_TO_TREE_SITTER_LANGUAGE


class CodeIndexEventHandler(FileSystemEventHandler):
    """处理文件系统事件并更新代码索引。"""

    def __init__(self, searcher: HybridCodeSearch):
        self.searcher = searcher
        self.pending_updates: Set[str] = set()
        self._debounce_timer: Optional[float] = None
        self._debounce_delay = 1.0  # 1秒的防抖延迟

    def on_created(self, event):
        if event.is_directory:
            return
        if self._should_process_file(event.src_path):
            self._schedule_update(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            return
        if self._should_process_file(event.src_path):
            self._schedule_update(event.src_path)

    def on_deleted(self, event):
        if event.is_directory:
            return
        # TODO: 实现文件删除的处理
        pass

    def on_moved(self, event):
        if event.is_directory:
            return
        # 处理文件重命名/移动
        if self._should_process_file(event.dest_path):
            self._schedule_update(event.dest_path)

    def _should_process_file(self, file_path: str) -> bool:
        """检查是否应该处理该文件。"""
        # 检查文件扩展名
        ext = os.path.splitext(file_path)[1]
        return ext in EXTENSION_TO_TREE_SITTER_LANGUAGE

    def _schedule_update(self, file_path: str):
        """计划更新文件的索引。"""
        self.pending_updates.add(file_path)
        current_time = time.time()

        # 如果这是第一次更新或者距离上次更新已经超过防抖延迟
        if self._debounce_timer is None or (current_time - self._debounce_timer) >= self._debounce_delay:
            self._process_updates()
            self._debounce_timer = current_time

    def _process_updates(self):
        """处理所有待处理的更新。"""
        for file_path in self.pending_updates:
            try:
                self.searcher.update_index(file_path)
            except Exception as e:
                print(f"Error updating index for {file_path}: {str(e)}")
        self.pending_updates.clear()


class CodeIndexWatcher:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, root_path: str):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self, root_path: str):
        if not hasattr(self, '_initialized'):
            self.root_path = root_path
            self.searcher = HybridCodeSearch(root_path)
            self.event_handler = CodeIndexEventHandler(self.searcher)
            self.observer = Observer()
            self._initialized = True
            self._running = False
            self._setup_signal_handlers()
            atexit.register(self.stop)

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            print(f"\nReceived signal {signum}. Stopping watcher gracefully...")
            self.stop()
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def start(self):
        """开始监控文件系统变化。"""
        # 首先索引所有现有文件
        print("Indexing existing files...")
        self.searcher.index_files()
        print("Initial indexing completed.")

        # 开始监控变化
        self.observer.schedule(self.event_handler, self.root_path, recursive=True)
        self.observer.start()
        print(f"Started watching {self.root_path} for changes.")
        self._running = True

    def stop(self):
        """停止文件系统监控。"""
        self.observer.stop()
        self.observer.join()
        print("Stopped watching for changes.")
        self._running = False

    def search(self, query: str, limit: int = 10):
        """执行代码搜索。"""
        return self.searcher.search(query, limit)

    def process_pending_updates(self):
        """处理任何待处理的更新。"""
        self.event_handler._process_updates()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
