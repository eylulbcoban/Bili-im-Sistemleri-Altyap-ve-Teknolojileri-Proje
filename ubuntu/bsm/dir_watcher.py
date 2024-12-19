import os
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

# İzlenecek dizin ve log dosyasının yolu
WATCH_DIRECTORY = "/home/ubuntu/bsm/test"
LOG_FILE_TEMPLATE = "/home/ubuntu/bsm/logs/changes_{}.json"
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5MB

class ChangeHandler(FileSystemEventHandler):
    def __init__(self):
        self.log_file_path = self.get_next_log_file()
        # Log dosyasının bulunduğu klasörü oluştur
        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        # Log dosyasını başlat (eğer yoksa)
        if not os.path.exists(self.log_file_path):
            with open(self.log_file_path, "w") as f:
                f.write("")

    def get_next_log_file(self):
        """Mevcut log dosyasını kontrol eder, boyutu aşarsa yeni bir dosya oluşturur."""
        file_index = 1
        while True:
            log_file = LOG_FILE_TEMPLATE.format(file_index)
            if not os.path.exists(log_file) or os.path.getsize(log_file) < MAX_LOG_SIZE:
                return log_file
            file_index += 1

    def process_event(self, event):
        """Olayları log dosyasına yazan ana fonksiyon."""
        change = {
            "event_type": event.event_type,
            "file_path": event.src_path,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.check_log_file_size()
        with open(self.log_file_path, "a") as log_file:
            json.dump(change, log_file)
            log_file.write("\n")

    def check_log_file_size(self):
        """Log dosyasının boyutunu kontrol eder ve yeni bir dosya açar."""
        if os.path.exists(self.log_file_path) and os.path.getsize(self.log_file_path) >= MAX_LOG_SIZE:
            self.log_file_path = self.get_next_log_file()

    def on_created(self, event):
        self.process_event(event)

    def on_deleted(self, event):
        self.process_event(event)

    def on_modified(self, event):
        self.process_event(event)

    def on_moved(self, event):
        self.process_event(event)

if __name__ == "__main__":
    # Log dizininin mevcut olup olmadığını kontrol et
    os.makedirs(os.path.dirname(LOG_FILE_TEMPLATE.format(1)), exist_ok=True)

    # İzleme başlatma
    event_handler = ChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path=WATCH_DIRECTORY, recursive=True)
    observer.start()
    print(f"Watching directory: {WATCH_DIRECTORY}")

    try:
        while True:
            pass  # Sonsuz döngü
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
