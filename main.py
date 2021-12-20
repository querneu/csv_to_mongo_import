import time
import shutil
import pandas as pd
from pymongo import MongoClient
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from watchdog.utils import patterns


client = MongoClient(host="localhost", port=27017)
database = client['teste_import']
cadastros = database['cadastros']
telefones = database['telefones']


#TODO 
# IDENTIFICAR OS ARQUIVOS PELO NOME 
#     SEPARAR AS CONFIGS DE IMPORTAÇÃO DO MONGO
#     RODAR COMO SERVIÇO DO WINDOWS 
#     CONFIGURAR A MOVIMENTAÇÃO DE ARQUIVOS NA PASTA


path = "."
go_recursively = True
my_observer = Observer()


if __name__ == "__main__":
    patterns = ["*.txt"]
    ignore_patterns = None
    ignore_directories = True
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns,ignore_patterns, ignore_directories, case_sensitive)


def csv_to_json(filename):
    data = pd.read_csv(filename, sep=";")
    return data.to_dict('records')


def on_created(event):
    print(f"Arquivo {event.src_path} foi adicionado!")
    print(event)
    cadastros.insert_many(csv_to_json(event.src_path))   
    shutil.move(event.src_path, "C:\import\Importados")

def on_deleted(event):
    print(f"Arquivo {event.src_path} foi deletado!")

def on_modified(event):
    print(f"Arquivo {event.src_path} foi alterado!")

def on_moved(event):
    print(f"Arquivo {event.src_path} foi movido para {event.dest_path}")

my_event_handler.on_created = on_created
my_event_handler.on_deleted = on_deleted
my_event_handler.on_modified = on_modified
my_event_handler.on_moved = on_moved

my_observer.schedule(my_event_handler, path, recursive=go_recursively)

my_observer.start()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    my_observer.stop()
    my_observer.join()

