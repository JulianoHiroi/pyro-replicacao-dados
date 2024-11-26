#importe a biblioteca do pyro5
from datetime import datetime
from Pyro5.api import *
import Pyro5.api
from threading import Thread
from pathlib import Path
import uuid

class Publicador:
    def __init__(self):
        self.daemon = Pyro5.api.Daemon()
        self.servidor_nomes = locate_ns()
        uri_objetoPyro = self.servidor_nomes.lookup("Lider-Epoca-1")
        self.lider = Pyro5.api.Proxy(uri_objetoPyro)
    
    def publicar(self):
        self.lider.publicar("Mensagem 1 para publicacao teste")
        print("Publicação realizada")


if __name__ == "__main__":
    publicador = Publicador()
    publicador.publicar()