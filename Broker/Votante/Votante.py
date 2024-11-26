#importe a biblioteca do pyro5
from datetime import datetime
from Pyro5.api import *
import Pyro5.api
from threading import Thread
from pathlib import Path
import uuid

class Broker:
    def __init__(self, estado):
        self.daemon = Daemon()
        self.estado = estado
        self.id = str(uuid.uuid4())
        self.servidor_nomes = locate_ns()
        self.uri_objetoPyro = self.daemon.register(self)
        self.lider = None
        self.seguidores = []
        self.observadores = []
        diretorio_base = Path(__file__).parent
        (diretorio_base / "log").mkdir(parents=True, exist_ok=True)
        self.caminho_log = diretorio_base / "log" / "log.txt"

      

    def iniciar(self):
        if(self.estado == "Lider"):
            self.servidor_nomes.register("Lider-Epoca-1", self.uri_objetoPyro)
            print("Lider Inicializado")
            self.daemon.requestLoop()
        elif(self.estado == "Seguidor"):
            uri_objetoPyro = self.servidor_nomes.lookup("Lider-Epoca-1")
            self.lider = Pyro5.api.Proxy(uri_objetoPyro)
            self.lider.registrar(self.id ,self.estado, self.uri_objetoPyro)
            print("Seguidor Inicializado")
        elif(self.estado == "Observador"):
            uri_objetoPyro = self.servidor_nomes.lookup("Lider-Epoca-1")
            self.lider = Pyro5.api.Proxy(uri_objetoPyro)
            self.lider.registrar(self.id ,self.estado, self.uri_objetoPyro)
            print("Observador Inicializado")

    # @expose
    # def registrar(self, id ,estado , uri_objetoPyro):
    #     print("Registrando um" + estado)
    #     if(estado == "Seguidor"):
    #         self.seguidores.append(uri_objetoPyro)
    #         # coloca no log o horario que o seguidor foi registrado
    #         self.inserirlog("Seguidor registrado: " + id)
    #     elif(estado == "Observador"):
    #         self.observadores.append(uri_objetoPyro)
    #         self.inserirlog("Seguidor registrado: " + id)
            
        
        

    # def inserirlog(self, mensagem):
    #     # Adiciona no inicio da mensagem o horario e dia do log
    #     timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    #     mensagem = f"{timestamp} - {mensagem}"
    #     with self.caminho_log.open("a") as arquivo:
    #         arquivo.write(mensagem + "\n")

    # @expose
    # def publicar(self):
    #     self.inserirlog("Requisição de publicação recebida")
   
    # @expose
    # def somar(self, a, b):
    #     return a + b

if __name__ == "__main__":
    lider = Broker("Seguidor")
    lider.iniciar()