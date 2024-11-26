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
        self.lider_uri = None
        self.seguidores = []
        self.observadores = []
        self.publicacoes = []
        self.offset = 0
        diretorio_base = Path(__file__).parent
        (diretorio_base / "log").mkdir(parents=True, exist_ok=True)
        self.caminho_log = diretorio_base / "log" / "log.txt"
        self.caminho_log.write_text("")

    def iniciar(self):
        if self.estado == "Lider":
            self.servidor_nomes.register("Lider-Epoca-1", self.uri_objetoPyro)
            print("Líder Inicializado")
        elif self.estado in ["Seguidor", "Observador"]:
            self.lider_uri = self.servidor_nomes.lookup("Lider-Epoca-1")
            with Proxy(self.lider_uri) as lider_proxy:
                lider_proxy.registrar(self.id, self.estado, self.uri_objetoPyro)
                print(f"{self.estado} Inicializado")
        self.daemon.requestLoop()

    def inserirlog(self, mensagem):
        # Adiciona no início da mensagem o horário e dia do log
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        mensagem = f"{timestamp} - {mensagem}"
        with self.caminho_log.open("a", encoding='UTF-8') as arquivo:
            arquivo.write(mensagem + "\n")

    @expose
    def publicar(self, mensagem):
        publicacao = f"Publicação: {mensagem}"
        self.inserirlog(publicacao)
        self.publicacoes.append(publicacao)
        self.offset += 1
        for seguidor in self.seguidores:
            with Proxy(seguidor) as seguidor_proxy:
                seguidor_proxy.avisa_publicacao()

    @expose
    def registrar(self, id, estado, uri_objetoPyro):
        print("Registrando um " + estado)
        if estado == "Seguidor":
            self.seguidores.append(uri_objetoPyro)
            self.inserirlog("Seguidor registrado: " + id)
        elif estado == "Observador":
            self.observadores.append(uri_objetoPyro)
            self.inserirlog("Observador registrado: " + id)

    @expose 
    def replica_publicacoes(self, offset):
        # Retorna o range de publicações que o seguidor ainda não tem
        publicacoes = self.publicacoes[offset:]
        return publicacoes

    @expose
    def avisa_publicacao(self):
        print("Notificação: Publicação recebida")
        self.inserirlog("Notificação: Publicação recebida")
        with Proxy(self.lider_uri) as lider_proxy:
            publicacoes_faltantes = lider_proxy.replica_publicacoes(self.offset)
            for publicacao in publicacoes_faltantes:
                self.publicacoes.append(publicacao)
                self.inserirlog("Publicação replicada: " + publicacao)
                self.offset += 1


if __name__ == "__main__":
    # Altere o estado para "Lider", "Seguidor" ou "Observador" conforme necessário
    broker = Broker("Seguidor")
    broker.iniciar()
