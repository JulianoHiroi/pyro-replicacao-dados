from datetime import datetime
from Pyro5.api import *
import Pyro5.api
from threading import Thread
from pathlib import Path
import uuid
# importa pandas para manipulação de dados
import pandas as pd
class Broker:

    def __init__(self, estado , epoca):
        # Pyro
        self.daemon = Daemon()
        self.id = str(uuid.uuid4())
        self.servidor_nomes = locate_ns()
        self.uri_objetoPyro = self.daemon.register(self)

        # Broker
        self.estado = estado
        self.epoca = epoca
        self.lider_uri = None
        self.qtd_confirmados = pd.DataFrame()
        self.seguidores = []
        self.observadores = []
        self.log = Log()

    def iniciar(self):

        if self.estado == "Lider":
            self.servidor_nomes.register(self.epoca, self.uri_objetoPyro)
            print("Líder Inicializado")

        elif self.estado in ["Seguidor", "Observador"]:
            self.lider_uri = self.servidor_nomes.lookup(self.epoca)
            with Proxy(self.lider_uri) as lider_proxy:
                self.epoca = lider_proxy.registrar(self.id, self.estado, self.uri_objetoPyro)
                print(f"{self.estado} Inicializado")

        self.daemon.requestLoop()

    ## Lider ##
    @expose
    def registrar(self, id, estado, uri_objetoPyro):
        print("Registrando um " + estado)
        if estado == "Seguidor":
            self.seguidores.append({"uri": uri_objetoPyro , "id": id})
        elif estado == "Observador":
            self.observadores.append({"uri": uri_objetoPyro , "id": id})
        return self.epoca

    @expose
    def publicar(self, mensagem):
        publicacao = f"publicação: {mensagem}"
        self.log.inserir_log(publicacao, self.epoca)
        if self.epoca not in self.qtd_confirmados.columns:
            self.qtd_confirmados[self.epoca] = pd.Series(dtype='int')
        self.qtd_confirmados.at[len(self.qtd_confirmados), self.epoca] = 0
        for seguidor in self.seguidores:
            with Proxy(seguidor["uri"]) as seguidor_proxy:
                seguidor_proxy.avisa_publicacao()

    @expose 
    def replica_publicacoes(self, offset , epoca):
        publicacoes = self.log.consultar_publicacoes(epoca, offset)
        if(epoca != self.epoca):
            return True, [], self.epoca , self.log.consultar_offset(epoca)
        return False, publicacoes, self.epoca, self.log.consultar_offset(epoca)
    
    @expose 
    def confirmar_recebimento(self, epoca , offset_antigo):
        # incrementa no array de votos a quantidade de votos recebidos de acordo com a epoca e o offset antigo
        # incrementa até o offset atual
        for i in range(offset_antigo + 1, self.log.consultar_offset(epoca)):
            self.qtd_confirmados[epoca][offset_antigo + i] += 1
    
     ## Votante ##
    @expose
    @oneway
    def avisa_publicacao(self):
        with Proxy(self.lider_uri) as lider_proxy:
            offset_antigo = self.log.consultar_offset(self.epoca)
            erro , publicoes_faltantes, epoca, offset  = lider_proxy.replica_publicacoes(self.log.consultar_offset(self.epoca) , self.epoca)
            if erro == True: 
                self.corrige_publicacoes(epoca, offset, lider_proxy)
            if erro == False:
                for publicacao in publicoes_faltantes:
                    self.log.inserir_log(publicacao, epoca)
                    lider_proxy.confirmar_recebimento(self.epoca, offset_antigo)
            print("Notificação: Publicação recebida")
            
            
        

        
    def  corrige_publicacoes(self, epoca, offset, lider_proxy):
        # trunca o log da sua epoca 
        offset_antigo = self.log.consultar_offset(self.epoca)
        if(offset < offset_antigo):
            self.log.matriz_publicacoes[self.epoca] = self.log.matriz_publicacoes[self.epoca][:offset]
        
        self.epoca = epoca
        erro , publicoes_faltantes, epoca, offset  = lider_proxy.replica_publicacoes(self.log.consultar_offset(self.epoca) , self.epoca)
        if erro == False:
            for publicacao in publicoes_faltantes:
                self.log.inserir_log(publicacao, epoca)
        else:
            self.corrige_publicacoes(epoca, offset, lider_proxy)
        return 
                
    
    
   

    



class Log:
    def __init__(self):
        diretorio_base = Path(__file__).parent
        (diretorio_base / "log").mkdir(parents=True, exist_ok=True)
        self.caminho_log = diretorio_base / "log" / "log.txt"
        self.caminho_log.write_text("")  # Limpa o arquivo de log ao iniciar
        self.matriz_publicacoes = pd.DataFrame()  # Inicializa o DataFrame vazio
    
    def inserir_log(self, publicacao, epoca):
        # Verifica se a coluna para a época existe, se não, cria uma coluna vazia
        if epoca not in self.matriz_publicacoes.columns:
            self.matriz_publicacoes[epoca] = pd.Series(dtype='object')

        # Adiciona a publicação à lista da coluna correspondente
        self.matriz_publicacoes.at[len(self.matriz_publicacoes), epoca] = publicacao
        
        # Adiciona no início da mensagem o horário e dia do log
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        mensagem = f"{epoca} - {timestamp} - {publicacao}"
        with self.caminho_log.open("a", encoding='UTF-8') as arquivo:
            arquivo.write(mensagem + "\n")

    def consultar_offset(self, epoca):
        if epoca not in self.matriz_publicacoes.columns:
            return 0  # Retorna 0 se a coluna não existir
        return len(self.matriz_publicacoes[epoca].dropna())

    def consultar_publicacoes(self, epoca, offset):
        if epoca not in self.matriz_publicacoes.columns:
            return []  # Retorna uma lista vazia se a coluna não existir
        return self.matriz_publicacoes[epoca].dropna().iloc[offset:].tolist()



if __name__ == "__main__":
    # Altere o estado para "Lider", "Seguidor" ou "Observador" conforme necessário
    broker = Broker("Seguidor", "Lider-Epoca-1")
    broker.iniciar()
