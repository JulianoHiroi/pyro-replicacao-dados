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
        self.num_seguidores = 0
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
            self.num_seguidores += 1
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
                print(self.log.consulta_publicacao_confirmada(self.epoca, self.log.consultar_offset(self.epoca)))
                print("Publicação realizada com sucesso.")
                
                
               
        
        

    @expose 
    def replica_publicacoes(self, offset , epoca):
        publicacoes = self.log.consultar_publicacoes(epoca, offset)
        if(epoca != self.epoca):
            return True, [], self.epoca , self.log.consultar_offset(epoca)
        return False, publicacoes, self.epoca, self.log.consultar_offset(epoca)
    
    @expose 
    def confirmar_recebimento(self, epoca, offset):
        # Incrementa a quantidade de recebimento de publicações
        
        # Garante que a coluna da época exista no DataFrame
        if epoca not in self.qtd_confirmados.columns:
            self.qtd_confirmados[epoca] = pd.Series(dtype='int')

        # Garante que o offset exista no índice do DataFrame
        if offset >= len(self.qtd_confirmados[epoca]):
            for _ in range(len(self.qtd_confirmados[epoca]), offset + 1):
                self.qtd_confirmados = pd.concat(
                    [self.qtd_confirmados, pd.DataFrame({epoca: [0]})],
                    ignore_index=True
                )

        # Incrementa o contador de confirmações para a publicação específica
        self.qtd_confirmados.at[offset, epoca] += 1
        
        # Verifica se alcançou a maioria simples
        maioria_simples = int(self.num_seguidores / 2 + 1) 
        if self.qtd_confirmados.at[offset, epoca] >= maioria_simples:
            print("Confirmou a publicação")
            self.log.confirmar_publicacao(epoca, offset)
    
     ## Votante ##
    @expose
    @oneway
    def avisa_publicacao(self):
        with Proxy(self.lider_uri) as lider_proxy:
            erro , publicoes_faltantes, epoca, offset  = lider_proxy.replica_publicacoes(self.log.consultar_offset(self.epoca) , self.epoca)
            if erro == True: 
                self.corrige_publicacoes(epoca, offset, lider_proxy)
            if erro == False:
                for publicacao in publicoes_faltantes:
                    self.log.inserir_log(publicacao, epoca)
                    lider_proxy.confirmar_recebimento(self.epoca, offset)
            print("Notificação: Publicação recebida")
            print (self.epoca, self.log.consultar_offset(self.epoca))
            return {"epoca": self.epoca,"offset": self.log.consultar_offset(self.epoca)} 
              
            
        

        
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
        # uma matriz que servirá para indicar se um publicao foi confirmado ou não
        self.matriz_publicacoes_confirmadas = pd.DataFrame()  # Inicializa o DataFrame vazio

    
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
            return []  # Retorna uma lista vazia se a coluna não existires
        return self.matriz_publicacoes[epoca].dropna().iloc[offset:].tolist()
    
    def confirmar_publicacao(self, epoca, offset):
        # Garante que a coluna correspondente à época exista na matriz de publicações confirmadas
        if epoca not in self.matriz_publicacoes_confirmadas.columns:
            self.matriz_publicacoes_confirmadas[epoca] = pd.Series(dtype='int')
        
        # Garante que o índice (offset) exista no DataFrame
        while offset >= len(self.matriz_publicacoes_confirmadas):
            self.matriz_publicacoes_confirmadas = pd.concat(
                [self.matriz_publicacoes_confirmadas, 
                pd.DataFrame({epoca: [0]})], 
                ignore_index=True
            )
    
        # Atualiza o valor para 1 na célula correspondente à época e ao offset
        self.matriz_publicacoes_confirmadas.at[offset, epoca] = 1

    def consulta_publicacao_confirmada(self, epoca, offset):
        print ("Estou aq")
        
        if epoca not in self.matriz_publicacoes_confirmadas.columns:
            return 0
        return self.matriz_publicacoes_confirmadas[offset, epoca]



if __name__ == "__main__":
    # Altere o estado para "Lider", "Seguidor" ou "Observador" conforme necessário
    broker = Broker("Seguidor", "Lider-Epoca-1")
    broker.iniciar()
