from datetime import datetime
import time
import schedule
from Pyro5.api import *
import Pyro5.api
from threading import Thread
from pathlib import Path
import uuid
import pandas as pd
NUM_MINIMO_SEGUIDORES = 2

class Broker:

    def __init__(self, estado , epoca, pasta_log):
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
        self.log = Log(pasta_log)

    def iniciar(self):

        if self.estado == "Lider":
            self.servidor_nomes.register(self.epoca, self.uri_objetoPyro)
            self.iniciar_verificador_keep_alive()
            print("Líder Inicializado")

        elif self.estado in ["Seguidor", "Observador"]:
            
            self.lider_uri = self.servidor_nomes.lookup(self.epoca)
            
            with Proxy(self.lider_uri) as lider_proxy:
                self.epoca = lider_proxy.registrar(self.id, self.estado, self.uri_objetoPyro)
                print(f"{self.estado} Inicializado")
                self.epoca = "Lider-Epoca-2"
                self.iniciar_keep_alive()
        self.daemon.requestLoop()

    ## Lider ##
    @expose
    def registrar(self, id, estado, uri_objetoPyro):
        print("Registrando um " + estado)
        if estado == "Seguidor":
            self.seguidores.append({"uri": uri_objetoPyro , "id": id , "keep_alive": time.time()})
            self.num_seguidores += 1
        elif estado == "Observador":
            self.observadores.append({"uri": uri_objetoPyro , "id": id, "keep_alive": time.time()})

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
        # faça uma   função que espere a publicação ser commitada pelos votantes
        while True:
            schedule.run_pending()
            time.sleep(1)
            if self.qtd_confirmados.at[len(self.qtd_confirmados) - 1, self.epoca] >= int(self.num_seguidores / 2 + 1):
                break
        return True

    @expose 
    def replica_publicacoes(self, offset , epoca):
        publicacoes = self.log.consultar_publicacoes(epoca, offset)
        if(epoca != self.epoca):
            return True, [], self.epoca , self.log.consultar_offset(epoca)
        return False, publicacoes, self.epoca, self.log.consultar_offset(epoca)
    
    @expose 
    def confirmar_recebimento(self, epoca, offset):
        # Incrementa a quantidade de recebimento de publicações
        print("Confirmado recebimento")
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
            self.log.confirmar_publicacao(epoca, offset)
            self.comita_publicacao(epoca, offset)
        return
    
    def comita_publicacao(self, epoca, offset):
        for seguidor in self.seguidores:
            with Proxy(seguidor["uri"]) as seguidor_proxy:
                seguidor_proxy.receber_commit(epoca, offset)
        return
    @expose
    def consome_publicacao(self):
        publicacoes = self.log.consultar_publicacoes_confirmadas(self.epoca)
        print(publicacoes)
        return publicacoes
    @expose 
    @oneway
    def avisa_keep_alive(self, id):
        for seguidor in self.seguidores:
            if seguidor["id"] == id:
                seguidor["keep_alive"] = time.time()
                # print("Keep Alive recebido do ", id )
                break
        return



    def iniciar_verificador_keep_alive(self):
        def verificador_keep_alive():
            while True:
                for seguidor in self.seguidores:
                    if time.time() - seguidor["keep_alive"] > 10:
                        self.seguidores.remove(seguidor)
                        self.num_seguidores -= 1
                        print("Seguidor removido por inatividade")
                        if(self.num_seguidores < NUM_MINIMO_SEGUIDORES):
                            self.pede_observador_virar_seguidor()
                        self.notifica_numero_seguidores()
                time.sleep(1)
        Thread(target=verificador_keep_alive).start()

    def pede_observador_virar_seguidor(self):
        if len(self.observadores) > 0:
            observador = self.observadores.pop(0)
            with Proxy(observador["uri"]) as observador_proxy:
                observador_proxy.virar_seguidor(self.epoca, self.uri_objetoPyro)
                print("Observador virou seguidor")
        return
    
    def notifica_numero_seguidores(self):
        for seguidor in self.seguidores:
            with Proxy(seguidor["uri"]) as seguidor_proxy:
                print("Tenta notificar seguidor")
                seguidor_proxy.atualiza_numero_seguidores(self.num_seguidores)
        return
    

     ## Votante ##
    @expose
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
            # return {"epoca": self.epoca,"offset": self.log.consultar_offset(self.epoca)} 
            return              
            
    @expose
    @oneway 
    def atualiza_numero_seguidores(self , numero_seguidores):
        print("Numero de seguidores atualizado para ", numero_seguidores)
        self.num_seguidores= numero_seguidores
        
    def  corrige_publicacoes(self, epoca, offset, lider_proxy):
        # trunca o log da sua epoca 
        print("Erro nos logs")
        offset_antigo = self.log.consultar_offset(self.epoca)
        if(offset < offset_antigo):
            print("Truncando log")
            self.log.matriz_publicacoes[self.epoca] = self.log.matriz_publicacoes[self.epoca][:offset]
            self.log.refaz_arquivo()
        
        self.epoca = epoca
        erro , publicoes_faltantes, epoca, offset  = lider_proxy.replica_publicacoes(self.log.consultar_offset(self.epoca) , self.epoca)
        if erro == False:
            for publicacao in publicoes_faltantes:
                self.log.inserir_log(publicacao, epoca)
                lider_proxy.confirmar_recebimento(self.epoca, offset)
        else:
            self.corrige_publicacoes(epoca, offset, lider_proxy)
        return 
    
    def iniciar_keep_alive(self):
        def keep_alive():
            while True:
                with Proxy(self.lider_uri) as lider_proxy:
                    lider_proxy.avisa_keep_alive(self.id)
                time.sleep(5)
        Thread(target=keep_alive).start()
    @expose
    def receber_commit(self, epoca, offset):
        self.log.confirmar_publicacao(epoca, offset)
        return
                

    
    ## Observador ##
    @expose
    def virar_seguidor(self, epoca, uri_objetoPyro):
        self.estado = "Seguidor"
        self.epoca = epoca
        self.iniciar_keep_alive()
        print("Virou seguidor")
        self.lider = uri_objetoPyro
        # se atualiza com o lider
        with Proxy(self.lider) as lider_proxy:
            lider_proxy.registrar(self.id, self.estado, self.uri_objetoPyro)
        self.avisa_publicacao()
        return True

   

    



class Log:
    def __init__(self, pasta_log):
        diretorio_base = Path(__file__).parent
        (diretorio_base / "log").mkdir(parents=True, exist_ok=True)
        self.caminho_log = diretorio_base / pasta_log / "log" / "log.txt"
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
        if epoca not in self.matriz_publicacoes_confirmadas.columns:
            return 0
        return self.matriz_publicacoes_confirmadas[offset, epoca]
    
    def consultar_publicacoes_confirmadas(self, epoca):
        if epoca not in self.matriz_publicacoes.columns:
            return []
        # Lista para armazenar as publicações confirmadas
        publicacoes = []
        for i, publicacao in self.matriz_publicacoes[epoca].items():  
            if self.matriz_publicacoes_confirmadas.at[i + 1 , epoca] == 1:
                publicacoes.append(publicacao)
        # Retorna a lista de publicações confirmadas
        return publicacoes
    
    def refaz_arquivo(self):
        self.caminho_log.write_text("")
        for epoca in self.matriz_publicacoes.columns:
            for publicacao in self.matriz_publicacoes[epoca].dropna():
                timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                mensagem = f"{epoca} - {timestamp} - {publicacao}"
                with self.caminho_log.open("a", encoding='UTF-8') as arquivo:
                    arquivo.write(mensagem + "\n")




