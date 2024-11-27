import sys
import os

# Adiciona o diretório raiz do projeto ao caminho
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from broker import Broker

if __name__ == "__main__":
    broker = Broker("Seguidor", "Lider-Epoca-1","Votante")
    broker.iniciar()