from Pyro5.api import locate_ns, Proxy

class Consumidor:
    def __init__(self):
        self.servidor_nomes = locate_ns()
    
    def consumir(self):
        try:
            # Obtém o URI do objeto remoto
            uri_objetoPyro = self.servidor_nomes.lookup("Lider-Epoca-1")
            print(f"URI obtido: {uri_objetoPyro}")
            
            # Cria o proxy no momento da publicação
            with Proxy(uri_objetoPyro) as lider:
                publicacoes = lider.consome_publicacao()
                print(publicacoes)
                print("Consumo de publicao realizada com sucesso.")
        except Exception as e:
            print(f"Erro ao tentar consumir: {e}")

if __name__ == "__main__":
    consumidor = Consumidor()
    while True:
        input("Pressione Enter para consumir uma publicação")
        consumidor.consumir()