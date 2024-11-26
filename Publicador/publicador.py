from Pyro5.api import locate_ns, Proxy

class Publicador:
    def __init__(self):
        self.servidor_nomes = locate_ns()
    
    def publicar(self):
        try:
            # Obtém o URI do objeto remoto
            uri_objetoPyro = self.servidor_nomes.lookup("Lider-Epoca-1")
            print(f"URI obtido: {uri_objetoPyro}")
            
            # Cria o proxy no momento da publicação
            with Proxy(uri_objetoPyro) as lider:
                mensagem = input("Digite a mensagem a ser publicada: ")
                lider.publicar(mensagem)
                print("Publicação realizada com sucesso.")
        except Exception as e:
            print(f"Erro ao tentar publicar: {e}")

if __name__ == "__main__":
    publicador = Publicador()
    publicador.publicar()
