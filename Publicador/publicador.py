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
                valido = lider.publicar(mensagem)
                if(valido):
                    print("Publicação realizada com sucesso.")
                else:
                    print("Publicação não realizada.")
        except Exception as e:
            print(f"Erro ao tentar publicar: {e}")


if __name__ == "__main__":
    publicador = Publicador()
    while True:
        publicador.publicar()
