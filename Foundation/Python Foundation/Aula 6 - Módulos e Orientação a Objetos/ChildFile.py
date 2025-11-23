from ParentFile import Parent # IMPORTANDO A CLASSE PAI PARA A FILHA


class Child(Parent):

    def __init__(self):
        print("Chamando o construtor filho")

    def childMethod(self):
        print("Chamando o método filho")

    def myMethod(self):
        print("Método da classe filha")
