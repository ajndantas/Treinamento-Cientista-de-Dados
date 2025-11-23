class Parent:  # Classe pai

    parentAttr = 100

    def __init__(self):
        print("Chamando construtor da classe pai")    
        
    def parentMethod(self):
        print("Chamando método da classe pai")

    def setAttr(self, attr):
        Parent.parentAttr = attr

    def getAttr(self):
        print("Atributo pai", Parent.parentAttr)

    def myMethod(self):
        print("Método da classe pai")
