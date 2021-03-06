import numpy as np
import matplotlib.pyplot as plt

'''
    Return evenly spaced numbers over a specified interval
'''
x = np.linspace(0, 10, 500) # Semelhante a np.arange, a diferença é que se especifica                              # a quantidade de valores, não os limites.
                            # linspace(start, stop[, num, endpoint, …])

print("Tamanho de x: " + str(len(x)) + " elementos")
#print(x) # x é um array

y = np.sin(x) # Calcula o seno para cada item do array

fig, ax = plt.subplots() # Retorna uma figura e os eixos. Método de classe matplotlib.pyplot

# Using set_dashes() to modify dashing of an existing line
line1, = ax.plot(x, y, label='Using set_dashes()') # Método plot da classe matplotlib.axe.Axes que retorna um Line2D (<class 'matplotlib.lines.Line2D'>)
#print(type(line1))

'''
    Parâmetros de plotagem de linhas
'''
line1.set_dashes([2, 2, 10, 2])  # 2pt line, 2pt break, 10pt line, 2pt break

# Using dashes parameter
# Using plot(..., dashes=...) to set the dashing when creating a line
line2, = ax.plot(x, y - 0.2, dashes=[6, 2], label='Using the dashes parameter')

ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), shadow=True, ncol=2)
plt.show() # Mostra o gráfico