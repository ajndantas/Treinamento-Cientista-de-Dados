# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
def fib(n):
    result = []

    a, b = 0 ,1

    while b < n:
        result.append(b)
        a, b = b , b + a
    
    return result

if __name__ == "__main__": # Igual ao public static void main do Java
    f = fib(100) 
    print(f) 