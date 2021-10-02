from random import randrange


class Matriz:
    
    def __init__(self, mat):
        self.mat = mat
        self.lin = len(mat)
        self.col = len(mat[0])

    def get_line(self, n):
        return [i for i in self.mat[n]]

    def get_column(self, n):
        return [i[n] for i in self.mat]

    def __mul__(self, mat2):
        matRes = []

        for i in range(self.lin):           
            matRes.append([])

            for j in range(mat2.col):
                listMult = [x * y for x, y in zip(self.get_line(i), mat2.get_column(j))]
                matRes[i].append(sum(listMult))

        return matRes


def generate_array(n):
    if (n % 2):
        print('only even numbers !')
        return
    
    return [[randrange(11) for i in range(n)] for i in range(n)]


if __name__ == '__main__':
    array1 = generate_array(2)
    array2 = generate_array(2)

    print('Matriz A', array1)
    print('Matriz B', array2)

    array1 = Matriz(array1)
    array2 = Matriz(array2)

    print('Produto', array1 * array2)
