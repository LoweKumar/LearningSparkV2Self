def printPattern():
    print("Hello World")
    for i in range(5,0, -1):
        while i!=0:
            print("*",end="")
            i-=1
        print()

# alternate solution
def printPatternRange():
    for i in range(1, 6):
        for j in range(i):
            print(j+1, end="")
        print()

def right_angle_triangle(n):
    for i in range(1, n + 1):
        print('*' * i)

def printPatternReverse():
    for i in range(5, 0, -1):
        for j in range(i):
            print("*", end="")
        print()

def pyramid_pattern(n):
    for i in range(1, n + 1):
        print(' ' * (n - i) + '*' * (2 * i - 1))


if __name__ == '__main__':
    # printPattern()
    # printPatternRange()
    # right_angle_triangle(5)
    # printPatternReverse()
    pyramid_pattern(5)
