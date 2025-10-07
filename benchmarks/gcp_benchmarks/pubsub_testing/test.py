import sys

if __name__ == "__main__":
    my_int = 10
    print(f"Size of integer: {sys.getsizeof(my_int)} bytes")

    my_string = "hello"
    print(f"Size of string: {sys.getsizeof(my_string)} bytes")

    my_list = [1, 2, 3]
    print(f"Size of list: {sys.getsizeof(my_list)} bytes")
