from src.app.random_utils import get_random_number


def get_number(input_number):
    number = get_random_number()
    print("Value of input_number is {}".format(input_number))
    print("Value of number is {}".format(number))

    if input_number == number:
        print("Number is matched")
        return True
    else:
        print("Number is not matched")
        return False

