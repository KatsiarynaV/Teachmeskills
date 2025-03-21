# Задача с урока.
# Напишите программу, которая запрашивает у пользователя два числа и выполняет деление.
# Если ввод не является числом или деление на ноль, программа должна обрабатывать эти исключения
# и продолжать запрашивать ввод до тех пор, пока не будут введены корректные значения.
from numpy.matlib import empty

while True:
    try:
        num1 = int(input("Введите число: "))
        num2 = int(input("Введите число: "))

        result = num1 / num2
        print(f"Результат деления: {result}")
        break
    except ValueError:
        print("Ошибка: введено не число")
    except ZeroDivisionError:
        print("Ошибка: деление на ноль")


# Задача 1.
# Напишите функцию, которая принимает список чисел и возвращает их среднее значение.
# Обработайте исключения, связанные с пустым списком и некорректными типами данных.

import sys

def list_average():
    try:
        numbers.txt = sys.argv[1:]
        if not numbers.txt:
            return "Ошибка: вы не ввели числа"
        my_list = [int(num) for num in numbers.txt]
        list_average = sum(my_list) / len(my_list)
        return f"Среднее значение: {list_average}"
    except ValueError:
        return "Ошибка: введены некорректные данные"
    except ZeroDivisionError:
        return "Ошибка: деление на ноль"

if __name__ == "__main__":
    print(list_average())


# Задача 2.
# Создайте программу, которая считывает список чисел из файла, проверяет каждое число на чётность
# и записывает результаты (чётное или нечётное) в другой файл.
# Используйте обработку исключений для возможных ошибок ввода-вывода.

# Импортируем функции
from lesson_11_functions import read_numbers_from_file, create_numbers_files

numbers = read_numbers_from_file("numbers.txt")

if numbers: # Проверяем, есть ли данные
    # Фильтруем четные, нечетные
    even_numbers = [num for num in numbers if num % 2 == 0]
    odd_numbers = [num for num in numbers if num % 2 != 0]
    # Создаем файлы
    create_numbers_files("even_numbers.txt", even_numbers)
    print(f"Файл успешно создан")
    create_numbers_files("odd_numbers.txt", odd_numbers)
    print(f"Файл успешно создан")
else:
    print("Не удалось обработать данные")

