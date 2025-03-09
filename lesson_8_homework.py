# Задача 1: “Группировка по четности”
# Условие:
# Разделите список чисел на два кортежа: один с четными, другой с нечетными числами.
#
# Вход: [1, 5, 76, 13, 12]
# Выход: (76, 12), (1, 13)

import sys

numbers = sys.argv[1:]
even_numbers = tuple()
odd_numbers = tuple()

for num in numbers:
    if int(num) % 2 == 0:
        even_numbers = (*even_numbers, num)
    else:
        odd_numbers = (*odd_numbers, num)

print(even_numbers, odd_numbers)

# even_numbers = tuple(num for num in numbers if int(num) % 2 == 0)
# odd_numbers = tuple(num for num in numbers if int(num) % 2 != 0)
# print(even_numbers, odd_numbers)


# Задача 2: “Анаграммы”
# Условие:
# Проверьте, являются ли две строки анаграммами друг друга.
#
# Вход: марш, шрам
# Выход: Да, это “Анаграмма”

word_1 = sys.argv[1]
word_2 = sys.argv[2]

dict_1 = {}
dict_2 = {}

for char in word_1:
    if char in dict_1:
        dict_1[char] += 1
    else:
        dict_1[char] = 1

for char in word_2:
    if char in dict_2:
        dict_2[char] += 1
    else:
        dict_2[char] = 1

if dict_1 == dict_2:
    print("Да, это Анаграмма")
else:
    print("Нет, это не Анаграмма")



