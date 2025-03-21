# Функции для Задачи 2.
# Создайте программу, которая считывает список чисел из файла, проверяет каждое число на чётность
# и записывает результаты (чётное или нечётное) в другой файл.
# Используйте обработку исключений для возможных ошибок ввода-вывода.

# Считываем числа из файла и добавляем их в список. Проверяем на некорректные данные и существует ли файл
def read_numbers_from_file(filename):
    try:
        with open(filename, "r", encoding="utf-8") as file:
            numbers = [int(line.strip()) for line in file]
            return numbers
    except ValueError:
        print("Ошибка: введены некорректные данные")
    except FileNotFoundError:
        print("Ошибка: файл не найден")
    return []

# Создаем файлы с четными и нечетными числами
def create_numbers_files(filename, numbers_list):
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            file.writelines(f"{num}\n" for num in numbers_list)
    except Exception as e:
        print(f"Ошибка при создании файла: {e}")