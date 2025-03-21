# Задача 1.
# Создать и обработать файлы
# Есть данные:
# data_2023_01 = [
#     "2023-01-01:1000:Иван Иванов",
#     "2023-01-02:1500:Петр Петров",
#     "2023-01-03:2000:Мария Сидорова"
# ]
#
# data_2023_02 = [
#     "2023-02-01:1200:Иван Иванов",
#     "2023-02-02:1800:Петр Петров",
#     "2023-02-03:2200:Мария Сидорова"
# ]
#
# data_2023_03 = [
#     "2023-03-01:1300:Иван Иванов",
#     "2023-03-02:1700:Петр Петров",
#     "2023-03-03:2100:Мария Сидорова"
# ]
# 1. Нужно создать файлы:
# Каждый файл имеет формат sales_YYYY_MM.txt, где YYYY — год, а MM — месяц. Внутри каждого файла данные представлены в формате:
# Дата:Сумма продаж:Менеджер.
#
# Пример файла sales_2023_01.txt:
#
# 2023-01-01:1000:Иван Иванов
# 2023-01-02:1500:Петр Петров
# 2023-01-03:2000:Мария Сидорова
#
# 2. Напишите скрипт, который:
# - Считает общую сумму продаж за все месяцы.
# - Находит менеджера с наибольшей суммой продаж.
# - Сохраняет результаты в файл report.txt в формате:
# Общая сумма продаж: <сумма>
# Лучший менеджер: <имя менеджера>

data_2023_01 = [
    "2023-01-01:1000:Иван Иванов",
    "2023-01-02:1500:Петр Петров",
    "2023-01-03:2000:Мария Сидорова"
]

data_2023_02 = [
    "2023-02-01:1200:Иван Иванов",
    "2023-02-02:1800:Петр Петров",
    "2023-02-03:2200:Мария Сидорова"
]

data_2023_03 = [
    "2023-03-01:1300:Иван Иванов",
    "2023-03-02:1700:Петр Петров",
    "2023-03-03:2100:Мария Сидорова"
]

# Создаем три файла
def create_sales_files(filename, data):
    with open(filename, "w", encoding="utf-8") as file:
        file.writelines(list(map(lambda x: x + "\n", data)))


create_sales_files("sales_2023_01.txt", data_2023_01)
create_sales_files("sales_2023_02.txt", data_2023_02)
create_sales_files("sales_2023_03.txt", data_2023_03)

# Высчитываем общую сумму продаж, находим лучшего менеджера
total_sales = 0
manager_sales = {}

filenames = ["sales_2023_01.txt", "sales_2023_02.txt", "sales_2023_03.txt"]

for filename in filenames:
    try:
        with open(filename, "r", encoding="utf-8") as file:
            for line in file:
                data = line.strip().split(':')
                amount = int(data[1])
                manager = data[2]
                total_sales += amount
                if manager in manager_sales:
                    manager_sales[manager] += amount
                else:
                    manager_sales[manager] = amount
    except FileNotFoundError:
        print("Error: File not found")

def find_best_manager(manager_sales):
    best_manager = None
    best_manager_sales = 0
    for manager, amount in manager_sales.items():
        if amount > best_manager_sales:
            best_manager = manager
            best_manager_sales = amount
    return best_manager

def write_report_file(filename, total_sales, best_manager):
    try:
        with open(filename, "w", encoding="utf-8") as file:
            file.write(f"Общая сумма продаж: {total_sales}\n")
            file.write(f"\nЛучший менеджер: {best_manager}\n")
    except:
        print("Error on file create")

best_manager = find_best_manager(manager_sales)
write_report_file("report.txt", total_sales, best_manager)