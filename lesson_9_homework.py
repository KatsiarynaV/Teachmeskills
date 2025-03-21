# Задача 2: Объединение данных о студентах
# Есть два файла: “students.txt” (ФИО, группа) и “grades.txt” (ФИО, оценки)
# Задача: создать файл “report.txt” с информацией о студентах и их оценках, о среднем бале для каждого студента.
# А так же в конце файла для каждой группы вывести средний бал по группе в порядке возрастания
#
# Пример вывода:
# Иванов Иван Иванович, Группа 101, 5 4 3 4 5, Средняя оценка: 4.20
# Петров Петр Петрович, Группа 102, 4 4 5 3 4, Средняя оценка: 4.00
# Сидоров Сидор Сидорович, Группа 101, 3 5 4 4 3, Средняя оценка: 3.80
# Козлова Анна Сергеевна, Группа 102, 5 5 4 5 4, Средняя оценка: 4.60
#
# Средние оценки по группам (в порядке возрастания):
# Группа 101: 3.92
# Группа 202: 4.30

students = {}
try:
    with open('files/students.txt', 'r', encoding='utf-8') as file:
        for line in file:
            data = line.strip().split(', ')
            name = data[0]
            group = data[1]
            students[name] = {"group": group, "grades": [], "average": 0}
except FileNotFoundError:
    print("Файл не найден")

try:
    with open('files/grades.txt', 'r', encoding='utf-8') as file:
        for line in file:
            data = line.strip().split(', ')
            name = data[0]
            grades = list(map(int, data[1].split(' ')))
            average_grade = sum(grades) / len(grades)
            if name in students:
                students[name]["grades"] = grades
                students[name]["average"] = average_grade
except FileNotFoundError:
    print("Файл не найден")

group_average = {}
for name, info in students.items():
    group = info["group"]
    if group not in group_average:
        group_average[group] = []
    group_average[group].append(info["average"])


group_results = {group: round(sum(scores) / len (scores), 2) for group, scores in group_average.items()}

try:
    with open('files/report.txt', 'w', encoding='utf-8') as file:
        for name, info in students.items():
            grades_str = " ".join(map(str, info["grades"]))
            file.write(f"{name}, {info['group']}, {grades_str}, Средняя оценка: {info['average']}\n")

        file.write("\nСредние оценки по группам (в порядке возрастания): \n")
        for group, result in sorted(group_results.items(), key=lambda item: item[1]):
            file.write(f"{group}: {group_results[group]}\n")
except Exception as e:
    print("Ошибка обновления файла")



# Задача 3
# Условие:
# У вас есть два файла:
# accounts.txt — содержит текущие балансы счетов клиентов. Формат: Имя:Баланс.
# transactions.txt — содержит список транзакций, которые нужно применить к балансам.
# Формат: Имя:Сумма, где Сумма может быть положительной (пополнение) или отрицательной (списание).
# Задание: Вывести в accounts.txt - итоговое значение счета

accounts = {}
try:
    with open('files/accounts.txt', 'r', encoding='utf-8') as file:
        for line in file:
            data = line.strip().split(':')
            name = data[0]
            balance = int(data[1])
            accounts[name] = balance
except FileNotFoundError:
    print("Файл не найден")


try:
    with open('files/transactions.txt', 'r', encoding='utf-8') as file:
        for line in file:
            data = line.strip().split(':')
            name = data[0]
            transactions = int(data[1])
            if name in accounts:
                accounts[name] += transactions
            else:
                accounts[name] = transactions
except FileNotFoundError:
    print("Файл не найден")

try:
    with open('files/accounts.txt', 'w', encoding='utf-8') as file:
        for name, balance in accounts.items():
            file.write(f"{name}, {balance}\n")
except Exception as e:
    print("Ошибка обновления файла")

