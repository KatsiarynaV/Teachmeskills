# Условие:
# У вас есть файл raw_data.txt, который содержит "сырые" данные о транзакциях. Данные имеют неструктурированный формат и могут содержать ошибки. Пример содержимого:
# 2023-01-01:1000:Иван Иванов
# 2023-01-02:1500:Петр Петров
# 2023-01-03:2000:Мария Сидорова
# 2023-01-04:ERROR
# 2023-01-05:2500:Ольга Кузнецова
# 2023-01-06:3000:ERROR
#
# Задача:
# Напишите скрипт, который:
# Читает данные из файла raw_data.txt.
# Удаляет строки с ошибками (где вместо значений стоит ERROR).
# Преобразует оставшиеся данные в формат Дата,Сумма,Менеджер.
# Сохраняет очищенные данные в новый файл cleaned_data.txt.


def clean_data(filename):
    cleaned_data = []
    with open(filename, "r", encoding="utf-8") as file:
        for line in file:
            data = line.strip().split(':')
            if "ERROR" in line:
                continue
            else:
                cleaned_data.append(f"{data[0]}:{data[1]}:{data[2]}")

    return cleaned_data


def write_cleaned_data(filename, data):
    with open(filename, 'w', encoding='utf-8') as file:
        file.writelines(list(map(lambda x: x + "\n", data)))


cleaned_data = clean_data("raw_data.txt")
write_cleaned_data("cleaned_data.txt", cleaned_data)