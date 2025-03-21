# Задача 3.
# Система анализа транзакций клиентов банка
# Описание задачи:
# Банк хочет создать систему для анализа транзакций своих клиентов. Каждый клиент имеет счет, на котором происходят транзакции (пополнения, списания). Банк хочет:
# - Хранить информацию о клиентах и их транзакциях.
# - Анализировать транзакции (например, считать общий доход, расход, баланс).
# - Генерировать отчеты для клиентов.
#
# Необходимо создать классы на Python с функционалом описанным выше:
# Класс Client (Клиент):
# - Хранит информацию о клиенте (имя, ID).
# - Содержит список счетов клиента.
# Класс Account (Счет):
# - Хранит информацию о счете (номер счета, баланс).
# - Содержит список транзакций.
# Класс Transaction (Транзакция):
# - Хранит информацию о транзакции (тип: доход/расход, сумма, дата).
# Класс Bank (Банк):
# - Управляет клиентами и счетами.
# - Предоставляет методы для анализа данных (например, общий доход, расход, баланс).
#
# Дополнительные задания для практики:
# - Добавьте возможность фильтрации транзакций по дате.
# - Реализуйте метод для поиска клиента по ID.
# - Добавьте возможность экспорта транзакций в файл (например, CSV).
# - Реализуйте класс для генерации отчетов (например, PDF или Excel).
import csv
from datetime import datetime

# Создаем класс Транзакции, который содержит список транзакций
class Transaction:
    def __init__(self, transaction_type, amount, date=None):
        self.transaction_type = transaction_type
        self.amount = amount
        self.date = date if date else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def __str__(self):
        return f"Transaction {self.transaction_type}, {self.amount}. Дата: {self.date}"

# Создаем класс Аккаунт, который хранит информацию о счете и содержит список транзакций.
class Account:
    def __init__(self, acc_number):
        self.acc_number = acc_number
        self.balance = 0
        self.transactions = []

    # Создаем метод добавления транзакций и обновления баланса клиента
    def add_transaction(self, transaction):
        if transaction.transaction_type == "deposit":
            self.balance += transaction.amount
        elif transaction.transaction_type == "withdraw":
            # Проверяем достаточно ли средств на балансе
            if self.balance >= transaction.amount:
                self.balance -= transaction.amount
            else:
                print("Недостаточно средств для снятия")
                return
        self.transactions.append(transaction)

    def __str__(self):
        return f"Счёт {self.acc_number}. Баланс: {self.balance}"

# Создаем класс Клиент, который хранит информацию о клиенте и содержит список счетов клиента.
class Client:
    def __init__(self, name, client_id):
        self.name = name
        self.client_id = client_id
        self.accounts = []

    # Создаем метод добавления аккаунтов клиенту
    def add_account(self, account):
       self.accounts.append(account)

    # Метод получения общего баланса клиента
    def get_total_balance(self):
        return sum(account.balance for account in self.accounts)

    def __str__(self):
        return f"Клиент {self.name}, id = {self.client_id}"

# Создаем класс Банк, который управляет клиентами и счетами.
class Bank:
    def __init__(self, bank_name):
        self.bank_name = bank_name
        self.clients = []

    # Метод добавления клиента банка
    def add_clients(self, client):
        self.clients.append(client)

    # Получение клиента по id
    def get_client_by_id(self, client_id):
        for client in self.clients:
            if client.client_id == client_id:
                return client
        return None

    # Экспорт списка транзакций клиента в CSV
    def export_transactions_to_csv(self, client_id, filename):
        client = self.get_client_by_id(client_id)
        if not client:
            print("Клиент не найден")
            return

        with open(filename, 'w', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Account Number', 'Type', 'Amount', 'Date'])
            for account in client.accounts:
                for transaction in account.transactions:
                    writer.writerow(
                        [account.acc_number, transaction.transaction_type, transaction.amount, transaction.date])
        print(f"Transactions exported to {filename}")

    def __str__(self):
        return f"Банк {self.bank_name}"


bank = Bank("Superbank")
client = Client("Ivan Ivanov", "12")
account = Account("Acc101")
account.add_transaction(Transaction("deposit", 500))
account.add_transaction(Transaction("withdraw", 300))
client.add_account(account)
bank.add_clients(client)

# Вывод данных
print(bank.get_client_by_id("12"))

# Экспорт в CSV
bank.export_transactions_to_csv("12", "transactions.csv")

print(client.get_total_balance())
