# ==================================================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# ==================================================================================================

# Создаем данные для файла transactions.csv
data = {
    "transaction_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "customer_id": [1001, 1002, 1001, 1003, 1004, 1002, 1005, 1006, 1007, 1003],
    "product_id": [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010],
    "product_category": ["Electronics", "Clothing", "Electronics", "Groceries", "Electronics",
                         "Clothing", "Groceries", "Books", "Electronics", "Books"],
    "amount": [299.99, 49.99, 199.99, 15.99, 399.99, 29.99, 8.99, 14.99, 249.99, 24.99],
    "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-02-01",
             "2024-02-02", "2024-02-03", "2024-03-01", "2024-03-05", "2024-03-10"]
}

# ==================================================================================================

# Сохраняем данные в CSV файл
df = pd.DataFrame(data)
df.to_csv('transactions.csv', index=False)

# Загрузка данных из файла
df = pd.read_csv('transactions.csv')

# Проверка на наличие пропущенных значений
print("Пропущенные значения в данных:")
print(df.isnull().sum())

# Преобразование столбца date в формат datetime
df['date'] = pd.to_datetime(df['date'])

# Выводим первые несколько строк DataFrame для проверки
print("\nПервые несколько строк данных:")
print(df.head())

# ==================================================================================================

# Общее количество транзакций
total_transactions = df.shape[0]
print(f"Общее количество транзакций: {total_transactions}")

# Количество уникальных клиентов
unique_customers = df['customer_id'].nunique()
print(f"Количество уникальных клиентов: {unique_customers}")

# Топ-5 категорий продуктов по общему доходу
top_categories = df.groupby('product_category')['amount'].sum().nlargest(5)
print("\nТоп-5 категорий продуктов по общему доходу:")
print(top_categories)

# Средняя сумма транзакции по каждой категории продуктов
average_transaction_amount = df.groupby('product_category')['amount'].mean()
print("\nСредняя сумма транзакции по каждой категории продуктов:")
print(average_transaction_amount)

# ==================================================================================================

# Гистограмма распределения количества транзакций по месяцам
df['month'] = df['date'].dt.to_period('M')  # Извлекаем месяц
transactions_per_month = df['month'].value_counts().sort_index()

plt.figure(figsize=(10, 6))
sns.barplot(x=transactions_per_month.index.astype(str), y=transactions_per_month.values, palette='viridis')
plt.title('Распределение количества транзакций по месяцам')
plt.xlabel('Месяц')
plt.ylabel('Количество транзакций')
plt.xticks(rotation=45)
plt.show()

# ==================================================================================================

# Круговая диаграмма, показывающая долю дохода по категориям продуктов
revenue_by_category = df.groupby('product_category')['amount'].sum()

plt.figure(figsize=(8, 8))
plt.pie(revenue_by_category, labels=revenue_by_category.index, autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
plt.title('Доля дохода по категориям продуктов')
plt.axis('equal')
plt.show()

# ==================================================================================================

# Создаем новый столбец year_month
df['year_month'] = df['date'].dt.to_period('M').astype(str)

# ==================================================================================================

# Подготовка сводной таблицы
pivot_table = df.pivot_table(
    index='customer_id', 
    columns='year_month', 
    values='amount', 
    aggfunc='sum', 
    fill_value=0
).reset_index()

# Выводим сводную таблицу
print("\nСводная таблица общего дохода по каждому клиенту для каждого месяца:")
print(pivot_table)

# Сохраняем сводную таблицу в новый CSV-файл
pivot_table.to_csv('monthly_revenue_per_customer.csv', index=False)

# ==================================================================================================
