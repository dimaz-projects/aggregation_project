﻿
# Тестовое задание

Тип: 

- Консольная программа (Для Java, C#, Python).
- Веб-приложение (Для JavaScript).

> **Замечание для JavaScript**: задание изначально писалось для языков общего назначения, что не делает его менее замечательным. Если вы выполняете его на JavaScript, _самостоятельно_ определите список допущений и видоизмените реализацию на свое усмотрение (сохранив общий функционал).


## Требования

Этапы: 

- Получение данных на вход.
- Агрегация данных в соответствии с ключами запуска.
- Вывод данных.


### Запуск программы

Возможные варианты запуска:

- `Aggregator.jar -itype csv -if ./log-2017-06-06.csv -agg user:avg:type -otype xml -of ./out.xml`
- `Aggregator.jar -itype -otype db -conf ./config.prop -agg referer:count:transmit < cat ./log-2017-06-06.csv`


### Входные данные 

Для генерации входных данных необходимо запустить файл generate_data.py (необходим установленный Python 3.x)

(выбор по ключу):
- Метод ввода:
    + Std-in
    + Файл
- Формат:
    + CSV:
        ```csv
        start_page,referer,user,ts,depth,duration,transmit,type
        321,4432,5,2016-11-02 16:37:03.240,223,543,7,2345
        ```
    + XML:
        ```xml
        <?xml version="1.0" encoding="UTF-8"?>
        <root>
            <row>
                <start_page>321</start_page>
                <referer>4432</referer>
                <user>5</user>
                <ts>2016-11-02 16:37:03.240</ts>
                <depth>223</depth>
                <duration>543</duration>
                <transmit>7</transmit>
                <type>2345</type>
            </row>
        </root>
        ```


### Обработка:

- Провести агрегацию данных:
    + Передается ключ в виде:
        ```
        поле_группировки:вид_агрегации:агрегируемое_поле
        вид_агрегации := [ sum | avg | count | min | max ]
        ```
        (примеры агрегаций ниже)
    + Соответственно, вывод имеет вид:
        - CSV:
            ```csv
            referer,count_ts
            4432,28034
            ```
        - XML:
            ```xml
            <?xml version="1.0" encoding="UTF-8"?>
            <root>
                <row>
                    <referer>4432</referer>
                    <count_ts>28034</count_ts>
                </row>
            </root>
            ```
                
                
### Вывод

(выбор по ключу)

- Метод:
    + Std-out
    + Файл
- Формат (аналогично вводу):
    + CSV
    + XML
- JDBC/ADO.NET/pyodbc (коннект по файлу конфига, cоздание таблицы с соответствующими типами, заливка обработанных данных в таблицу).


# Требования к реализации

- Соблюдение принципов ООП.
- Использование типичной структуры проекта для выбранного языка.
- Параллельное выполнение расчетов.
- Логирование.
- Обработка исключений.
- Не применять сторонние библиотеки для реализации логики.
- Применять (при необходимости) сторонние библотеки для утилитарных операций (парсинг ключей, логирование и пр.)
- Javadoc/docstring и комментирование кода + `README.md` с инструкциями по запуску.
- Стиль кода, например, в соответствии с Google Style Guides.
- Unit-тесты.

> Если какие-то моменты вам кажутся спорными или непонятными, можете трактовать задание, как вам будет удобно.

> Убедительная просьба сделать так, чтобы код можно было запустить на любой машине с минимум телодвижений. Идеальный вариант: докеризация приложения.

Очень ждем ссылок на ваш репозиторий с реализаций приложения.


# Примеры агрегаций

- Входные данные:
    ```
    category    value
    1           984
    1           688
    1           938
    2           747
    1           95
    1           241
    3           815
    1           394
    1           397
    3           113
    3           572
    2           22
    1           475
    2           498
    2           401
    3           281
    3           961
    ```
    
- `count` (количество значений по категории):
    ```
    category    count_value
    1           4
    2           10
    3           3
    ```

- `sum` (сумма значений):
    ```
    category    sum_value
    1           1590
    2           4162
    3           1408
    ```

- `avg` (среднее арифметическое):
    ```
    category    avg_value
    1           397.5
    2           416.2
    3           469.3333333
    ```

