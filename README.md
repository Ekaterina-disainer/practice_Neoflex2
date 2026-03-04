# practice_Neoflex2
## Задание по Big Data
#### 1.  Познакомится с экосистемой Big Datа. Запустить локальный экземпляр Hadoop Distributed File System (HDFS) на ноутбуке или ПК. 
#### 2.  Придумать каждый свою базу данных на любую тему и реализовать в Hadoop.
#### 3.  * Сделать 3 модели данных: звезда, снежинка, Data Vault.
<img width="624" height="452" alt="parametr" src="https://github.com/user-attachments/assets/96ddf9e9-fba0-4867-b3b4-1e54f8ade5ea" />
<img width="624" height="510" alt="interface" src="https://github.com/user-attachments/assets/5051ef61-959d-46ee-9f9f-69836781060b" />

#### Запрос для создания БД.

<img width="624" height="477" alt="zapros" src="https://github.com/user-attachments/assets/0010212c-1aa6-48c7-baf3-2990b59fc370" />

```sql
--Создание и заполнение таблиц
CREATE TABLE IF NOT EXISTS books (
    book_id INT,
    title STRING,
    author STRING,
    genre STRING,
    year INT,
    pages INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE books VALUES
    (1, 'War and Peace', 'Leo Tolstoy', 'Novel', 1869, 1225),
    (2, 'Crime and Punishment', 'Fyodor Dostoevsky', 'Novel', 1866, 671),
    (3, '1984', 'George Orwell', 'Dystopia', 1949, 328),
    (4, 'The Master and Margarita', 'Mikhail Bulgakov', 'Novel', 1967, 480),
    (5, 'The Little Prince', 'Antoine de Saint-Exupery', 'Fairy tale', 1943, 96);
```

<img width="624" height="337" alt="ы" src="https://github.com/user-attachments/assets/02c5ed9c-fa86-4366-aa6a-4c83a98f82bb" />

<img width="624" height="602" alt="Рисунок5" src="https://github.com/user-attachments/assets/2143608d-77f7-4c17-bd67-fd80f2edcba9" />


#### Модель 1: Звезда 

```sql
-- Создание таблиц
CREATE TABLE dim_book (
    book_sk INT,        
    book_id INT,        
    title STRING,
    author STRING,
    genre STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

CREATE TABLE dim_reader (
    reader_sk INT,
    reader_id INT,
    full_name STRING,
    reg_date STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

CREATE TABLE dim_date (
    date_sk INT,
    full_date STRING,
    year INT,
    month INT,
    day INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

CREATE TABLE fact_loans (
    loan_id INT,
    book_sk INT,        
    reader_sk INT,      
    date_sk INT,        
    return_flag STRING 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
```
```sql
--Заполение таблиц
INSERT INTO dim_book VALUES
(1, 1, 'War and Peace', 'Leo Tolstoy', 'Novel'),
(2, 2, 'Crime and Punishment', 'Fyodor Dostoevsky', 'Novel'),
(3, 3, '1984', 'George Orwell', 'Dystopia'),
(4, 4, 'The Master and Margarita', 'Mikhail Bulgakov', 'Novel'),
(5, 5, 'The Little Prince', 'Antoine de Saint-Exupery', 'Fairy tale');

INSERT INTO dim_reader VALUES
(1, 101, 'Ivan Petrov', '2025-01-15'),
(2, 102, 'Maria Sidorova', '2025-02-20'),
(3, 103, 'Alex Smirnov', '2025-03-10');

INSERT INTO dim_date VALUES
(1, '2026-02-01', 2026, 2, 1),
(2, '2026-02-02', 2026, 2, 2),
(3, '2026-02-03', 2026, 2, 3);

INSERT INTO fact_loans VALUES
(1001, 1, 1, 1, 'N'),  
(1002, 3, 2, 1, 'Y'),  
(1003, 2, 3, 2, 'N');
```

<img width="624" height="587" alt="Рисунок6" src="https://github.com/user-attachments/assets/fdb3c18c-edab-409f-b425-d2dadfaa8cc0" />

#### Модель 2: Снежинка

```sql
--Создание и заполнение таблиц
CREATE TABLE dim_genre (
    genre_sk INT,
    genre_name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

INSERT INTO dim_genre VALUES
(1, 'Novel'),
(2, 'Dystopia'),
(3, 'Fairy tale');

CREATE TABLE dim_book_snow (
    book_sk INT,
    book_id INT,
    title STRING,
    author STRING,
    genre_sk INT   
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
INSERT INTO dim_book_snow VALUES
(1, 1, 'War and Peace', 'Leo Tolstoy', 1),
(2, 2, 'Crime and Punishment', 'Fyodor Dostoevsky', 1),
(3, 3, '1984', 'George Orwell', 2),
(4, 4, 'The Master and Margarita', 'Mikhail Bulgakov', 1),
(5, 5, 'The Little Prince', 'Antoine de Saint-Exupery', 3);
```
<img width="624" height="608" alt="Рисунок7" src="https://github.com/user-attachments/assets/2b446617-1415-4f03-a8ed-e5f3fca22d40" />

#### Модель 3: Data Vault

```sql
CREATE TABLE hub_book (
    book_hub_id INT,
    book_id INT,
    load_date STRING,
    record_source STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

CREATE TABLE hub_reader (
    reader_hub_id INT,
    reader_id INT,
    load_date STRING,
    record_source STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
CREATE TABLE hub_date (
    date_hub_id INT,
    full_date STRING,
    load_date STRING,
    record_source STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

CREATE TABLE link_loan (
    loan_link_id INT,
    book_hub_id INT,
    reader_hub_id INT,
    date_hub_id INT,
    load_date STRING,
    record_source STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
CREATE TABLE sat_loan_details (
    loan_link_id INT,
    return_flag STRING,
    load_date STRING,
    record_source STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
-- Заполение 
INSERT INTO hub_book VALUES
(1, 1, '2026-02-27', 'books_source'),
(2, 2, '2026-02-27', 'books_source'),
(3, 3, '2026-02-27', 'books_source');

INSERT INTO hub_reader VALUES
(1, 101, '2026-02-27', 'readers_source'),
(2, 102, '2026-02-27', 'readers_source'),
(3, 103, '2026-02-27', 'readers_source');

INSERT INTO hub_date VALUES
(1, '2026-02-01', '2026-02-27', 'dates_source'),
(2, '2026-02-02', '2026-02-27', 'dates_source');

INSERT INTO link_loan VALUES
(1001, 1, 1, 1, '2026-02-27', 'loans_source'),
(1002, 3, 2, 1, '2026-02-27', 'loans_source'),
(1003, 2, 3, 2, '2026-02-27', 'loans_source');

INSERT INTO sat_loan_details VALUES
(1001, 'N', '2026-02-27', 'loans_source'),
(1002, 'Y', '2026-02-27', 'loans_source'),
(1003, 'N', '2026-02-27', 'loans_source');
```

<img width="624" height="671" alt="Рисунок8" src="https://github.com/user-attachments/assets/792adf7f-b896-4a46-92e7-f51ed784e4bd" />
