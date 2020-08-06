-- CREATE DATABASE chitai_gorod;

CREATE SCHEMA catalog;

CREATE TABLE catalog.Editor(
    editor_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    editor_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE catalog.EditionYear(
    edition_year SMALLINT PRIMARY KEY
);

CREATE TABLE catalog.Book(
    book_id INT PRIMARY KEY,
    book_name VARCHAR(255) /*UNIQUE*/ NOT NULL,
    price NUMERIC(9,2) /*NOT*/ NULL,
    edition_year SMALLINT NOT NULL REFERENCES catalog.EditionYear(edition_year),
    editor_id INT REFERENCES catalog.Editor(editor_id) NOT NULL
);

CREATE TABLE catalog.Author(
    author_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    author_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE catalog.AuthorRole(
    role_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    role_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE catalog.AuthorBook(
    author_id INT REFERENCES catalog.Author(author_id),
    book_id INT REFERENCES catalog.Book(book_id),
    role_id INT REFERENCES catalog.AuthorRole(role_id) NOT NULL,
    CONSTRAINT PK_AuthorBook PRIMARY KEY (author_id, book_id)
);


-- Скрипт для загрузки данных в БД из csv, в переменной csv_path необходимо указать путь к папке с файлами
DO $$
    DECLARE
        csv_path VARCHAR(255) := 'C:/projects/data/';
    BEGIN
        CREATE TEMPORARY TABLE temp_editor(name VARCHAR(255));
        CREATE TEMPORARY TABLE temp_author_role(role_name VARCHAR(50));
        CREATE TEMPORARY TABLE temp_author(author_name VARCHAR(255));
        CREATE TEMPORARY TABLE temp_book(book_id INT, book_name VARCHAR(255), price NUMERIC(9,2), edition_year SMALLINT, editor_name VARCHAR(255));
        CREATE TEMPORARY TABLE temp_author_book(author_name VARCHAR(255), book_id INT, role_name VARCHAR(50));
        CREATE TEMPORARY TABLE temp_year(edition_year SMALLINT);
    EXECUTE 'COPY temp_editor(name) FROM ' || quote_literal(concat(csv_path, 'editors.csv')) || 'WITH (FORMAT csv, HEADER true)';
        INSERT INTO catalog.editor(editor_name) SELECT name FROM temp_editor;
    EXECUTE 'COPY temp_author_role(role_name) FROM ' || quote_literal(concat(csv_path, 'roles.csv')) || 'WITH (FORMAT csv, HEADER true)';
        INSERT INTO catalog.AuthorRole(role_name) SELECT role_name FROM temp_author_role;
    EXECUTE 'COPY temp_author(author_name) FROM ' || quote_literal(concat(csv_path, 'authors.csv')) || 'WITH (FORMAT csv, HEADER true)';
        INSERT INTO catalog.Author(author_name) SELECT author_name FROM temp_author;
    EXECUTE 'COPY temp_year(edition_year) FROM ' || quote_literal(concat(csv_path, 'years.csv')) || 'WITH (FORMAT csv, HEADER true)';
        INSERT INTO catalog.EditionYear(edition_year) SELECT edition_year FROM temp_year;
    EXECUTE 'COPY temp_book(book_id, book_name, price, edition_year, editor_name) FROM '
                || quote_literal(concat(csv_path, 'books.csv'))
                || 'WITH (FORMAT csv, HEADER true, DELIMITER ' || quote_literal(';') || ')';
        INSERT INTO catalog.book(book_id, book_name, price, edition_year, editor_id)
        SELECT
            TB.book_id,
            TB.book_name,
            TB.price,
            TB.edition_year,
            (SELECT editor_id FROM catalog.Editor WHERE editor_name = TB.editor_name)
        FROM temp_book AS TB;

    EXECUTE 'COPY temp_author_book(author_name, book_id, role_name) FROM '
                || quote_literal(concat(csv_path, 'authors_books.csv'))
                || 'WITH (FORMAT csv, HEADER true, DELIMITER ' || quote_literal(';') || ')';
        INSERT INTO catalog.AuthorBook(author_id, book_id, role_id)
        SELECT
            (SELECT A.author_id FROM catalog.Author AS A WHERE A.author_name = TAB.author_name),
            book_id,
            (SELECT AR.role_id FROM catalog.AuthorRole AS AR WHERE AR.role_name = TAB.role_name)
        FROM temp_author_book AS TAB;
    END;
    $$ LANGUAGE plpgsql;

-- Примеры запросов к БД
-- 1. Получить годы, в которые не были издана ни одна книга из выборки
SELECT edition_year FROM catalog.EditionYear
WHERE edition_year NOT IN (SELECT DISTINCT edition_year FROM catalog.Book);

-- 2. Получить сведения о книгах и лиц, участвовавших в их оформлении, чья роль отлична от "автор"
SELECT
    A.author_name,
    B.book_name,
    AR.role_name
FROM catalog.AuthorBook AS AB
    JOIN catalog.Author AS A ON A.author_id = AB.author_id
    JOIN catalog.Book AS B ON B.book_id = AB.book_id
    JOIN catalog.AuthorRole AS AR ON AR.role_id = AB.role_id
WHERE AR.role_name <> 'автор';

-- 3. Получить издательства, выпустившие более 10000 книг с 2010 по 2020 годы включительно
SELECT
    E.editor_name
FROM catalog.Editor AS E
    JOIN catalog.Book AS B ON E.editor_id = B.editor_id
WHERE B.edition_year BETWEEN 2010 AND 2020
GROUP BY E.editor_name
HAVING COUNT(*) > 10000;

-- 4. Получить идентификатор и наименование для книг, в кажестве автора которых указано издательство
SELECT
    B.book_id,
    B.book_name
FROM catalog.AuthorBook AS AB
    JOIN catalog.Author AS A ON AB.author_id = A.author_id
    JOIN catalog.Book AS B ON AB.book_id = B.book_id
WHERE replace(A.author_name, '.', '') IN (SELECT editor_name FROM catalog.Editor);

-- 5. Получить количество книг, у которых не указан автор
SELECT COUNT(*) FROM catalog.Book AS B
     WHERE B.book_id NOT IN (SELECT book_id FROM catalog.AuthorBook);

SELECT
    B.book_id,
    B.book_name,
    B.edition_year
FROM catalog.Book AS B
WHERE B.price = (SELECT MAX(price) FROM catalog.Book);

-- 6. Получите перечень издательство - цена самой дорогой книги в рамках издательства,
-- исключив издательства с наибольшей и наименьшей стоимостью самых дорогих книг среди издателей
SELECT DISTINCT
    E.editor_name,
    B.price
FROM catalog.Editor AS E
    JOIN catalog.Book AS B ON B.editor_id = E.editor_id
WHERE B.price = (SELECT MAX(price) FROM catalog.Book AS B2 WHERE E.editor_id = B2.editor_id)
ORDER BY B.price DESC
OFFSET 1 ROWS LIMIT ((SELECT COUNT(*) FROM catalog.Editor) - 2);


-- 7. Для каждого издательства получить название и идентификатор самой дорогой книги,
-- если наибольшая цена соответствует нескольким книгам - выводить книгу с наименьшим идентификатором

-- 7.1 CTE
EXPLAIN ANALYSE
WITH temp_book AS (
    SELECT
        B.book_id,
        E.editor_id
    FROM catalog.Editor AS E
        JOIN catalog.Book AS B ON E.editor_id = B.editor_id
    WHERE B.price = (SELECT MAX(price) FROM catalog.Book AS B2 WHERE B2.editor_id = E.editor_id)
)
SELECT
    E.editor_name,
    b.book_id,
    B.book_name
FROM catalog.Editor AS E
    JOIN catalog.Book AS B ON B.editor_id = E.editor_id
WHERE B.book_id IN (SELECT MIN(T.book_id) FROM temp_book AS T GROUP BY T.editor_id)
ORDER BY B.price;

-- 7.2 Вложенные запросы
EXPLAIN ANALYSE
SELECT
    E.editor_name,
    b.book_id,
    B.book_name
FROM catalog.Editor AS E
    JOIN catalog.Book AS B ON B.editor_id = E.editor_id
WHERE B.book_id = (SELECT MIN(BB.book_id) FROM catalog.Book AS BB
                          WHERE price = (SELECT MAX(B2.price) FROM catalog.Book AS B2 WHERE B2.editor_id = E.editor_id)
                   GROUP BY BB.editor_id)
ORDER BY B.price;
