-- Примеры запросов к БД
-- 1. Получить названия и идентификатры книг изданных в прошлом году относительно текущей даты
SELECT
    B.book_name,
    B.book_id
FROM catalog.Book AS B
WHERE B.edition_year = EXTRACT(YEAR FROM CURRENT_DATE) - 1;

-- 1. Получить годы, в которые не были издана ни одна книга из выборки
SELECT edition_year FROM catalog.EditionYear
WHERE edition_year NOT IN (SELECT DISTINCT edition_year FROM catalog.Book);

-- 2. Получить идентификаторы книг, написанных в соавторстве
SELECT
    B.book_id
FROM catalog.Book AS B
    JOIN catalog.AuthorBook AS AB ON B.book_id = AB.book_id
GROUP BY B.book_id
HAVING count(AB.author_id) >= 2
ORDER BY COUNT(AB.author_id) DESC;

-- 3. Получить сведения о книгах и лиц, участвовавших в их оформлении, чья роль отлична от "автор"
SELECT
    A.author_name,
    B.book_name,
    AR.role_name
FROM catalog.AuthorBook AS AB
    JOIN catalog.Author AS A ON A.author_id = AB.author_id
    JOIN catalog.Book AS B ON B.book_id = AB.book_id
    JOIN catalog.AuthorRole AS AR ON AR.role_id = AB.role_id
WHERE AR.role_name <> 'автор';

-- 4. Получить издательства, выпустившие более 10000 книг с 2010 по 2020 годы включительно
SELECT
    E.editor_name
FROM catalog.Editor AS E
    JOIN catalog.Book AS B ON E.editor_id = B.editor_id
WHERE B.edition_year BETWEEN 2010 AND 2020
GROUP BY E.editor_name
HAVING COUNT(*) > 10000;

-- 5. Получить идентификатор и наименование для книг, в кажестве автора которых указано издательство
SELECT
    B.book_id,
    B.book_name
FROM catalog.AuthorBook AS AB
    JOIN catalog.Author AS A ON AB.author_id = A.author_id
    JOIN catalog.Book AS B ON AB.book_id = B.book_id
WHERE replace(A.author_name, '.', '') IN (SELECT editor_name FROM catalog.Editor);

-- 6. Получить количество книг, у которых не указан автор
SELECT COUNT(*) FROM catalog.Book AS B
     WHERE B.book_id NOT IN (SELECT book_id FROM catalog.AuthorBook);

-- 7. Получить идентификатор, название и год издания самой дорогой книги из каталога
SELECT
    B.book_id,
    B.book_name,
    B.edition_year
FROM catalog.Book AS B
WHERE B.price = (SELECT MAX(price) FROM catalog.Book);

-- 8. Получите перечень издательство - цена самой дорогой книги в рамках издательства,
-- исключив издательства с наибольшей и наименьшей стоимостью самых дорогих книг среди издателей
SELECT DISTINCT
    E.editor_name,
    B.price
FROM catalog.Editor AS E
    JOIN catalog.Book AS B ON B.editor_id = E.editor_id
WHERE B.price = (SELECT MAX(price) FROM catalog.Book AS B2 WHERE E.editor_id = B2.editor_id)
ORDER BY B.price DESC
OFFSET 1 ROWS LIMIT ((SELECT COUNT(*) FROM catalog.Editor) - 2);


-- 9. Для каждого издательства получить название и идентификатор самой дорогой книги,
-- если наибольшая цена соответствует нескольким книгам - выводить книгу с наименьшим идентификатором

-- 9.1 DISTINCT ON
EXPLAIN ANALYSE
SELECT DISTINCT ON (E.editor_name)
    E.editor_name,
    B.book_id,
    B.book_name
FROM catalog.Book AS B
    JOIN catalog.Editor AS E ON E.editor_id = B.editor_id
WHERE B.price IN (SELECT MAX(price) FROM catalog.Book AS B2 GROUP BY B2.editor_id)
ORDER BY E.editor_name, book_id;

-- 9.2 CTE
-- 9.2.1
EXPLAIN ANALYSE
WITH temp_book AS (
    SELECT
        B.book_id,
        B.book_name,
        B.editor_id
    FROM catalog.Book AS B
    WHERE B.price IN (SELECT MAX(price) FROM catalog.Book AS B2 GROUP BY B2.editor_id)
)
SELECT
    E.editor_name,
    b.book_id,
    B.book_name
FROM catalog.Editor AS E
    JOIN catalog.Book AS B ON B.editor_id = E.editor_id
WHERE B.book_id IN (SELECT MIN(T.book_id) FROM temp_book AS T GROUP BY T.editor_id)
ORDER BY E.editor_name, B.book_id;

-- 9.2.2
EXPLAIN ANALYSE
WITH temp_book AS (
    SELECT
        B.book_id,
        B.book_name,
        B.editor_id
    FROM catalog.Book AS B
    WHERE B.price IN (SELECT MAX(price) FROM catalog.Book AS B2 GROUP BY B2.editor_id)
)
SELECT
    E.editor_name,
    (SELECT MIN(book_id) FROM temp_book AS T WHERE E.editor_id = T.editor_id),
    (SELECT book_name FROM temp_book AS T WHERE T.book_id = (SELECT MIN(book_id) FROM temp_book AS T WHERE E.editor_id = T.editor_id))
FROM catalog.Editor AS E;

-- 9.3 Оконные функции + CTE
EXPLAIN ANALYSE
WITH temp AS (
    SELECT B.book_id, B.book_name, B.editor_id, row_number() over (PARTITION BY B.editor_id ORDER BY B.book_id) AS num
    FROM catalog.Book AS B
    GROUP BY B.book_id, book_name
    HAVING MAX(B.price) IN (SELECT MAX(B2.price) FROM catalog.Book AS B2 GROUP BY B2.editor_id)
)
SELECT
    E.editor_name,
    T.book_id,
    T.book_name
FROM temp AS T
    JOIN catalog.Editor AS E ON E.editor_id = T.editor_id
WHERE T.num = 1
ORDER BY E.editor_name;

-- 9.4 JOIN LATERAL
EXPLAIN ANALYSE
SELECT
    E.editor_name,
    B.book_id,
    B.book_name
FROM catalog.Editor AS E
    JOIN LATERAL (
            SELECT
                B.book_id, B.book_name
            FROM catalog.Book AS B WHERE B.price = (SELECT MAX(price) FROM catalog.Book AS B2 WHERE B2.editor_id = E.editor_id)
            ORDER BY B.book_id
            FETCH FIRST ROW ONLY) As B ON true;
