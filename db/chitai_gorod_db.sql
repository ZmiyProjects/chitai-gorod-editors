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
