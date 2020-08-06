import csv
from typing import Iterable, Union, List, NamedTuple
import re


class Header(NamedTuple):
    product_id: str
    product_price: str
    book_name: str
    authors: str
    edition_year: str
    editor: str
    role: str

    def book_header(self, sep: str = ';'):
        return "{1}{0}{2}{0}{3}{0}{4}{0}{5}". \
            format(sep, self.product_id, self.book_name, self.product_price, self.edition_year, self.editor)

    def author_book_header(self, sep: str = ';'):
        return "{1}{0}{2}{0}{3}".format(sep, self.authors, self.product_id, self.role)


def to_file(path: str, data: Iterable, header: Union[None, str, List[str]] = None,
            encoding: str = 'utf-8', sep: str = ';'):
    with open(path, 'w', encoding=encoding) as wr:
        if header is not None:
            if isinstance(header, str):
                wr.write(header)
            elif isinstance(header, list):
                wr.write(f"{sep}".join(header))
            else:
                raise ValueError
        wr.writelines(f"\n{line}" for line in data)


if __name__ == "__main__":
    with open("data/chitai_gorod_catalog.csv", "r", encoding="utf-8") as reader, \
            open("data/books.csv", "w", encoding="utf-8") as books:
        find_role = r'\([а-я. \-]+\)'
        csv_data = csv.reader(reader, delimiter=';')
        headers = Header(*next(csv_data), 'role')
        editors = set()
        years = set()
        authors = set()
        books_authors = set()
        roles = {"автор"}
        books.write(headers.book_header())
        # author_book.write(headers.author_book_header())
        for i in csv_data:
            books.write(f"\n{i[0]};{i[2]};{i[1]};{i[4]};{i[5]}")
            editors.add(i[5])
            years.add(int(i[4]))
            for j in i[3].split(','):
                author = j
                current_role = 'автор'
                role = re.search(find_role, j)
                if j != '':
                    if role:
                        current_role = re.sub(
                            r'[^а-я]+', '-', role.group(0).replace(' ', '-').strip('()').strip()).strip('-')
                        roles.add(current_role)
                        author = re.sub(role.group(0), '', author)
                    author = re.sub(r'[^А-яA-Zа-яa-z ]', '', author).strip() + '.'
                    authors.add(author)
                    # author_book.write(f"\n{author};{i[0]};{current_role}")
                    books_authors.add(f"{author};{i[0]};{current_role}")

        for year in range(min(years), max(years)):
            if year not in years:
                years.add(year)

        to_file("data/years.csv", years, headers.edition_year)
        to_file("data/authors.csv", authors, headers.authors)
        to_file("data/editors.csv", editors, headers.editor)
        to_file("data/roles.csv", roles, headers.role)
        to_file("data/authors_books.csv", books_authors, headers.author_book_header())



