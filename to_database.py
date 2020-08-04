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
    with open("editors100.csv", "r", encoding="utf-8") as reader, \
            open("books.csv", "w", encoding="utf-8") as books,\
            open("authors_books.csv", "w", encoding="utf-8") as author_book:
        find_role = r'\([а-я. \-]+\)'
        csv_data = csv.reader(reader, delimiter=';')
        headers = Header(*next(csv_data), 'role')
        editors = set()
        years = set()
        authors = set()
        roles = {"автор"}
        books.write(headers.book_header())
        author_book.write(headers.author_book_header())
        for i in csv_data:
            books.write(f"\n{i[0]};{i[2]};{i[1]};{i[4]};{i[5]}")
            editors.add(i[5])
            years.add(i[4])
            for j in i[3].split(','):
                current_role = 'автор'
                role = re.search(find_role, j)
                if j != '':
                    if role:
                        current_role = re.sub(
                            r'[^а-я]+', '-', role.group(0).replace(' ', '-').strip('()').strip()).strip('-')
                        roles.add(current_role)
                    authors.add(j)
                    author_book.write(f"\n{j};{i[0]};{current_role}")
        print(len(authors))

        to_file("years.csv", years, headers.edition_year)
        to_file("author.csv", authors, headers.authors)
        to_file("editors.csv", editors, headers.editor)
        to_file("roles.csv", roles, headers.role)



