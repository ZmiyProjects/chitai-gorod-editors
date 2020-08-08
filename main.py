from lxml import html
import requests
import re
from typing import Generator, NamedTuple, Dict, List, Iterable, Union
from threading import Thread
from datetime import datetime
import os


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


class Book(NamedTuple):
    product_id: int
    product_price: int
    book_name: str
    authors: List[str]
    edition_year: int
    editor: str

    def __hash__(self):
        return id(self)

    def to_csv_str(self, delimiter: str = ';', list_delimiter: str = ','):
        return f"{self.product_id}{delimiter}{self.product_price}{delimiter}{self.book_name}{delimiter}" \
               f"{f'{list_delimiter}'.join(self.authors)}{delimiter}{self.edition_year}{delimiter}{self.editor}"

    def book_str(self, sep: str = ';'):
        return "{1}{0}{2}{0}{3}{0}{4}{0}{5}".format(
            sep, self.product_id, self.book_name, self.product_price, self.edition_year, self.editor)

    @staticmethod
    def header(delimiter: str = ';'):
        return "book_id{0}price{0}book_name{0}authors{0}edition_year{0}editor_name".format(delimiter)

    @staticmethod
    def book_header(sep: str = ';'):
        return "{1}{0}{2}{0}{3}{0}{4}{0}{5}". \
            format(sep, "book_id", "book_name", "price", "edition_year", "editor_name")

    @staticmethod
    def author_book_header(sep: str = ';'):
        return "{1}{0}{2}{0}{3}".format(sep, "author_name", "book_id", "role_name")


def editor_catalog(
        url: str, start_page: int, end_page: int,
        headers: Dict[str, str], no_page_exception: bool = False) -> Generator[Book, None, None]:
    if start_page > end_page or 0 > start_page or 0 > end_page:
        raise ValueError
    for current_page in range(start_page, end_page + 1):
        try:
            page = requests.get(f"{url}?page={current_page}", headers=headers)
            if page.status_code in {403, 404}:
                if no_page_exception:
                    raise Exception
                else:
                    break
        except:
            break
        tree = html.fromstring(page.content)
        product_id = tree.xpath(
            "//div[@class='product-card js_product js__product_card js__slider_item']/@data-product")
        product_price = tree.xpath(
            "//div[@class='product-card js_product js__product_card js__slider_item']/@data-productprice")
        product_names = tree.xpath("//div[@class='img-product-block']/a/img/@title")
        author_path = "//div[@class='product-card__author']/node()"
        authors = [re.sub(r'[\t\n]+', '', cur_author).replace(' и др.', '') for cur_author in tree.xpath(author_path)]
        years = tree.xpath("//span[@class='publisher']/span[position() = 2]/text()")[1::2]
        editors = tree.xpath("//span[@class='publisher']/span[position() = 2]/text()")[0::2]
        for p_id, p_price, p_name, au, y, e in zip(product_id, product_price, product_names, authors, years, editors):
            if re.match('[0-9]{4}', y) is None or re.match('[А-Яа-я ]+', e) is None:
                continue
            if int(y) < 1900:
                continue
            yield Book(
                p_id, p_price, p_name.replace(';', '').replace('"', ''),
                [a.replace(',', '').strip() for a in au.replace('И др.', '').split(',')], y, e.upper())


class Controller:
    def __init__(self, encoding: str = 'utf-8'):
        self.processes: List[Thread] = []
        self.encoding = encoding
        self.values: List[str] = []
        self.editors = set()
        self.years = set()
        self.authors = set()
        self.books = set()
        self.books_authors = set()
        self.roles = {"автор"}

    def scanner(self, url: str, start_page: int, end_page: int,
                headers: Dict[str, str], no_page_exception: bool = False):
        editor = editor_catalog(url, start_page, end_page, headers, no_page_exception=no_page_exception)
        find_role = r'\([а-я. \-]+\)'
        for ed in editor:
            self.books.add(ed.book_str())
            self.values.append(ed.to_csv_str())
            self.editors.add(ed.editor)
            self.years.add(int(ed.edition_year))
            for j in ed.authors:
                author = j
                current_role = 'автор'
                role = re.search(find_role, j)
                if j != '':
                    if role:
                        current_role = re.sub(
                            r'[^а-я]+', '-', role.group(0).replace(' ', '-').strip('()').strip()).strip('-')
                        self.roles.add(current_role)
                        author = re.sub(role.group(0), '', author)
                    author = re.sub(r'[^А-яA-Zа-яa-z ]', '', author).strip() + '.'
                    self.authors.add(author)
                    self.books_authors.add(f"{author};{ed.product_id};{current_role}")

    def start(self, url: str, headers: Dict[str, str], start_page: int, end_page: int,
              process_pages: int, no_page_exception: bool = False):
        cur_start_page = start_page
        cur_end_page = (start_page + process_pages)
        while cur_start_page < end_page:
            self.processes.append(Thread(target=self.scanner, args=(url, cur_start_page, cur_end_page, headers,)))
            self.processes[-1].start()
            cur_start_page += process_pages + 1
            cur_end_page += process_pages + 1

    def join(self):
        for p in self.processes:
            p.join()

    def to_file(self):
        for year in range(min(self.years), max(self.years)):
            if year not in self.years:
                self.years.add(year)
        dir_name = datetime.strftime(datetime.now(), "%d.%m.%Y_%H-%M-%S")
        os.mkdir(dir_name)

        to_file(dir_name + "/chitai_gorod_catalog.csv", self.values, Book.header())
        to_file(dir_name + "/books.csv", self.books, Book.book_header())
        to_file(dir_name + "/years.csv", self.years, "edition_year")
        to_file(dir_name + "/authors.csv", self.authors, "author_name")
        to_file(dir_name + "/editors.csv", self.editors, "editor_name")
        to_file(dir_name + "/roles.csv", self.roles, "role_name")
        to_file(dir_name + "/authors_books.csv", self.books_authors, Book.author_book_header())


if __name__ == "__main__":
    agent = {
        'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
    }
    eksmo_url = 'https://www.chitai-gorod.ru/books/publishers/eksmo/'
    ast_url = 'https://www.chitai-gorod.ru/books/publishers/ast/'
    rosmen_url = 'https://www.chitai-gorod.ru/books/publishers/rosmen/'
    alpina_pablisher_url = 'https://www.chitai-gorod.ru/books/publishers/alpina_pablisher/'
    azbuka_url = 'https://www.chitai-gorod.ru/books/publishers/azbuka/'

    controller = Controller('chitai_gorod_catalog.csv')
    controller.start(eksmo_url, agent, 1, 1500, 25)
    controller.start(ast_url, agent, 1, 1500, 25)
    controller.start(rosmen_url, agent, 1, 1500, 25)
    controller.start(alpina_pablisher_url, agent, 1, 1500, 25)
    controller.start(azbuka_url, agent, 1, 1500, 25)

    controller.join()

    controller.to_file()
