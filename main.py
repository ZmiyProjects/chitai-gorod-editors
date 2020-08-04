from lxml import html
import requests
import re
from typing import Generator, NamedTuple, Dict, List, Iterable
from threading import Thread


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

    @staticmethod
    def header(delimiter: str = ';'):
        return "product_id{0}product_price{0}book_name{0}authors{0}edition_year{0}editor".format(delimiter)


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
            print(au)
            yield Book(
                p_id, p_price, p_name.replace(';', '').replace('"', ''),
                [a.replace(',', '').strip() for a in au.replace('И др.', '').split(',')], y, e.upper())


def to_file(path: str, values: Iterable[Book], header: str = None, encoding: str = 'utf-8') -> None:
    with open(path, 'a', encoding=encoding) as writer:
        if header is not None:
            writer.write(header)
        writer.writelines(f"\n{line.to_csv_str()}" for line in values)


class Controller:
    def __init__(self, path: str, header: str, encoding: str = 'utf-8'):
        self.processes: List[Thread] = []
        self.path = path
        self.encoding = encoding

        with open(path, 'w', encoding=encoding) as wr:
            wr.write(header)

    def scanner(
            self, url: str, start_page: int, end_page: int,
            headers: Dict[str, str], no_page_exception: bool = False):
        editor = editor_catalog(url, start_page, end_page, headers, no_page_exception=no_page_exception)
        to_file(self.path, editor, encoding=self.encoding)

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


if __name__ == "__main__":
    agent = {
        'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
    }
    eksmo_url = 'https://www.chitai-gorod.ru/books/publishers/eksmo/'
    ast_url = 'https://www.chitai-gorod.ru/books/publishers/ast/'
    rosmen_url = 'https://www.chitai-gorod.ru/books/publishers/rosmen/'
    alpina_pablisher_url = 'https://www.chitai-gorod.ru/books/publishers/alpina_pablisher/'
    azbuka_url = 'https://www.chitai-gorod.ru/books/publishers/azbuka/'

    controller = Controller('editors100.csv', Book.header(";"))
    controller.start(eksmo_url, agent, 1, 1500, 25)
    controller.start(ast_url, agent, 1, 1500, 25)
    controller.start(rosmen_url, agent, 1, 1500, 25)
    controller.start(alpina_pablisher_url, agent, 1, 1500, 25)
    controller.start(azbuka_url, agent, 1, 1500, 25)

    controller.join()
