from lxml import html
import requests
import re
from typing import Generator, NamedTuple, Dict, List, Iterable, Set
from multiprocessing import Process
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
        page = requests.get(f"{url}?page={current_page}", headers=headers)
        if page.status_code in {403, 404}:
            if no_page_exception:
                raise Exception
            else:
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
        for p_id, p_price, p_name, a, y, e in zip(product_id, product_price, product_names, authors, years, editors):
            yield Book(p_id, p_price, p_name.replace(';', '').replace('"', ''), a.split(', '), y, e)


def to_file(path: str, values: Iterable[Book], header: str = None, encoding: str = 'utf-8') -> None:
    with open(path, 'a', encoding=encoding) as writer:
        if header is not None:
            writer.write(header)
        writer.writelines(f"\n{line.to_csv_str()}" for line in values)


def get_write(
        url: str, start_page: int, end_page: int,
        headers: Dict[str, str], path: str,
        header: str = None, encoding: str = 'utf-8', no_page_exception: bool = False) -> None:
    editor = editor_catalog(url, start_page, end_page, headers, no_page_exception=no_page_exception)
    to_file(path, editor, header, encoding=encoding)


def processes_to_file(
        url: str, headers: Dict[str, str], path: str,
        start_page: int, end_page: int, process_pages: int,
        encoding: str = 'utf-8', no_page_exception: bool = False) -> List[Process]:
    process_list: List[Process] = []
    cur_start_page = start_page
    cur_end_page = (start_page + process_pages)
    while cur_start_page < end_page:
        process_list.append(Process(target=get_write, args=(url, cur_start_page, cur_end_page, headers, path,)))
        process_list[-1].start()
        cur_start_page += process_pages + 1
        cur_end_page += process_pages + 1
    return process_list


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

    # controller.to_csv('editors100.csv', Book.header())

    # eksmo = editor_catalog(eksmo_url, 1, 1, agent)
    # ast = editor_catalog(ast_url, 1, 1, agent)

    # to_file('editors.csv', eksmo, Book.header())
    # to_file('editors.csv', ast)

    """

    processes: List[List[Process]] = []

    with open('editors40.csv', 'w', encoding='utf-8') as wr:
        wr.write(Book.header(delimiter=';'))

    processes.append(processes_to_file(eksmo_url, agent, 'editors40.csv', 1, 500, 500))
    processes.append(processes_to_file(ast_url, agent, 'editors40.csv', 1, 500, 500))
    processes.append(processes_to_file(rosmen_url, agent, 'editors40.csv', 1, 500, 500))
    processes.append(processes_to_file(alpina_pablisher_url, agent, 'editors40.csv', 1, 500, 500))
    processes.append(processes_to_file(azbuka_url, agent, 'editors40.csv', 1, 500, 500))

    for p1 in processes:
        for p2 in p1:
            p2.join()

    """
    """
    pages = 500

    with open('editors4.csv', 'w', encoding='utf-8') as wr:
        wr.write(Book.header(delimiter=';'))

    eksmo = Process(target=get_write, args=(eksmo_url, 1, pages, agent, 'editors4.csv',))
    eksmo_2 = Process(target=get_write, args=(eksmo_url, pages + 1, pages + 100, agent, 'editors4.csv',))

    ast = Process(target=get_write, args=(ast_url, 1, pages, agent, 'editors4.csv',))
    ast_2 = Process(target=get_write, args=(ast_url, pages + 1, pages + 100, agent, 'editors4.csv',))

    rosmen = Process(target=get_write, args=(rosmen_url, 1, pages, agent, 'editors4.csv',))
    alpina_pablisher = Process(target=get_write, args=(alpina_pablisher_url, 1, pages, agent, 'editors4.csv',))
    azbuka = Process(target=get_write, args=(azbuka_url, 1, pages, agent, 'editors4.csv',))

    eksmo.start()
    eksmo_2.start()
    ast.start()
    ast_2.start()
    rosmen.start()
    alpina_pablisher.start()
    azbuka.start()

    eksmo.join()
    eksmo_2.join()
    ast.join()
    ast_2.join()
    rosmen.join()
    alpina_pablisher.join()
    azbuka.join()
    """

    # get_write(eksmo_url, 1, 25, agent, 'editors4.csv', no_page_exception=True)
    # get_write(ast_url, 1, 25, agent, 'editors4.csv', no_page_exception=True)
