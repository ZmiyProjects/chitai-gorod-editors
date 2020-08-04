import csv
import re


if __name__ == "__main__":
    redex = r'\([а-я. \-]+\)'
    roles = set()
    with open("authors_books.csv", "r", encoding="utf-8") as reader:
        csv_data = csv.reader(reader, delimiter=';')
        next(csv_data)
        for i in csv_data:
            role = re.search(r'\(.+\)', str(i[0]))
            if role:
                # print(i, role.group(0))
                roles.add(role.group(0))
                if role.group(0) == "(Алфеев)":
                    print(i)
    for i in roles:
        print(i)


