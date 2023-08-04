from IPython.display import display
import graphviz
from pyqrlew import Relation


MAGENTA_COLOR = '\033[35m'
BLUE_COLOR = '\033[34m'
RESET_COLOR = '\033[0m'

def display_relation(relation: Relation) -> None:
    display(graphviz.Source(relation.dot()))


def print_query(query: str) -> None:
    keywords = ["SELECT", "AS", "GROUP BY", "LIMIT", "ORDER BY", "WHERE"]
    colored_query = query
    colored_query = colored_query.replace("WITH", "WITH\n ")
    colored_query = colored_query.replace(" SELECT", "\nSELECT")
    colored_query = colored_query.replace("),", "),\n ")
    for word in keywords:
        colored_query = colored_query.replace(word, MAGENTA_COLOR + word + RESET_COLOR)
    colored_query = colored_query.replace("WITH", BLUE_COLOR + "WITH" + RESET_COLOR)
    print(colored_query)
