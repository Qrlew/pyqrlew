MAGENTA_COLOR = '\033[35m'
BLUE_COLOR = '\033[34m'
RESET_COLOR = '\033[0m'


def print_query(query: str) -> None:
    """
    Print a formatted and syntax-highlighted SQL query.

    This function takes an SQL query as input and prints it to the console with syntax
    highlighting applied to certain keywords for better readability.

    Parameters
    ----------
    query: str
        The SQL query to be printed and highlighted.

    Returns
    -------
    None
        This function does not return any value. It directly prints the formatted
        and highlighted SQL query to the console.
    """
    keywords = ["SELECT", "AS", "GROUP BY", "LIMIT", "ORDER BY", "WHERE"]
    colored_query = query
    colored_query = colored_query.replace("WITH", "WITH\n ")
    colored_query = colored_query.replace(" SELECT", "\nSELECT")
    colored_query = colored_query.replace("),", "),\n ")
    for word in keywords:
        colored_query = colored_query.replace(word, MAGENTA_COLOR + word + RESET_COLOR)
    colored_query = colored_query.replace("WITH", BLUE_COLOR + "WITH" + RESET_COLOR)
    print(colored_query)
