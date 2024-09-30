import tempfile
import os
import platform
import subprocess
import typing as t
from .pyqrlew import Dialect

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


def display_graph(dot_string):
    # This is an optional dependency
    from graphviz import Source  #type: ignore

    # Create a temporary file for the SVG
    with tempfile.NamedTemporaryFile(delete=False, suffix='.svg') as temp:
        # Create a Source object from the DOT string
        src = Source(dot_string)
        
        # Render the source to a temporary SVG file
        svg_path = src.render(filename=temp.name, format='svg', cleanup=False)

    # Create a temporary HTML file to embed the SVG
    html_path = f"{temp.name}.html"
    with open(html_path, 'w') as html_file:
        # Use absolute path for SVG in the HTML to ensure it can be loaded
        html_content = f"""
        <html>
            <head>
                <style>
                    body, html {{
                        height: 100%;
                        margin: 0;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                    }}
                </style>
            </head>
            <body>
                <img src="file://{svg_path}" alt="Graph" style="max-width: 100%; max-height: 100%;"/>
            </body>
        </html>
        """
        html_file.write(html_content)
    
    # Open the rendered HTML file in the default system browser
    try:
        if platform.system() == 'Darwin':       # macOS
            subprocess.run(['open', html_path])
        elif platform.system() == 'Windows':   # Windows
            os.startfile(html_path)
        else:                                   # Linux variants
            subprocess.run(['xdg-open', html_path])
    finally:
        pass

def tables_prefix(query: str, dialect: Dialect) -> t.List[str]:
    """Util to extract the prefix of fully qualified table names from a query.
    Only base tables are considered. If table names are quoted, the quoting
    must be coherent with the dialect.

    Args:
        query (str):
        dialect (Dialect): 

    Returns:
        t.List[str]: prefix of table names
    """
    from pyqrlew.pyqrlew import tables_prefix
    return tables_prefix(query, dialect)