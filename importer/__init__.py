from pydantic import BaseModel
from rich.console import Console

RICH_CONSOLE = Console()

###############################
# useful validation models
###############################
class PostgresConnectionArgsModel(BaseModel):
    """
    we use this info directly when creating connections from psycopg2 to PostgreSQL database
    """

    database: str
    host: str
    user: str
    password: str
    port: int


class MongoConnectionArgsModel(BaseModel):
    """
    we use this info directly when creating conections from Pymongo to MongoDB Database
    """

    host: str
    username: str
    password: str
    port: int


###############################
# helper CLI prettifiers
###############################
def show_nice_texts_on_process_start_and_end_in_cli(func):
    def wrapper(*args, **kwargs):
        print("\n")

        RICH_CONSOLE.print(" ⏱️  Process started ⏱️  ".center(98, "="), style="bold green")

        print()
        result = func(*args, **kwargs)
        print()

        RICH_CONSOLE.print(" 🎉 All Done! 🎉 ".center(93, "="), style="bold green")
        return result

    return wrapper
