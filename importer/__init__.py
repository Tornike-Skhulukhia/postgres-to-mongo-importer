import time
from typing import Optional

from pydantic import BaseModel, Extra
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
    schema_name: Optional[str] = "public"

    class Config:
        extra = Extra.forbid


class MongoConnectionArgsModel(BaseModel):
    """
    we use this info directly when creating conections from Pymongo to MongoDB Database
    """

    host: str
    username: str
    password: str
    port: int

    class Config:
        extra = Extra.forbid


###############################
# helper CLI prettifiers
###############################
def show_nice_texts_on_process_start_and_end_in_cli(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        print("\n")

        RICH_CONSOLE.print(" ⏱️  Process started ⏱️  ".center(98, "="), style="bold green")

        print()
        result = func(*args, **kwargs)
        print()

        process_took = time.time() - start_time

        RICH_CONSOLE.print(
            f" 🎉 All Done in {process_took:^.3f} seconds ! 🎉 ".center(93, "="), style="bold green"
        )
        return result

    return wrapper
