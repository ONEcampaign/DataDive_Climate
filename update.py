"""Update page charts"""

from scripts.charts import update_charts
from csv import writer
from scripts import config
import datetime


def log_update() -> None:
    """Log latest update in output folder"""

    with open(config.paths.output + r"/updates.csv", "a+", newline="") as file:
        # Create a writer object from csv module
        csv_writer = writer(file)
        # Add contents of list as last row in the csv file
        csv_writer.writerow([datetime.datetime.today()])


if __name__ == "__main__":

    update_charts()  # update charts
    log_update()  # Log update


