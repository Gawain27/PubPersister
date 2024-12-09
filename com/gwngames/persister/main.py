import logging
import os
import sys
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from com.gwngames.persister.Context import Context
from com.gwngames.persister.LogFileHandler import LogFileHandler
from com.gwngames.persister.ScraperListener import ScraperListener
from com.gwngames.persister.SynchroSocketServer import SynchroSocketServer
from com.gwngames.persister.utils.JsonReader import JsonReader


class ExcludeFilter(logging.Filter):
    def filter(self, record):
        return not any(
            record.name.startswith(mod) for mod in ('httpx', 'httpcore', 'urllib3', 'selenium'))


if __name__ == '__main__':
    ctx: Context = Context()
    ctx.set_current_dir(os.getcwd())

    DATABASE_URL = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres"
    logging.info(f"Connecting to the database using URL: {DATABASE_URL}")

    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        ctx.set_session_maker(Session)
        logging.info("Session maker has been successfully initialized.")
    except Exception as e:
        logging.error(f"Failed to initialize the session maker: {e}")
        raise

    # Initialize files for caching
    conf_reader = JsonReader(JsonReader.CONFIG_FILE_NAME)
    ctx.set_config(conf_reader)

    # Set up logging
    log_file_handler = LogFileHandler(
        filename="server.log",
        max_lines=10000,
        encoding='utf-8'
    )
    console_handler = logging.StreamHandler(sys.stdout)

    log_format = logging.Formatter('[%(asctime)s - %(thread)d]  %(name)s - %(levelname)s: %(message)s')
    log_file_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)
    log_file_handler.addFilter(ExcludeFilter())
    console_handler.addFilter(ExcludeFilter())

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(level=logging.DEBUG, handlers=[log_file_handler, console_handler])

    logging.info("Logging successfully initialized.")

    # Start scraper
    scraper_server = SynchroSocketServer(
        host="127.0.0.1",
        port=5151,
        handler=ScraperListener.process_listened_message
    )
    scraper_server.start()
    logging.info("Scraper server started.")

    while True:  # Keep child processes alive
        time.sleep(100000)