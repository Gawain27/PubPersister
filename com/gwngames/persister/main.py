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

    # Initialize config file
    conf_reader = JsonReader(JsonReader.CONFIG_FILE_NAME)
    ctx.set_config(conf_reader)

    db_url = conf_reader.get_value("db_url")
    db_name = conf_reader.get_value("db_name")
    db_user = conf_reader.get_value("db_user")
    db_password = conf_reader.get_value("db_password")
    db_port = conf_reader.get_value("db_port")

    DATABASE_URL = f"postgresql+psycopg2://{db_user}:{db_password}@{db_url}:{db_port}/{db_name}"
    logging.info(f"Connecting to the database using URL: {DATABASE_URL}")

    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        ctx.set_session_maker(Session)
        logging.info("Session maker has been successfully initialized.")
    except Exception as e:
        logging.error(f"Failed to initialize the session maker: {e}")
        raise

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
        host="0.0.0.0",
        port=5151,
        handler=ScraperListener.process_listened_message
    )
    scraper_server.start()
    logging.info("Scraper server started.")

    while True:  # Keep child processes alive
        time.sleep(100000)