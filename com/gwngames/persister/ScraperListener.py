import json
import logging
import threading
import time

from com.gwngames.persister.Context import Context
from com.gwngames.persister.entity.base.Author import Author
from com.gwngames.persister.entity.base.Conference import Conference
from com.gwngames.persister.entity.base.Journal import Journal
from com.gwngames.persister.entity.base.Publication import Publication
from com.gwngames.persister.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor
from com.gwngames.persister.entity.variant.scholar.GoogleScholarCitation import GoogleScholarCitation
from com.gwngames.persister.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication
from com.gwngames.persister.parser.ConferenceParser import ConferenceProcessor
from com.gwngames.persister.parser.DblpAssocParser import PublicationAssociationProcessor
from com.gwngames.persister.parser.JournalParser import JournalParser
from com.gwngames.persister.parser.ScholarAuthorParser import ScholarAuthorParser
from com.gwngames.persister.parser.ScholarCitationParser import ScholarCitationParser
from com.gwngames.persister.parser.ScholarPublicationParser import ScholarPublicationParser
from com.gwngames.persister.utils.JsonReader import JsonReader


class ScraperListener(object):
    deadlock_lock = threading.Lock()

    @staticmethod
    def process_listened_message(message):
        dict_message = json.loads(message)
        max_retries = Context().get_config().get_value("max_retries")
        delay = Context().get_config().get_value("delay_secs")
        attempts = 0

        while attempts < max_retries:
            try:
                with ScraperListener.deadlock_lock:
                    class_id = dict_message['class_id']
                    variant_id = dict_message['variant_id']
                    session = Context().get_session()

                    if variant_id == GoogleScholarAuthor.VARIANT_ID and class_id == Author.CLASS_ID:
                        ScholarAuthorParser(session).process_google_scholar_data(dict_message)
                    elif variant_id == GoogleScholarPublication.VARIANT_ID and class_id == Publication.CLASS_ID:
                        ScholarPublicationParser(session).process_json(dict_message)
                    elif variant_id == Conference.VARIANT_ID and class_id == Conference.CLASS_ID:
                        ConferenceProcessor(session).process_json(dict_message)
                    elif variant_id == Journal.VARIANT_ID and class_id == Journal.CLASS_ID:
                        JournalParser(session).process_json(dict_message)
                    elif variant_id == 100 and class_id == Publication.CLASS_ID:
                        PublicationAssociationProcessor(session).process_json(dict_message)
                    elif variant_id == GoogleScholarCitation.VARIANT_ID and class_id == GoogleScholarCitation.CLASS_ID:
                        ScholarCitationParser(session).process_json(dict_message)
                    else:
                        logging.warning("Invalid message: " + str(message))
                    return  # Exit loop on success
            except Exception as e:
                logging.error(f"Attempt {attempts + 1} failed for message: {message}")
                logging.error(e)
                if attempts == max_retries - 1:
                    logging.error("Max retries reached. Aborting.")
                    if dict_message is not None:
                        JsonReader("persister.errors").set_and_save(str(dict_message['_id']), str(e))
                    break
                time.sleep(delay)
                attempts += 1

