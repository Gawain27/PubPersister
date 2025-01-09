from com.gwngames.persister.entity.base.Author import Author
from com.gwngames.persister.entity.base.Conference import Conference
from com.gwngames.persister.entity.base.Journal import Journal
from com.gwngames.persister.entity.base.Publication import Publication
from com.gwngames.persister.entity.base.Relationships import PublicationAuthor, AuthorCoauthor
from com.gwngames.persister.utils.StringUtils import StringUtils


import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func, desc

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class PublicationAssociationProcessor:
    """
    Processes and persists publication data, associating it with authors, journals, and conferences.
    """

    def __init__(self, session):
        self.session = session

    def process_json(self, json_data: dict):
        """
        Processes the provided JSON and persists/updates publication associations.
        """
        json_id = json_data.get("_id", "unknown_id")

        try:
            self.session.begin_nested()
            publications = json_data.get("publications", [])
            for pub_data in publications:
                logging.info(f"Processing publication: {pub_data.get('title', 'unknown_title')} in JSON ID: {json_id}")
                self._process_publication(pub_data)
            self.session.commit()
            logging.info(f"Successfully processed JSON with ID: {json_id}")
        except Exception as e:
            self.session.rollback()
            raise Exception(f"Error processing JSON data: {str(e)}")
        finally:
            self.session.close()

    def _process_publication(self, pub_data: dict):
        """
        Processes and persists a single publication.
        """
        title = pub_data.get("title", "unknown_title").lower()

        try:
            first_word = StringUtils.first_after_fifth(title)

            publication = (
                self.session.query(Publication)
                .filter(Publication.title.like(f"%{first_word}%"))
                .filter(func.jaro_winkler_similarity(Publication.title, title) >= 0.87)
                .order_by(desc(func.jaro_winkler_similarity(Publication.title, title)))
                .first()
            )

            if not publication:
                logging.warning(f"No matching publication found for title: {title}")
                return

            self._process_authors(pub_data.get("authors", []), publication)

            if pub_data["type"] == "Journal":
                self._process_journal(pub_data, publication)
            elif pub_data["type"] == "Conference":
                self._process_conference(pub_data, publication)
        except SQLAlchemyError as e:
            logging.error(f"SQLAlchemy error while processing publication {title}: {str(e)}")
            raise

    def _process_authors(self, author_names: list, publication: Publication):
        """
        Processes and associates authors with the publication, and establishes co-author relationships.
        """
        authors = []

        for author_name in author_names:
            try:
                author_name = author_name.lower()
                surname = author_name.split(" ")[-1]

                author = (
                    self.session.query(Author)
                    .filter(Author.name.like(f"%{surname}"))
                    .filter(Author.name.like(f"{author_name[:1]}%"))
                    .filter(func.jaro_winkler_similarity(Author.name, author_name) >= 0.7)
                    .order_by(desc(func.jaro_winkler_similarity(Author.name, author_name)))
                    .first()
                )

                if not author:
                    logging.warning(f"No matching author found for name: {author_name} - {publication.title}")
                    continue

                authors.append(author)

                association_exists = (
                    self.session.query(PublicationAuthor)
                    .filter_by(publication_id=publication.id, author_id=author.id)
                    .first()
                )

                if not association_exists:
                    association = PublicationAuthor(publication_id=publication.id, author_id=author.id)
                    self.session.add(association)
            except SQLAlchemyError as e:
                logging.error(f"Error processing author {author_name}: {str(e)}")
                raise

        if authors:
            logging.info(f"Processed {len(authors)} authors for publication ID: {publication.id}")
        self._process_coauthors(authors)

    def _process_coauthors(self, authors: list):
        """
        Establishes co-author relationships between all authors in the list.
        """
        for i, author in enumerate(authors):
            for j, coauthor in enumerate(authors):
                if i != j:
                    association_exists = (
                        self.session.query(AuthorCoauthor)
                        .filter_by(author_id=author.id, coauthor_id=coauthor.id)
                        .first()
                    )

                    if not association_exists:
                        coauthor_association = AuthorCoauthor(author_id=author.id, coauthor_id=coauthor.id)
                        self.session.add(coauthor_association)

    def _process_journal(self, pub_data: dict, publication: Publication):
        """
        Processes and associates a journal with the publication.
        """
        journal_name = pub_data.get("journal_name", "unknown_journal").lower()
        journal_year = pub_data.get("publication_year")

        try:
            first_word = StringUtils.first_after_fifth(journal_name)

            journal = (
                self.session.query(Journal)
                .filter(Journal.title.like(f"%{first_word}%"))
                .filter(func.jaro_similarity(Journal.title, journal_name) >= 0.8)
                .order_by(desc(func.jaro_similarity(Journal.title, journal_name)))
                .first()
            )

            if not journal:
                journal = Journal(
                    title=journal_name,
                    year=journal_year,
                    type="journal",
                    q_rank="N/A",
                    class_id=Journal.CLASS_ID,
                    variant_id=Journal.VARIANT_ID
                )
                self.session.add(journal)
                logging.info(f"Created new journal: {journal_name} with ID: {journal.id}")

            publication.journal_id = journal.id
            publication.journal = journal
            self.session.flush()
        except SQLAlchemyError as e:
            logging.error(f"Error processing journal {journal_name}: {str(e)}")
            raise

    def _process_conference(self, pub_data: dict, publication: Publication):
        """
        Processes and associates a conference with the publication.
        """
        acronym = pub_data.get("conference_acronym", "unknown_conference").upper()

        try:
            # Helper function to find a conference based on an acronym
            def find_conference(acronym_part):
                return (
                    self.session.query(Conference)
                    .filter(func.jaro_winkler_similarity(Conference.acronym, acronym_part) >= 0.94)
                    .order_by(desc(func.jaro_winkler_similarity(Conference.acronym, acronym_part)))
                    .first()
                )

            conference = find_conference(acronym)
            candidates = set()

            if not conference and "@" in acronym:
                logging.info(f"Processing acronym '{acronym}' with @ splitting")
                parts = acronym.split("@")
                if len(parts) > 1:
                    for part in parts:
                        conference = find_conference(part)
                        if conference:
                            break
                        candidates.add(part)

            if not conference and "/" in acronym:
                logging.info(f"Processing acronym '{acronym}' with / splitting")
                parts = acronym.split("/")
                if len(parts) > 1:
                    for part in parts:
                        conference = find_conference(part)
                        if conference:
                            break
                        candidates.add(part)

            if not conference and "-" in acronym:
                logging.info(f"Processing acronym '{acronym}' with - filtering")
                parts = acronym.split("-")
                if len(parts) > 1:
                    for part in parts:
                        conference = find_conference(part)
                        if conference:
                            break
                        candidates.add(part)

            if not conference:
                acronym = acronym if len(candidates) == 0 else candidates.pop()
                conference = Conference(
                    acronym=acronym,
                    class_id=Conference.CLASS_ID,
                    rank="Unranked",
                    variant_id=Conference.VARIANT_ID
                )
                self.session.add(conference)
                logging.info(f"Created new conference: {acronym} with ID: {conference.id}")

            publication.conference_id = conference.id
            publication.conference = conference
            logging.info(f"Associated conference {conference.acronym} with publication ID: {publication.id}")
            self.session.flush()
        except SQLAlchemyError as e:
            logging.error(f"Error processing conference {acronym}: {str(e)}")
            raise




