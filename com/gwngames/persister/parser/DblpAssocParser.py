from sqlalchemy import func

from com.gwngames.persister.entity.base.Author import Author
from com.gwngames.persister.entity.base.Conference import Conference
from com.gwngames.persister.entity.base.Journal import Journal
from com.gwngames.persister.entity.base.Publication import Publication
from com.gwngames.persister.entity.base.Relationships import PublicationAuthor, AuthorCoauthor

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from datetime import datetime

class PublicationAssociationProcessor:
    """
    Processes and persists publication data, associating it with authors, journals, and conferences.
    """

    def __init__(self, session: Session):
        self.session = session

    def process_json(self, json_data: dict):
        """
        Processes the provided JSON and persists/updates publication associations.
        """
        try:
            self.session.begin_nested()
            publications = json_data.get("publications", [])
            for pub_data in publications:
                self._process_publication(pub_data, json_data)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise Exception(f"Error processing JSON data: {str(e)}")

    def _process_publication(self, pub_data: dict, metadata: dict):
        """
        Processes and persists a single publication.
        """
        title = pub_data["title"]
        # Truncate the input title to match the length of the database title
        title_length = func.length(Publication.title)
        truncated_title = func.substring(title, 1, title_length)

        publication = (
            self.session.query(Publication)
            .filter(func.word_similarity(Publication.title, truncated_title) > 0.85)
            .with_for_update()
            .first()
        )

        if not publication:
            return  # Skip if no matching publication is found

        publication.update_date = datetime.strptime(metadata["update_date"], "%Y-%m-%d %H:%M:%S")
        publication.update_count = metadata.get("update_count",
                                                publication.update_count + 1 if publication.update_count else 1)

        self._process_authors(pub_data.get("authors", []), publication)

        if pub_data["type"] == "Journal":
            self._process_journal(pub_data, publication)
        elif pub_data["type"] == "Conference":
            self._process_conference(pub_data, publication)

    def _process_authors(self, author_names: list, publication: Publication):
        """
        Processes and associates authors with the publication, and establishes co-author relationships.
        """
        authors = []
        for author_name in author_names:
            # Truncate the input author name to match the length of the database name
            name_length = func.length(Author.name)
            truncated_author_name = func.substring(author_name, 1, name_length)

            # Fetch the author using similarity
            author = (
                self.session.query(Author)
                .filter(func.word_similarity(Author.name, truncated_author_name) > 0.85)
                .with_for_update()
                .first()
            )

            if not author:
                continue  # Skip if no matching author is found

            # Add author to the list for co-author processing
            authors.append(author)

            # Check if the association already exists
            association_exists = (
                self.session.query(PublicationAuthor)
                .filter_by(publication_id=publication.id, author_id=author.id)
                .first()
            )

            if not association_exists:
                # Add the author-publication association
                association = PublicationAuthor(publication_id=publication.id, author_id=author.id)
                self.session.add(association)

        # Establish co-author relationships
        self._process_coauthors(authors)

    def _process_coauthors(self, authors: list):
        """
        Establishes co-author relationships between all authors in the list.
        """
        for i, author in enumerate(authors):
            for j, coauthor in enumerate(authors):
                if i != j:  # Avoid self-referencing
                    association_exists = (
                        self.session.query(AuthorCoauthor)
                        .filter_by(author_id=author.id, coauthor_id=coauthor.id)
                        .first()
                    )

                    if not association_exists:
                        # Add the co-author relationship
                        coauthor_association = AuthorCoauthor(author_id=author.id, coauthor_id=coauthor.id)
                        self.session.add(coauthor_association)

    def _process_journal(self, pub_data: dict, publication: Publication):
        """
        Processes and associates a journal with the publication.
        If no matching journal is found, creates a new one.
        """
        journal_name = pub_data["journal_name"]
        journal_year = pub_data.get("publication_year")

        # Truncate the input journal name to match the length of the database title
        journal_name_length = func.length(Journal.title)
        truncated_journal_name = func.substring(journal_name, 1, journal_name_length)

        # Query using similarity for journal name and exact match for year
        journal = (
            self.session.query(Journal)
            .filter(
                func.word_similarity(Journal.title, truncated_journal_name) > 0.7,
                Journal.year == journal_year
            )
            .with_for_update()
            .first()
        )

        if not journal:
            # Create a new Journal object if no match is found
            journal = Journal(
                title=journal_name,
                year=journal_year
            )
            self.session.add(journal)
            self.session.flush()  # Ensure the new Journal gets an ID for association

        # Create the association
        publication.journal = journal

    def _process_conference(self, pub_data: dict, publication: Publication):
        """
        Processes and associates a conference with the publication.
        If no matching conference is found, creates a new one.
        """
        conference_acronym = pub_data["conference_acronym"]

        # Truncate the input conference acronym to match the length of the database field
        acronym_length = func.length(Conference.acronym)
        truncated_acronym = func.substring(conference_acronym, 1, acronym_length)

        conference = (
            self.session.query(Conference)
            .filter(
                func.word_similarity(Conference.acronym, truncated_acronym) > 0.75
            )
            .with_for_update()
            .first()
        )

        if not conference:
            # Create a new Conference object if no match is found
            conference = Conference(
                acronym=conference_acronym,
            )
            self.session.add(conference)
            self.session.flush()  # Ensure the new Conference gets an ID for association

        # Create the association
        publication.conference = conference


