from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from com.gwngames.persister.entity.base.Author import Author
from com.gwngames.persister.entity.base.Conference import Conference
from com.gwngames.persister.entity.base.Journal import Journal
from com.gwngames.persister.entity.base.Publication import Publication
from com.gwngames.persister.entity.base.Relationships import PublicationAuthor


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
            # Begin a nested transaction for pessimistic locking
            self.session.begin_nested()

            # Get the list of publications
            publications = json_data.get("publications", [])

            # Process each publication
            for pub_data in publications:
                self._process_publication(pub_data, json_data)

            # Commit the changes
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise Exception(f"Error processing JSON data: {str(e)}")

    def _process_publication(self, pub_data: dict, metadata: dict):
        """
        Processes and persists a single publication.
        """
        title = pub_data["title"]
        publication = (
            self.session.query(Publication)
            .filter(Publication.title == title)
            .with_for_update()
            .first()
        )

        if not publication:
            # Create a new publication
            publication = Publication(
                title=title
            )
            publication.publication_date = pub_data["publication_date"]
            self.session.add(publication)

        # Update BaseEntity metadata
        publication.update_date = metadata.get("update_date")
        publication.update_count = metadata.get("update_count", publication.update_count + 1)

        # Process Authors
        self._process_authors(pub_data.get("authors", []), publication)

        # Process Journal or Conference
        if pub_data["type"] == "Journal":
            self._process_journal(pub_data, publication)
        elif pub_data["type"] == "Conference":
            self._process_conference(pub_data, publication)

    def _process_authors(self, author_names: list, publication: Publication):
        """
        Processes and associates authors with the publication.
        """
        for author_name in author_names:
            # Fetch or create the author
            author = (
                self.session.query(Author)
                .filter(Author.name == author_name)
                .with_for_update()
                .first()
            )

            if not author:
                author = Author(name=author_name)
                self.session.add(author)
                self.session.flush()  # Ensure `author.id` is available

            # Check if the association already exists
            association_exists = (
                self.session.query(PublicationAuthor)
                .filter_by(publication_id=publication.id, author_id=author.id)
                .first()
            )

            if not association_exists:
                # Add the association explicitly
                association = PublicationAuthor(publication_id=publication.id, author_id=author.id)
                self.session.add(association)

    def _process_journal(self, pub_data: dict, publication: Publication):
        """
        Processes and associates a journal with the publication.
        """
        journal_name = pub_data["journal_name"]
        publication_year = pub_data["publication_year"]

        journal = (
            self.session.query(Journal)
            .filter(Journal.title == journal_name, Journal.year == publication_year)
            .with_for_update()
            .first()
        )

        if not journal:
            # Create a new journal
            journal = Journal(
                title=journal_name,
                year=publication_year,
            )
            self.session.add(journal)

        # Link the journal to the publication
        publication.journal = journal

    def _process_conference(self, pub_data: dict, publication: Publication):
        """
        Processes and associates a conference with the publication.
        """
        conference_acronym = pub_data["conference_acronym"]
        conference_year = pub_data["conference_year"]

        conference = (
            self.session.query(Conference)
            .filter(
                Conference.acronym == conference_acronym,
                Conference.year == conference_year,
            )
            .with_for_update()
            .first()
        )

        if not conference:
            # Create a new conference
            conference = Conference(
                title=conference_acronym,
                acronym=conference_acronym,
                year=conference_year,
            )
            self.session.add(conference)

        # Link the conference to the publication
        publication.conference = conference