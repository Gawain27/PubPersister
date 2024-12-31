from sqlalchemy import func, literal

from com.gwngames.persister.entity.base.Author import Author
from com.gwngames.persister.entity.base.Conference import Conference
from com.gwngames.persister.entity.base.Journal import Journal
from com.gwngames.persister.entity.base.Publication import Publication
from com.gwngames.persister.entity.base.Relationships import PublicationAuthor, AuthorCoauthor

from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
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
        title_lower = title.lower()
        # Truncate the input title to match the length of the database title
        shortest_length = func.least(
            func.length(Publication.title),  # length of the DB column
            func.length(literal(title_lower))  # length of the title string
        )

        # Truncated DB string
        db_trunc = func.substring(
            func.lower(Publication.title),  # Apply lowercase to DB column
            1,
            shortest_length
        )

        # Truncated user input string
        user_trunc = func.substring(
            literal(title_lower),
            1,
            shortest_length
        )

        publication = (
            self.session.query(Publication)
            .filter(func.word_similarity(db_trunc, user_trunc) >= 0.85)
            .with_for_update()
            .first()
        )

        if not publication:
            publication = Publication(
                title=title,
                class_id=Publication.CLASS_ID,
                variant_id=Publication.VARIANT_ID,
            )
            self.session.add(publication)

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
            author_name_lower = author_name.lower()  # Convert to lowercase

            # Fetch the author using similarity
            author = (
                self.session.query(Author)
                .filter(func.word_similarity(func.lower(Author.name), author_name_lower) > 0.85)
                .with_for_update()
                .first()
            )

            if not author:
                author = Author(
                    name=author_name,
                    class_id=Author.CLASS_ID,
                    variant_id=Author.VARIANT_ID,
                )
                self.session.add(author)

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
        journal_lower = journal_name.lower()

        # Query using similarity for journal name and exact match for year
        journal = (
            self.session.query(Journal)
            .filter(
                func.word_similarity(func.lower(Journal.title), journal_lower) >= 0.65,
            )
            .with_for_update()
            .first()
        )

        if not journal:
            # Create a new Journal object if no match is found
            journal = Journal(
                title=journal_name,
                year=journal_year,
                type="journal",
                class_id=Journal.CLASS_ID,
                variant_id=Journal.VARIANT_ID
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
        acronym_lower = conference_acronym.lower()
        conference = (
            self.session.query(Conference)
            .filter(
                func.word_similarity(func.lower(Conference.acronym), acronym_lower) >= 0.6
            )
            .with_for_update()
            .first()
        )

        if not conference:
            # Create a new Conference object if no match is found
            conference = Conference(
                acronym=conference_acronym,
                class_id=Conference.CLASS_ID,
                variant_id=Conference.VARIANT_ID
            )
            self.session.add(conference)
            self.session.flush()  # Ensure the new Conference gets an ID for association

        # Create the association
        publication.conference = conference

