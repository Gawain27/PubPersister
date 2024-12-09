from datetime import datetime

from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from com.gwngames.persister.entity.base.Author import Author
from com.gwngames.persister.entity.base.BaseEntity import BaseEntity
from com.gwngames.persister.entity.base.Interest import Interest
from com.gwngames.persister.entity.base.Publication import Publication
from com.gwngames.persister.entity.base.Relationships import AuthorInterest, AuthorCoauthor, PublicationAuthor
from com.gwngames.persister.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor


class ScholarAuthorParser:
    """
    Class to handle Google Scholar JSON data and persist/update in the database.
    """

    def __init__(self, session: Session):
        """
        Initialize with a SQLAlchemy session.
        """
        self.session = session

    def process_google_scholar_data(self, json_data: dict):
        """
        Process the given JSON data, persisting/updating entities into the database.
        """
        try:
            # Process the main author (Google Scholar Author)
            author_name = json_data["name"]
            google_scholar_id = json_data["author_id"]

            # Retrieve or create the main Author
            author = self._get_or_create_author(author_name)
            author.role = json_data.get("role", author.role)
            author.organization = json_data.get("org", author.organization)
            author.image_url = json_data.get("image_url", author.image_url)
            author.homepage_url = json_data.get("homepage_url", author.homepage_url)

            # Retrieve or create the Google Scholar Author
            gscholar_author = self._get_or_create_google_scholar_author(author, google_scholar_id)
            gscholar_author.profile_url = json_data.get("profile_url", gscholar_author.profile_url)
            gscholar_author.verified = json_data.get("verified", gscholar_author.verified)
            gscholar_author.h_index = json_data.get("h_index", gscholar_author.h_index)
            gscholar_author.i10_index = json_data.get("i10_index", gscholar_author.i10_index)

            self._update_interests(author, json_data.get("interests", []))

            self._update_coauthors(author, json_data.get("coauthors", []))

            self._process_publications(author, json_data.get("publications", []))

            # Update metadata in BaseEntity
            self._update_metadata(author)

            self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e

    def _get_or_create_author(self, name: str) -> Author:
        """
        Retrieve or create an Author by name.
        """
        try:
            return self.session.query(Author).with_for_update().filter(Author.name == name).one()
        except NoResultFound:
            author = Author(name=name)
            self.session.add(author)
            return author

    def _get_or_create_google_scholar_author(self, author: Author, scholar_id: str) -> GoogleScholarAuthor:
        """
        Retrieve or create a GoogleScholarAuthor by scholar_id.
        """
        try:
            return self.session.query(GoogleScholarAuthor).with_for_update().filter(
                GoogleScholarAuthor.author_id == scholar_id
            ).one()
        except NoResultFound:
            gscholar_author = GoogleScholarAuthor(author_id=scholar_id)
            gscholar_author.author = author
            self.session.add(gscholar_author)
            return gscholar_author

    def _update_interests(self, author: Author, interests: list[str]):
        """
        Update the interests of an author.
        """
        for interest_name in interests:
            # Fetch or create the interest
            interest = (
                self.session.query(Interest)
                .filter(Interest.name == interest_name)
                .one_or_none()
            )
            if not interest:
                interest = Interest(name=interest_name)
                self.session.add(interest)
                self.session.flush()  # Ensure `interest.id` is available

            association_exists = (
                self.session.query(AuthorInterest)
                .filter_by(author_id=author.id, interest_id=interest.id)
                .first()
            )

            if not association_exists:
                # Add the association explicitly
                association = AuthorInterest(author_id=author.id, interest_id=interest.id)
                self.session.add(association)

    def _update_coauthors(self, author: Author, coauthors: list[str]):
        """
        Update the coauthors of an author.
        """
        for coauthor_name in coauthors:
            # Fetch or create the coauthor
            coauthor = self._get_or_create_author(coauthor_name)

            association_exists = (
                self.session.query(AuthorCoauthor)
                .filter(
                    (AuthorCoauthor.author_id == author.id) &
                    (AuthorCoauthor.coauthor_id == coauthor.id)
                )
                .first()
            )

            if not association_exists:
                # Add the association explicitly
                association = AuthorCoauthor(author_id=author.id, coauthor_id=coauthor.id)
                self.session.add(association)

    def _process_publications(self, author: Author, publications: list[dict]):
        """
        Process and associate publications with the author.
        """
        for pub_data in publications:
            title = pub_data["title"]
            # Fetch or create the publication
            publication = (
                self.session.query(Publication)
                .with_for_update()
                .filter(Publication.title == title)
                .one_or_none()
            )

            if not publication:
                publication = Publication(title=title)
                self.session.add(publication)
                self.session.flush()  # Ensure `publication.id` is available

            # Update publication details
            publication.publication_year = None  # Add year if available in the future
            publication.url = pub_data.get("url", publication.url)

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

    def _update_metadata(self, entity: BaseEntity):
        """
        Update metadata fields in BaseEntity.
        """
        entity.update_date = datetime.now()
        entity.update_count = entity.update_count + 1 if entity.update_count else 1