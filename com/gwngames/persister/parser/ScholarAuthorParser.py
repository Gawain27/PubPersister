from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from com.gwngames.persister.entity.base.Author import Author
from com.gwngames.persister.entity.base.Interest import Interest
from com.gwngames.persister.entity.base.Publication import Publication
from com.gwngames.persister.entity.base.Relationships import AuthorInterest, AuthorCoauthor, PublicationAuthor
from com.gwngames.persister.entity.variant.scholar.GoogleScholarAuthor import GoogleScholarAuthor


class ScholarAuthorParser:
    """
    Processes Google Scholar author data, including interests, co-authors, and publications.
    """

    def __init__(self, session: Session):
        """
        Initializes the ScholarAuthorParser with a SQLAlchemy session.

        :param session: SQLAlchemy session to manage database transactions.
        """
        self.session = session

    def process_google_scholar_data(self, json_data: dict):
        """
        Processes the provided JSON and persists/updates authors and related data.

        :param json_data: JSON data containing author details, interests, co-authors, and publications.
        """
        try:
            # Begin a nested transaction for pessimistic locking
            self.session.begin_nested()

            # Validate input data
            if "name" not in json_data or "author_id" not in json_data:
                raise ValueError("Missing required fields 'name' or 'author_id' in JSON data.")

            # Process the author
            author = self._process_author(json_data)

            # Process related data
            self._process_interests(author, json_data.get("interests", []))
            self._process_coauthors(author, json_data.get("coauthors", []))
            self._process_publications(author, json_data.get("publications", []))

            # Commit the changes if everything is successful
            self.session.commit()

        except (ValueError, SQLAlchemyError) as e:
            # Rollback transaction in case of any errors
            self.session.rollback()
            raise Exception(f"Error processing Google Scholar data: {str(e)}")

    def _process_author(self, json_data: dict) -> Author:
        """
        Processes and persists an author entity, including Google Scholar-specific data.

        :param json_data: Dictionary containing author details.
        :return: The persisted Author instance.
        """
        name = json_data["name"]
        scholar_id = json_data["author_id"]

        try:
            # Fetch or create the base Author
            author = (
                self.session.query(Author)
                .filter(Author.name == name)
                .with_for_update()
                .one_or_none()
            )

            if not author:
                author = Author(
                    name=name,
                    class_id=Author.CLASS_ID,
                    variant_id=Author.VARIANT_ID,
                )
                self.session.add(author)

            # Fetch or create the Google Scholar-specific author
            gscholar_author = (
                self.session.query(GoogleScholarAuthor)
                .filter(GoogleScholarAuthor.author_id == scholar_id)
                .with_for_update()
                .one_or_none()
            )

            if not gscholar_author:
                gscholar_author = GoogleScholarAuthor(
                    author_id=scholar_id,
                    author=author,
                    class_id=GoogleScholarAuthor.CLASS_ID,
                    variant_id=GoogleScholarAuthor.VARIANT_ID,
                )
                self.session.add(gscholar_author)

            # Update author fields
            author.role = json_data.get("role", author.role)
            author.organization = json_data.get("org", author.organization)
            author.image_url = json_data.get("image_url", author.image_url)
            author.homepage_url = json_data.get("homepage_url", author.homepage_url)

            # Update Google Scholar-specific fields
            gscholar_author.profile_url = json_data.get("profile_url", gscholar_author.profile_url)
            gscholar_author.verified = json_data.get("verified", gscholar_author.verified)
            gscholar_author.h_index = json_data.get("h_index", gscholar_author.h_index)
            gscholar_author.i10_index = json_data.get("i10_index", gscholar_author.i10_index)

            return author

        except SQLAlchemyError as e:
            raise Exception(f"Error processing author '{name}': {str(e)}")

    def _process_interests(self, author: Author, interests: list):
        """
        Processes and associates interests with the author.

        :param author: The Author instance.
        :param interests: List of interest names.
        """
        for interest_name in interests:
            if not interest_name:
                continue  # Skip invalid entries

            try:
                # Fetch or create the interest
                interest = (
                    self.session.query(Interest)
                    .filter(Interest.name == interest_name)
                    .one_or_none()
                )

                if not interest:
                    interest = Interest(
                        name=interest_name,
                        class_id=Interest.CLASS_ID,
                        variant_id=Interest.VARIANT_ID,
                    )
                    self.session.add(interest)

                # Associate interest with the author
                if not self.session.query(AuthorInterest).filter_by(
                    author_id=author.id, interest_id=interest.id
                ).first():
                    self.session.add(AuthorInterest(author_id=author.id, interest_id=interest.id))

            except SQLAlchemyError as e:
                raise Exception(f"Error processing interest '{interest_name}' for author '{author.name}': {str(e)}")

    def _process_coauthors(self, author: Author, coauthors: list):
        """
        Processes and associates co-authors with the author.

        :param author: The Author instance.
        :param coauthors: List of co-author names.
        """
        for coauthor_name in coauthors:
            if not coauthor_name:
                continue  # Skip invalid entries

            try:
                # Fetch or create the co-author
                coauthor = (
                    self.session.query(Author)
                    .filter(Author.name == coauthor_name)
                    .with_for_update()
                    .one_or_none()
                )

                if not coauthor:
                    coauthor = Author(
                        name=coauthor_name,
                        class_id=Author.CLASS_ID,
                        variant_id=Author.VARIANT_ID,
                    )
                    self.session.add(coauthor)

                # Associate co-author
                if not self.session.query(AuthorCoauthor).filter_by(
                    author_id=author.id, coauthor_id=coauthor.id
                ).first():
                    self.session.add(AuthorCoauthor(author_id=author.id, coauthor_id=coauthor.id))

            except SQLAlchemyError as e:
                raise Exception(f"Error processing co-author '{coauthor_name}' for author '{author.name}': {str(e)}")

    def _process_publications(self, author: Author, publications: list):
        """
        Processes and associates publications with the author.

        :param author: The Author instance.
        :param publications: List of publication dictionaries.
        """
        for pub_data in publications:
            try:
                title = pub_data.get("title")
                if not title:
                    continue  # Skip invalid entries

                # Fetch or create the publication
                publication = (
                    self.session.query(Publication)
                    .filter(Publication.title == title)
                    .with_for_update()
                    .one_or_none()
                )

                if not publication:
                    publication = Publication(
                        title=title,
                        url=pub_data.get("url"),
                        class_id=Publication.CLASS_ID,
                        variant_id=Publication.VARIANT_ID,
                    )
                    self.session.add(publication)

                # Associate publication with the author
                if not self.session.query(PublicationAuthor).filter_by(
                    publication_id=publication.id, author_id=author.id
                ).first():
                    self.session.add(PublicationAuthor(publication_id=publication.id, author_id=author.id))

            except SQLAlchemyError as e:
                raise Exception(f"Error processing publication '{pub_data.get('title')}' for author '{author.name}': {str(e)}")