from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy import func, literal

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
            self.session.begin_nested()

            if "name" not in json_data or "author_id" not in json_data:
                raise ValueError("Missing required fields 'name' or 'author_id' in JSON data.")

            author = self._process_author(json_data)

            self._process_interests(author, json_data.get("interests", []))
            self._process_coauthors(author, json_data.get("coauthors", []))
            self._process_publications(author, json_data.get("publications", []))

            self.session.commit()

        except (ValueError, SQLAlchemyError) as e:
            self.session.rollback()
            raise Exception(f"Error processing Google Scholar data: {str(e)}")

    def _process_author(self, json_data: dict) -> Author:
        """
        Processes and persists an author entity, including Google Scholar-specific data.

        :param json_data: Dictionary containing author details.
        :return: The persisted Author instance.
        """
        name = json_data["name"]
        name_lower = name.lower()
        scholar_id = json_data["author_id"]

        try:
            author = (
                self.session.query(Author)
                .filter(func.word_similarity(func.lower(Author.name), name_lower) > 0.85)
                .with_for_update()
                .first()
            )

            if not author:
                author = Author(
                    name=name,
                    class_id=Author.CLASS_ID,
                    variant_id=Author.VARIANT_ID,
                )
                self.session.add(author)

            gscholar_author = (
                self.session.query(GoogleScholarAuthor)
                .filter(GoogleScholarAuthor.author_id == scholar_id)
                .with_for_update()
                .first()
            )

            if not gscholar_author:
                gscholar_author = GoogleScholarAuthor(
                    author_id=scholar_id,
                    class_id=GoogleScholarAuthor.CLASS_ID,
                    variant_id=GoogleScholarAuthor.VARIANT_ID,
                )
                gscholar_author.author = author
                gscholar_author.author_key = author.id
                self.session.add(gscholar_author)

            author.role = json_data.get("role", author.role)
            author.organization = json_data.get("org", author.organization)
            author.image_url = json_data.get("image_url", author.image_url)
            author.homepage_url = json_data.get("homepage_url", author.homepage_url)

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
                continue

            interest_name_lower = interest_name.lower()  # Convert to lowercase

            try:
                interest = (
                    self.session.query(Interest)
                    .filter(func.word_similarity(func.lower(Interest.name), interest_name_lower) > 0.75)
                    .first()
                )

                if not interest:
                    interest = Interest(
                        name=interest_name,
                        class_id=Interest.CLASS_ID,
                        variant_id=Interest.VARIANT_ID,
                    )
                    self.session.add(interest)

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
                continue

            coauthor_name_lower = coauthor_name.lower()  # Convert to lowercase

            try:
                coauthor = (
                    self.session.query(Author)
                    .filter(func.word_similarity(func.lower(Author.name), coauthor_name_lower) >= 0.85)
                    .with_for_update()
                    .first()
                )

                if not coauthor:
                    coauthor = Author(
                        name=coauthor_name,
                        class_id=Author.CLASS_ID,
                        variant_id=Author.VARIANT_ID,
                    )
                    self.session.add(coauthor)

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
                    continue

                title_lower = title.lower()  # Convert to lowercase

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
                        url=pub_data.get("url"),
                        class_id=Publication.CLASS_ID,
                        variant_id=Publication.VARIANT_ID,
                    )
                    self.session.add(publication)

                if not self.session.query(PublicationAuthor).filter_by(
                    publication_id=publication.id, author_id=author.id
                ).first():
                    self.session.add(PublicationAuthor(publication_id=publication.id, author_id=author.id))

            except SQLAlchemyError as e:
                raise Exception(f"Error processing publication '{pub_data.get('title')}' for author '{author.name}': {str(e)}")

