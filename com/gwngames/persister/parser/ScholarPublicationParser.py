from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from com.gwngames.persister.entity.base.Author import Author
from com.gwngames.persister.entity.base.Publication import Publication
from com.gwngames.persister.entity.base.Relationships import PublicationAuthor
from com.gwngames.persister.entity.variant.scholar.GoogleScholarCitation import GoogleScholarCitation
from com.gwngames.persister.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication


class ScholarPublicationParser:
    """
    Processes Google Scholar publication data, including citations, authors, and metadata.
    """

    def __init__(self, session: Session):
        """
        Initializes the ScholarPublicationParser with a SQLAlchemy session.

        :param session: SQLAlchemy session to manage database transactions.
        """
        self.session = session

    def process_json(self, json_data: dict):
        """
        Processes the provided JSON and persists/updates the publication and related data.

        :param json_data: JSON data containing publication details, authors, and citations.
        """
        try:
            # Begin a nested transaction for pessimistic locking
            self.session.begin_nested()

            # Validate input data
            if "title" not in json_data or "publication_id" not in json_data:
                raise ValueError("Missing required fields 'title' or 'publication_id' in JSON data.")

            # Process the publication
            publication = self._process_publication(json_data)

            # Process Google Scholar-specific publication data
            gscholar_pub = self._process_google_scholar_publication(json_data, publication)

            # Process authors associated with the publication
            self._process_authors(json_data.get("authors", []), publication)

            # Process citations associated with the publication
        #    self._process_citations(json_data.get("citation_graph", []), gscholar_pub)

            # Commit the changes if everything is successful
            self.session.commit()

        except (ValueError, SQLAlchemyError) as e:
            # Rollback transaction in case of any errors
            self.session.rollback()
            raise Exception(f"Error processing Google Scholar publication data: {str(e)}")

    def _process_publication(self, json_data: dict) -> Publication:
        """
        Processes and persists the Publication entity.

        :param json_data: Dictionary containing publication details.
        :return: The persisted Publication instance.
        """
        try:
            title = json_data["title"]

            # Prevent flushing while querying
            with self.session:
                publication = (
                    self.session.query(Publication)
                    .filter(Publication.title == title)
                    .with_for_update()
                    .one_or_none()
                )

            if publication is None:
                publication = Publication(
                    title=title,
                    url=json_data.get("publication_url"),
                    publication_year=int(json_data.get("publication_date")),
                    pages=json_data.get("pages"),
                    publisher=json_data.get("publisher"),
                    description=json_data.get("description"),
                    class_id=Publication.CLASS_ID,
                    variant_id=Publication.VARIANT_ID,
                )
                self.session.add(publication)
            else:
                publication.title = title,
                publication.url = json_data.get("publication_url")
                publication.publication_year = int(json_data["publication_date"])
                publication.pages = json_data.get("pages")
                publication.publisher = json_data.get("publisher")
                publication.description = json_data.get("description")

            # Update metadata
            publication.update_date = datetime.now()
            publication.update_count = publication.update_count + 1 if publication.update_count else 1

            return publication

        except SQLAlchemyError as e:
            raise Exception(f"Error processing publication '{json_data.get('title')}': {str(e)}")

    def _process_google_scholar_publication(self, json_data: dict, publication: Publication) -> GoogleScholarPublication:
        """
        Processes the Google Scholar-specific publication data.

        :param json_data: Dictionary containing Google Scholar-specific details.
        :param publication: The Publication instance.
        :return: The persisted GoogleScholarPublication instance.
        """
        try:
            publication_id = json_data["publication_id"]

            with self.session:
                gscholar_pub = (
                    self.session.query(GoogleScholarPublication)
                    .filter(GoogleScholarPublication.publication_id == publication_id)
                    .filter(GoogleScholarPublication.cites_id == json_data.get("cites_id"))
                    .with_for_update()
                    .one_or_none()
                )

            if not gscholar_pub:
                gscholar_pub = GoogleScholarPublication(
                    publication_id=publication_id,
                    title_link=json_data.get("title_link"),
                    pdf_link=json_data.get("pdf_link"),
                    total_citations=json_data.get("total_citations"),
                    cites_id=json_data.get("cites_id"),
                    related_articles_url=json_data.get("related_articles_url"),
                    all_versions_url=json_data.get("all_versions_url"),
                    class_id=GoogleScholarPublication.CLASS_ID,
                    variant_id=GoogleScholarPublication.VARIANT_ID,
                )
                gscholar_pub.publication = publication
                self.session.add(gscholar_pub)

            # Update metadata
            gscholar_pub.update_date = datetime.now()
            gscholar_pub.update_count = gscholar_pub.update_count + 1 if gscholar_pub.update_count else 1

            return gscholar_pub

        except SQLAlchemyError as e:
            raise Exception(f"Error processing Google Scholar publication '{json_data.get('title')}': {str(e)}")

    def _process_authors(self, authors: list, publication: Publication):
        """
        Processes and associates authors with the publication.

        :param authors: List of author names.
        :param publication: The Publication instance.
        """
        for author_name in authors:
            if not author_name:
                continue  # Skip invalid entries

            try:
                with self.session.no_autoflush:
                    author = (
                        self.session.query(Author)
                        .filter(Author.name == author_name)
                        .with_for_update()
                        .one_or_none()
                    )

                if not author:
                    author = Author(
                        name=author_name,
                        class_id=Author.CLASS_ID,
                        variant_id=Author.VARIANT_ID,
                    )
                    self.session.add(author)

                # Associate the Author with the Publication
                if not self.session.query(PublicationAuthor).filter_by(
                    publication_id=publication.id, author_id=author.id
                ).first():
                    self.session.add(PublicationAuthor(publication_id=publication.id, author_id=author.id))

            except SQLAlchemyError as e:
                raise Exception(f"Error processing author '{author_name}' for publication '{publication.title}': {str(e)}")

    def _process_citations(self, citations: list, gscholar_pub: GoogleScholarPublication):
        """
        Processes and associates citations with the Google Scholar publication.

        :param citations: List of citation dictionaries.
        :param gscholar_pub: The GoogleScholarPublication instance.
        """
        for citation_data in citations:
            try:
                citation_link = citation_data.get("citation_link")
                if not citation_link:
                    continue  # Skip invalid entries

                with self.session.no_autoflush:
                    citation = (
                        self.session.query(GoogleScholarCitation)
                        .filter(GoogleScholarCitation.citation_link == citation_link)
                        .with_for_update()
                        .one_or_none()
                    )

                if not citation:
                    citation = GoogleScholarCitation(
                        publication_id=gscholar_pub.id,
                        citation_link=citation_link,
                        year=citation_data.get("year"),
                        citations=citation_data.get("citations"),
                        cites_id=gscholar_pub.cites_id,
                        class_id=GoogleScholarCitation.CLASS_ID,
                        variant_id=GoogleScholarCitation.VARIANT_ID,
                    )
                    self.session.add(citation)

                # Update metadata
                citation.publication_id = gscholar_pub.id
                citation.update_date = datetime.now()
                citation.update_count = citation.update_count + 1 if citation.update_count else 1

            except SQLAlchemyError as e:
                raise Exception(f"Error processing citation '{citation_data.get('citation_link')}' for publication '{gscholar_pub.publication.title}': {str(e)}")