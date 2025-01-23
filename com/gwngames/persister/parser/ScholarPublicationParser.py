import logging

from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from com.gwngames.persister.entity.base.Author import Author
from com.gwngames.persister.entity.base.Publication import Publication
from com.gwngames.persister.entity.base.Relationships import PublicationAuthor
from com.gwngames.persister.entity.variant.scholar.GoogleScholarCitation import GoogleScholarCitation
from com.gwngames.persister.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication
from com.gwngames.persister.utils.StringUtils import StringUtils

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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
            self.session.begin_nested()
            if "title" not in json_data or "publication_id" not in json_data:
                logger.error("Missing required fields 'title' or 'publication_id' in JSON data")
                raise ValueError("Missing required fields 'title' or 'publication_id' in JSON data.")

            authors = self._process_authors(json_data.get("authors", []))
            publication = self._process_publication(json_data)
            gscholar_pub = self._process_google_scholar_publication(json_data, publication)
            self._process_citations(json_data.get("citation_graph", []), gscholar_pub)

            for author in authors:
                if not self.session.query(PublicationAuthor).filter_by(
                        publication_id=publication.id, author_id=author.id
                ).first():
                    self.session.add(PublicationAuthor(publication_id=publication.id, author_id=author.id))

            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.exception("Error processing Google Scholar publication data")
            raise Exception(f"Error processing Google Scholar publication data: {str(e)}")
        self.session.close()

    def _process_publication(self, json_data: dict) -> Publication:
        """
        Processes and persists the Publication entity.
        """
        title = json_data["title"].lower()

        first_word = StringUtils.first_after_fifth(title)

        publication = (
            self.session.query(Publication)
            .filter(Publication.title.like(f"%{first_word}%"))
            .filter(func.jaro_winkler_similarity(Publication.title, title) >= 0.87)
            .order_by(desc(func.jaro_winkler_similarity(Publication.title, title)))
            .first()
        )

        if publication is None:
            publication = Publication(
                title=title,
                url=json_data.get("publication_url"),
                publication_year=int(json_data.get("publication_date", 0)),
                pages=json_data.get("pages"),
                publisher=json_data.get("publisher"),
                description=json_data.get("description"),
                class_id=Publication.CLASS_ID,
                variant_id=Publication.VARIANT_ID,
            )
            self.session.add(publication)
            self.session.flush()
        else:
            publication.url = json_data.get("publication_url", publication.url)
            publication.publication_year = int(
                json_data.get("publication_date", publication.publication_year)
            )
            publication.pages = json_data.get("pages", publication.pages)
            publication.publisher = json_data.get("publisher", publication.publisher)
            publication.description = json_data.get("description", publication.description)

        self.session.flush()
        return publication

    def _process_google_scholar_publication(self, json_data: dict,
                                            publication: Publication) -> GoogleScholarPublication:
        """
        Processes the Google Scholar-specific publication data.
        """
        publication_id = json_data["publication_id"]
        gscholar_pub = (
            self.session.query(GoogleScholarPublication)
            .filter(GoogleScholarPublication.publication_id == publication_id)
            .filter(GoogleScholarPublication.cites_id == json_data.get("cites_id"))
            .first()
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
            gscholar_pub.publication_key = publication.id
            self.session.add(gscholar_pub)

        return gscholar_pub

    def _process_authors(self, authors: list):
        """
        Processes and associates authors with the publication.
        """
        author_res = []
        for author_name in authors:
            if not author_name:
                logger.warning("Skipping empty author name")
                continue

            author_name = author_name.lower()
            surname = author_name.split(" ")[-1]
            if len(author_name.split(" ")[0].replace('.', '')) > 1:
                initials = author_name[:2]
            else:
                initials = author_name[:1]

            author = (
                self.session.query(Author)
                .filter(Author.name.like(f"%{surname}"))
                .filter(Author.name.like(f"{initials}%"))
                .filter(func.word_similarity(Author.name, author_name) >= 0.7)
                .order_by(desc(func.word_similarity(Author.name, author_name)))
                .first()
            )

            if not author:
                if StringUtils.is_first_word_short(author_name):
                    continue
                author = Author(
                    name=author_name,
                    class_id=Author.CLASS_ID,
                    variant_id=Author.VARIANT_ID,
                )
                self.session.add(author)
            author_res.append(author)
        return author_res

    def _process_citations(self, citations: list, gscholar_pub: GoogleScholarPublication):
        """
        Processes and associates citations with the Google Scholar publication.
        """
        for citation_data in citations:
            citation_link = citation_data.get("citation_link")
            if not citation_link:
                continue

            citation = (
                self.session.query(GoogleScholarCitation)
                .filter(GoogleScholarCitation.citation_link == citation_link)
                .first()
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

