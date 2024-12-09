from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from com.gwngames.persister.entity.variant.scholar.GoogleScholarCitation import GoogleScholarCitation
from com.gwngames.persister.entity.variant.scholar.GoogleScholarPublication import GoogleScholarPublication


class ScholarCitationParser:
    """
    Processes and persists Google Scholar citation data, linking it to existing publications.
    """

    def __init__(self, session: Session):
        """
        Initializes the ScholarCitationParser with a SQLAlchemy session.

        :param session: SQLAlchemy session to manage database transactions.
        """
        self.session = session

    def process_json(self, json_data: dict):
        """
        Processes the provided JSON and persists/updates citations linked to publications.

        :param json_data: JSON data containing citations and related publication information.
        """
        try:
            # Begin a nested transaction for pessimistic locking
            self.session.begin_nested()

            # Extract publication identifier
            cites_id = json_data.get("cites_id")
            if not cites_id:
                raise ValueError("Missing 'cites_id' in the input JSON.")

            # Find the publication linked to this citation
            publication = self._find_publication(cites_id)
            if not publication:
                raise ValueError(f"Publication with cites_id '{cites_id}' not found.")

            # Process each citation in the data
            citations = json_data.get("citations", [])
            if not citations:
                raise ValueError("No citations provided in the input JSON.")

            for citation_data in citations:
                self._process_citation(citation_data, publication)
            # Perform operations
            if not cites_id:
                raise ValueError("cites_id cannot be null")
            self.session.commit()
        except IntegrityError as e:
            self.session.rollback()
            print(f"Database constraint error: {e}")
        except Exception as e:
            self.session.rollback()
            print(f"Unexpected error: {e}")
        finally:
            self.session.close()  # Ensure session cleanup

    def _find_publication(self, cites_id: str) -> GoogleScholarPublication:
        """
        Finds the publication linked to the given cites_id.

        :param cites_id: The Google Scholar unique ID for the publication.
        :return: GoogleScholarPublication instance if found, None otherwise.
        """
        try:
            return (
                self.session.query(GoogleScholarPublication)
                .filter(GoogleScholarPublication.cites_id == cites_id)
                .with_for_update()
                .one_or_none()
            )
        except SQLAlchemyError as e:
            raise Exception(f"Database error while retrieving publication with cites_id '{cites_id}': {str(e)}")

    def _process_citation(self, citation_data: dict, publication: GoogleScholarPublication):
        """
        Processes and persists a single citation, linking it to the publication.

        :param citation_data: Dictionary containing citation details.
        :param publication: GoogleScholarPublication instance to link the citation to.
        """
        # Validate citation_data keys
        required_keys = ["link", "cites_id"]
        for key in required_keys:
            if key not in citation_data:
                raise ValueError(f"Missing required key '{key}' in citation data: {citation_data}")

        # Extract citation details
        citation_link = citation_data["link"]
        cites_id = citation_data["cites_id"]

        # Fetch or create the citation
        try:
            citation = (
                self.session.query(GoogleScholarCitation)
                .filter(GoogleScholarCitation.cites_id == cites_id)
                .with_for_update()
                .one_or_none()
            )

            if not citation:
                # Create a new citation
                citation = GoogleScholarCitation(
                    publication_id=publication.id,
                    citation_link=citation_link,
                    cites_id=cites_id,
                    title=citation_data.get("title"),
                    link=citation_data.get("link"),
                    summary=citation_data.get("summary"),
                    document_link=citation_data.get("document_link"),
                    year=self._extract_year(citation_data, publication),
                    citations=citation_data.get("citations", publication.total_citations),
                    class_id=GoogleScholarCitation.CLASS_ID,
                    variant_id=GoogleScholarCitation.VARIANT_ID,
                )
                self.session.add(citation)
            else:
                # Update existing citation
                citation.title = citation_data.get("title", citation.title)
                citation.link = citation_data.get("link", citation.link)
                citation.summary = citation_data.get("summary", citation.summary)
                citation.document_link = citation_data.get("document_link", citation.document_link)
                citation.year = citation.year or self._extract_year(citation_data, publication)
                citation.citations = citation_data.get("citations", citation.citations)

            # Update BaseEntity metadata
            citation.update_date = citation_data.get("update_date", publication.update_date)
            citation.update_count = citation_data.get("update_count", citation.update_count + 1 if citation.update_count else 1)

        except SQLAlchemyError as e:
            raise Exception(f"Error processing citation with link '{citation_link}': {str(e)}")

    def _extract_year(self, citation_data: dict, publication: GoogleScholarPublication) -> str:
        """
        Extracts the year for the citation, defaulting to the publication's year if not provided.

        :param citation_data: Dictionary containing citation details.
        :param publication: GoogleScholarPublication instance for fallback year.
        :return: The year as a string.
        """
        try:
            if "year" in citation_data:
                return str(citation_data["year"])
            if publication.publication and publication.publication.publication_date:
                return str(publication.publication.publication_date.year)
        except Exception as e:
            raise Exception(f"Error extracting year for citation: {str(e)}")

        # Fallback to 'Unknown'
        return "Unknown"
