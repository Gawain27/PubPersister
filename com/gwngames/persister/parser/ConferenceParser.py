import re
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from com.gwngames.persister.entity.base.Conference import Conference


class ConferenceProcessor:
    """
    Processes and persists Conference entities, ensuring BaseEntity metadata is updated.
    """

    def __init__(self, session: Session):
        self.session = session

    def process_json(self, json_data: dict):
        """
        Processes the provided JSON and persists/updates conference data.
        """
        try:
            self.session.begin_nested()

            conferences = json_data.get("conferences", [])

            for conference_data in conferences:
                self._process_conference(conference_data, json_data)

            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise Exception(f"Error processing JSON data: {str(e)}")

    import re
    from datetime import datetime

    def _process_conference(self, conference_data: dict, metadata: dict):
        """
        Processes and persists a single conference.
        """
        title = conference_data["title"]
        conference = (
            self.session.query(Conference)
            .filter(Conference.title == title)
            .with_for_update()
            .first()
        )

        # Extract year from source and create a valid Date object
        source = conference_data.get("source", "")
        year = self._extract_year_from_source(source)
        if not year:  # Fallback or default value
            year = datetime.now().year  # Default to the current year if not found
        year_date = datetime(year, 1, 1).date()  # Use January 1st for the year

        if not conference:
            conference = Conference(
                title=title,
                acronym=conference_data.get("acronym"),
                publisher=source,  # Mapped from 'source'
                rank=conference_data.get("rank"),
                note=conference_data.get("note"),
                dblp_link=conference_data.get("dblp_link"),
                primary_for=conference_data.get("primary_for"),
                comments=conference_data.get("comments"),
                average_rating=conference_data.get("average_rating"),
                year=year_date,  # Assign extracted year as a Date object
                class_id=Conference.CLASS_ID,
                variant_id=Conference.VARIANT_ID
            )
            self.session.add(conference)
        else:
            conference.acronym = conference_data.get("acronym", conference.acronym)
            conference.publisher = source
            conference.rank = conference_data.get("rank", conference.rank)
            conference.note = conference_data.get("note", conference.note)
            conference.dblp_link = conference_data.get("dblp_link", conference.dblp_link)
            conference.primary_for = conference_data.get("primary_for", conference.primary_for)
            conference.comments = conference_data.get("comments", conference.comments)
            conference.average_rating = conference_data.get("average_rating", conference.average_rating)
            conference.year = year_date  # Update year if modified

        # Update BaseEntity metadata
        conference.update_date = metadata.get("update_date")
        conference.update_count = metadata.get("update_count",
                                               conference.update_count + 1 if conference.update_count else 1)

    def _extract_year_from_source(self, source: str) -> int:
        """
        Extracts a 4-digit year from the source string.
        """
        match = re.search(r'\b(\d{4})\b', source)
        return int(match.group(1)) if match else None
