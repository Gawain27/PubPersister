from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from com.gwngames.persister.entity.base.Journal import Journal


class JournalParser:
    """
    Processes and persists Journal entities, ensuring BaseEntity metadata is updated.
    """

    def __init__(self, session: Session):
        self.session = session

    def process_json(self, json_data: dict):
        """
        Processes the provided JSON and persists/updates journal data.
        """
        try:
            # Begin a nested transaction for pessimistic locking
            self.session.begin_nested()

            journals = json_data.get("journals", [])

            for journal_data in journals:
                self._process_journal(journal_data, json_data)

            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise Exception(f"Error processing JSON data: {str(e)}")

    def _process_journal(self, journal_data: dict, metadata: dict):
        """
        Processes and persists a single journal.
        """
        title = journal_data["title"]
        journal = (
            self.session.query(Journal)
            .filter(Journal.title == title)
            .with_for_update()
            .first()
        )

        year = journal_data.get("year")
        if not year:
            year = datetime.now().year  # Default to the current year if not found
        year_date = datetime(year, 1, 1).date()  # Use January 1st for the year

        if not journal:
            journal = Journal(
                title=title,
                type=journal_data["type"],
                link=journal_data.get("link"),
                sjr=journal_data.get("sjr"),
                q_rank=journal_data.get("q_rank"),
                h_index=journal_data.get("h_index"),
                total_docs_2008=journal_data.get("total_docs_2008"),
                total_docs_3years=journal_data.get("total_docs_3years"),
                total_refs_2008=journal_data.get("total_refs_2008"),
                total_cites_3years=journal_data.get("total_cites_3years"),
                citable_docs_3years=journal_data.get("citable_docs_3years"),
                cites_per_doc_2years=journal_data.get("cites_per_doc_2years"),
                refs_per_doc_2008=journal_data.get("refs_per_doc_2008"),
                female_percent_2008=journal_data.get("female_percent_2008"),
                year = year_date,
                class_id = Journal.CLASS_ID,
                variant_id = Journal.VARIANT_ID
            )
            self.session.add(journal)
        else:
            journal.link = journal_data.get("link", journal.link)
            journal.sjr = journal_data.get("sjr", journal.sjr)
            journal.q_rank = journal_data.get("q_rank", journal.q_rank)
            journal.h_index = journal_data.get("h_index", journal.h_index)
            journal.total_docs_2008 = journal_data.get("total_docs_2008", journal.total_docs_2008)
            journal.total_docs_3years = journal_data.get("total_docs_3years", journal.total_docs_3years)
            journal.total_refs_2008 = journal_data.get("total_refs_2008", journal.total_refs_2008)
            journal.total_cites_3years = journal_data.get("total_cites_3years", journal.total_cites_3years)
            journal.citable_docs_3years = journal_data.get("citable_docs_3years", journal.citable_docs_3years)
            journal.cites_per_doc_2years = journal_data.get("cites_per_doc_2years", journal.cites_per_doc_2years)
            journal.refs_per_doc_2008 = journal_data.get("refs_per_doc_2008", journal.refs_per_doc_2008)
            journal.female_percent_2008 = journal_data.get("female_percent_2008", journal.female_percent_2008)
            journal.year = year_date

        # Update BaseEntity metadata
        journal.update_date = metadata.get("update_date")
        journal.update_count = metadata.get("update_count", journal.update_count + 1 if journal.update_count else 1)