from datetime import datetime

from sqlalchemy import func
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
        Processes and persists a single journal using word similarity for title matching.
        """
        title = journal_data["title"]

        # Truncate the input title to match the length of the database field
        title_length = func.length(Journal.title)
        truncated_title = func.substring(title, 1, title_length)

        # Query using similarity for journal title
        journal = (
            self.session.query(Journal)
            .filter(func.word_similarity(Journal.title, truncated_title) > 0.7)
            .with_for_update()
            .first()
        )

        year = journal_data.get("year")
        if not year:
            year = 0  # Default to 0 for null year

        if not journal:
            # Create a new Journal object if no match is found
            journal = Journal(
                title=title,
                type=journal_data["type"],
                link=journal_data.get("link"),
                sjr=journal_data.get("sjr"),
                q_rank=journal_data.get("q_rank"),
                h_index=journal_data.get("h_index"),
                total_docs=journal_data.get("total_docs"),
                total_docs_3years=journal_data.get("total_docs_3years"),
                total_refs=journal_data.get("total_refs"),
                total_cites_3years=journal_data.get("total_cites_3years"),
                citable_docs_3years=journal_data.get("citable_docs_3years"),
                cites_per_doc_2years=journal_data.get("cites_per_doc_2years"),
                refs_per_doc=journal_data.get("refs_per_doc"),
                female_percent=journal_data.get("female_percent"),
                year=year,
                class_id=Journal.CLASS_ID,
                variant_id=Journal.VARIANT_ID
            )
            self.session.add(journal)
        else:
            # Update existing Journal object
            journal.link = journal_data.get("link", journal.link)
            journal.sjr = journal_data.get("sjr", journal.sjr)
            journal.q_rank = journal_data.get("q_rank", journal.q_rank)
            journal.h_index = journal_data.get("h_index", journal.h_index)
            journal.total_docs = journal_data.get("total_docs", journal.total_docs)
            journal.total_docs_3years = journal_data.get("total_docs_3years", journal.total_docs_3years)
            journal.total_refs = journal_data.get("total_refs", journal.total_refs)
            journal.total_cites_3years = journal_data.get("total_cites_3years", journal.total_cites_3years)
            journal.citable_docs_3years = journal_data.get("citable_docs_3years", journal.citable_docs_3years)
            journal.cites_per_doc_2years = journal_data.get("cites_per_doc_2years", journal.cites_per_doc_2years)
            journal.refs_per_doc = journal_data.get("refs_per_doc", journal.refs_per_doc)
            journal.female_percent = journal_data.get("female_percent", journal.female_percent)
            journal.year = year

        # Update BaseEntity metadata
        journal.update_date = metadata.get("update_date")
        journal.update_count = metadata.get("update_count", journal.update_count + 1 if journal.update_count else 1)
