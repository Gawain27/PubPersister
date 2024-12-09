from com.gwngames.persister.entity.base.BaseEntity import BaseEntity

from sqlalchemy import Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import relationship, Mapped

from com.gwngames.persister.entity.base.Publication import Publication


class GoogleScholarPublication(BaseEntity):
    """
    Represents a Google Scholar-specific publication.
    """
    __tablename__ = 'google_scholar_publication'

    CLASS_ID = 1010
    VARIANT_ID = 50

    id = Column(Integer, ForeignKey('publication.id'), primary_key=True)
    publication_id = Column(String, unique=True, nullable=False)  # Google Scholar's unique publication ID
    title_link = Column(Text, nullable=True)  # Link to the title
    pdf_link = Column(Text, nullable=True)  # Link to the PDF
    total_citations = Column(Integer, nullable=True)
    related_articles_url = Column(Text, nullable=True)
    all_versions_url = Column(Text, nullable=True)
    cites_id = Column(Text, nullable=True)

    publication: Mapped["Publication"] = relationship("Publication", backref="google_scholar_data")

    def __repr__(self):
        return f"<GoogleScholarPublication(publication_id={self.publication_id}, title_link={self.title_link})>"
