import datetime
import enum
import sqlalchemy as db
import os
from config import POSTGRESQL_URL

metadata = db.MetaData()


class RoleEnum(str, enum.Enum):
    user = "user"
    system = "system"


class CSATEnum(str, enum.Enum):
    very_satisfied = "5"
    satisfied = "4"
    neutral = "3"
    dissatisfied = "2"
    very_dissatisfied = "1"


# Tables

Dialogue = db.Table(
    "dialogues",
    metadata,
    db.Column("id", db.Integer(), primary_key=True, autoincrement=True),
    db.Column("record_id", db.UUID(), nullable=True),
    db.Column("conversation_id", db.UUID(), nullable=True),
    db.Column("app_id", db.String(), nullable=True),
    db.Column("main_input", db.String(), nullable=True),
    db.Column("main_output", db.String(), nullable=True),
    db.Column("main_error", db.String(), nullable=True),
    db.Column("perf", db.JSON(), nullable=True),
    db.Column("calls", db.ARRAY(db.JSON), nullable=True),
    db.Column("created_at", db.DateTime(), default=datetime.datetime.utcnow)
)

Session = db.Table(
    "sessions",
    metadata,
    db.Column("id", db.Integer(), primary_key=True, autoincrement=True),
    db.Column("session_id", db.UUID()),
    db.Column("role", db.Enum(RoleEnum)),
    db.Column("content", db.Text()),
    db.Column("created_at", db.DateTime(), default=datetime.datetime.utcnow)
)

Feedback = db.Table(
    "feedbacks",
    metadata,
    db.Column("id", db.Integer(), primary_key=True, autoincrement=True),
    db.Column("session_id", db.UUID()),
    db.Column("content", db.Text(), nullable=True),
    db.Column("rating", db.Enum(CSATEnum)),
    db.Column("conversations", db.ARRAY(db.JSON)),
    db.Column("created_at", db.DateTime(), default=datetime.datetime.utcnow)
)

connection = db.create_engine(POSTGRESQL_URL)


def get_session():
    return connection


def init_database():
    metadata.create_all(connection)
