"""
Neon PostgreSQL schema models (SQLAlchemy Core, used by Alembic migrations).
All raw OpenF1 data is stored at import time and never touched live again —
the demo is fully self-contained regardless of OpenF1 uptime.
"""
from sqlalchemy import (
    Column, Integer, Float, String, Boolean, DateTime, Text,
    MetaData, Table, UniqueConstraint
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
import os

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_async_engine(DATABASE_URL, echo=False, pool_size=5, max_overflow=10)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()


class RawDriver(Base):
    __tablename__ = "raw_drivers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_key = Column(Integer, nullable=False, index=True)
    driver_number = Column(Integer, nullable=False)
    full_name = Column(String(100))
    abbreviation = Column(String(10))
    team_name = Column(String(100))
    team_colour = Column(String(10))  # hex e.g. "3671C6"
    __table_args__ = (UniqueConstraint("session_key", "driver_number"),)


class RawLap(Base):
    __tablename__ = "raw_laps"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_key = Column(Integer, nullable=False, index=True)
    driver_number = Column(Integer, nullable=False, index=True)
    lap_number = Column(Integer, nullable=False)
    lap_duration = Column(Float)        # seconds
    duration_sector_1 = Column(Float)
    duration_sector_2 = Column(Float)
    duration_sector_3 = Column(Float)
    i1_speed = Column(Integer)          # km/h speed trap 1
    i2_speed = Column(Integer)
    st_speed = Column(Integer)          # finish straight speed
    is_pit_out_lap = Column(Boolean, default=False)
    date_start = Column(DateTime(timezone=True))
    __table_args__ = (UniqueConstraint("session_key", "driver_number", "lap_number"),)


class RawStint(Base):
    __tablename__ = "raw_stints"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_key = Column(Integer, nullable=False, index=True)
    driver_number = Column(Integer, nullable=False, index=True)
    stint_number = Column(Integer, nullable=False)
    lap_start = Column(Integer)
    lap_end = Column(Integer)
    compound = Column(String(20))       # SOFT, MEDIUM, HARD, INTER, WET
    tyre_age_at_start = Column(Integer, default=0)
    __table_args__ = (UniqueConstraint("session_key", "driver_number", "stint_number"),)


class RawPit(Base):
    __tablename__ = "raw_pits"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_key = Column(Integer, nullable=False, index=True)
    driver_number = Column(Integer, nullable=False, index=True)
    lap_number = Column(Integer, nullable=False)
    pit_duration = Column(Float)        # total pit lane time (s)
    date = Column(DateTime(timezone=True))
    __table_args__ = (UniqueConstraint("session_key", "driver_number", "lap_number"),)


class RawInterval(Base):
    __tablename__ = "raw_intervals"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_key = Column(Integer, nullable=False, index=True)
    driver_number = Column(Integer, nullable=False, index=True)
    lap_number = Column(Integer, nullable=False)  # computed from date alignment
    gap_to_leader = Column(Float)       # seconds; None if leader
    interval = Column(Float)           # gap to car directly ahead
    __table_args__ = (UniqueConstraint("session_key", "driver_number", "lap_number"),)


class RawPosition(Base):
    __tablename__ = "raw_positions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_key = Column(Integer, nullable=False, index=True)
    driver_number = Column(Integer, nullable=False, index=True)
    lap_number = Column(Integer, nullable=False)
    position = Column(Integer)
    __table_args__ = (UniqueConstraint("session_key", "driver_number", "lap_number"),)


class RawRaceControl(Base):
    __tablename__ = "raw_race_control"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_key = Column(Integer, nullable=False, index=True)
    lap_number = Column(Integer)
    category = Column(String(50))       # Flag, SafetyCar, Drs, etc.
    flag = Column(String(30))           # GREEN, YELLOW, RED, SC, VSC
    message = Column(Text)
    date = Column(DateTime(timezone=True))
