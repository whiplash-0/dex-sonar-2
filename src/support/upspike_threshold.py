import asyncio

from sqlalchemy import Column, Float, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from src.config import parameters


DEFAULT_VALUE = 1.0
PRECISION = 10
TYPE = float


engine = create_async_engine(
    parameters.DATABASE_URL,
    future=True,
)
session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


Base = declarative_base()

class UpspikeThresholdTable(Base):
    __tablename__ = 'upspike_threshold'
    value = Column(Float, default=DEFAULT_VALUE, primary_key=True)


class UpspikeThreshold:
    value = None
    lock = asyncio.Lock()

    @classmethod
    async def init(cls):
        async with engine.begin() as c:
            await c.run_sync(Base.metadata.create_all)

        async with session() as s:
            row = (await s.execute(select(UpspikeThresholdTable))).scalars().first()

            if not row:
                row = UpspikeThresholdTable()
                s.add(row); await s.commit()

            cls.value = cls._truncate_rounding_error(row.value)

    @classmethod
    def get(cls) -> TYPE:
        return cls.value

    @classmethod
    async def set(cls, value: TYPE):
        async with cls.lock:
            cls.value = cls._truncate_rounding_error(value)
            async with session() as s:
                await s.execute(update(UpspikeThresholdTable).values(value=cls.value)); await s.commit()

    @staticmethod
    def _truncate_rounding_error(value):
        return round(value, PRECISION)
