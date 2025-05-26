import re

from sqlalchemy import Column, Float, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from src.config import parameters


DEFAULT_VALUE = 1.0
PRECISION = 10


Type = float



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


class AutoTablenameMixin:
    """
    Automatically defines necessary cls.__tablename__.
    Assumes 'Table' word at the end of a name
    """
    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.__tablename__ = re.sub(r'([a-z])([A-Z])', r'\1_\2', cls.__name__[:-5]).lower()


class UpspikeThresholdTable(AutoTablenameMixin, Base):
    value = Column(Float, default=DEFAULT_VALUE, primary_key=True)



class UpspikeThreshold:
    cached_value = None

    @classmethod
    async def init(cls):
        async with engine.begin() as c:  # ensure all tables exist
            await c.run_sync(Base.metadata.create_all)

        async with session() as s:
            row = (await s.execute(select(UpspikeThresholdTable))).scalars().first()

            if not row:
                row = UpspikeThresholdTable()
                s.add(row); await s.commit()

            cls.cached_value = cls._truncate_rounding_error(row.value)

    @classmethod
    def get_name(cls, title_case=False, separator=' '):
        name_tokens = [x.lower() for x in re.findall(r'[A-Z][a-z]*', cls.__name__)]
        return separator.join(
            [name_tokens[0].title(), *name_tokens[1:]]
            if title_case else
            name_tokens
        )

    @classmethod
    def get(cls) -> Type:
        return cls.cached_value

    @classmethod
    async def set(cls, value: Type):
        async with session() as s:
            cls.cached_value = cls._truncate_rounding_error(value)
            await s.execute(
                update(UpspikeThresholdTable)
                .values(value=cls.cached_value)
            ); await s.commit()

    @staticmethod
    def _truncate_rounding_error(value):
        return round(value, PRECISION)
