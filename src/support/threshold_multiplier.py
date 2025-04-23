import asyncio
import subprocess

from sqlalchemy import Column, Float, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from src.config.config import CONFIG


DEFAULT_VALUE = 1.0
PRECISION = 10
TYPE = float


def get_database_url():
    result = subprocess.run(
        ['heroku', 'config:get', 'DATABASE_URL', '-a', CONFIG.get('Heroku', 'app name')],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode == 0:
        return result.stdout.strip().replace('postgres://', 'postgresql+asyncpg://', 1)
    else:
        raise ValueError(f'Error fetching `DATABASE_URL`: {result.stderr.strip()}')

engine = create_async_engine(
    get_database_url(),
    future=True,
)
session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


Base = declarative_base()

class ThresholdMultiplierTable(Base):
    __tablename__ = 'threshold_multiplier'
    value = Column(Float, default=DEFAULT_VALUE, primary_key=True)


class ThresholdMultiplier:
    value = None
    lock = asyncio.Lock()

    @classmethod
    async def init(cls):
        async with engine.begin() as c:
            await c.run_sync(Base.metadata.create_all)

        async with session() as s:
            row = (await s.execute(select(ThresholdMultiplierTable))).scalars().first()

            if not row:
                row = ThresholdMultiplierTable()
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
                await s.execute(update(ThresholdMultiplierTable).values(value=cls.value)); await s.commit()

    @staticmethod
    def _truncate_rounding_error(value):
        return round(value, PRECISION)
