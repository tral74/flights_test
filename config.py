from datetime import timedelta

from pandas import DataFrame
from pydantic import BaseSettings


class Settings(BaseSettings):
    csv_file_name: str = 'flight_stats.csv'
    cnt_threshold: int = 20
    duration_threshold: timedelta = timedelta(minutes=180)


settings = Settings()
