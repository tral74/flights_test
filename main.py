from datetime import datetime, time, timedelta
from typing import Union

import pandas as pd
import uvicorn
from fastapi import FastAPI
from pandas import DataFrame
from pydantic import BaseModel

from config import settings

df: Union[DataFrame, None] = None

flights_info = {}

app = FastAPI()


def get_duration(departure: Union[time, str], arrival: Union[time, str]) -> timedelta:
    if type(departure) == str:
        departure = datetime.strptime(departure, '%H:%M')

    else:
        departure = datetime.strptime(departure.strftime('%H:%M'), '%H:%M')

    if type(arrival) == str:
        arrival = datetime.strptime(arrival, '%H:%M')

    else:
        arrival = datetime.strptime(arrival.strftime('%H:%M'), '%H:%M')

    return departure - arrival


# Models
class InputFlight(BaseModel):
    flight_id: str
    arrival: time
    departure: time


class OutputFlight(InputFlight):
    success: str


class Flights(BaseModel):
    flights: list[OutputFlight]


# Build initial data on startup
@app.on_event("startup")
async def on_startup():
    global df
    df = pd.read_csv(settings.csv_file_name)

    for index, row in df.iterrows():
        flight_id = row['flight ID']
        arrival = datetime.strptime(row['Arrival'], '%H:%M')
        departure = datetime.strptime(row['Departure'], '%H:%M')
        delta = departure - arrival
        success = row['success']

        cur_flight = flights_info.get(flight_id)

        if cur_flight:
            cur_flight['cnt'] += 1
            cur_flight['duration'] += delta

        else:
            flights_info[flight_id] = {'cnt': 1, 'duration': delta, 'success': success}


@app.get("/flights/{flight_id}", response_model=Flights)
async def get_flights(flight_id: str):
    global df
    flights = []

    result = df.loc[df['flight ID'] == flight_id]
    if not result.empty:
        result = result.sort_values(by='Arrival', ascending=True)

        for index, row in result.iterrows():
            flights.append(
                OutputFlight(
                    flight_id=row['flight ID'],
                    arrival=row['Arrival'],
                    departure=row['Departure'],
                    success=row['success'],
                )
            )

    return Flights(flights=flights)


@app.post("/flights/")
async def create_flight(flight: InputFlight):
    global df

    new_duration = get_duration(flight.departure, flight.arrival)

    # Search for flights with such ID
    result = df.loc[df['flight ID'] == flight.flight_id]

    #  If exists
    if not result.empty:
        result = result.sort_values(by='Arrival', ascending=True)

        index = result.index[0]

        old_duration = get_duration(df.iloc[index]['Departure'], df.iloc[index]['Arrival'])

        df.iloc[index] = [
            flight.flight_id,
            flight.arrival.strftime('%H:%M'),
            flight.departure.strftime('%H:%M'),
            'success'
        ]

        # Update `flights_info`
        flights_info[flight.flight_id]['duration'] = flights_info[flight.flight_id]['duration'] - old_duration + new_duration

    else:
        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    [[flight.flight_id, flight.arrival.strftime('%H:%M'), flight.departure.strftime('%H:%M')]],
                    columns=['flight ID', 'Arrival', 'Departure']
                )
            ],
            ignore_index=True
        )

        # Add data to `flights_info`
        flights_info[flight.flight_id] = {'cnt': 1, 'duration': new_duration}

    # Calculate current status
    if flights_info[flight.flight_id]['cnt'] <= settings.cnt_threshold and flights_info[flight.flight_id]['duration'] >= settings.duration_threshold:
        status = 'success'

    else:
        status = 'fail'

    # Update `success` for all flights with ID equals to `flight.flight_id`
    df.loc[df['flight ID'] == flight.flight_id, 'success'] = status

    df.to_csv(settings.csv_file_name, index=False)


if __name__ == "__main__":
    uvicorn.run('main:app', host="127.0.0.1", port=8000, reload=True, workers=8)
