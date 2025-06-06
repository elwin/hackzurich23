import datetime
import glob
import os
import pathlib

import matplotlib
import pandas
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm
import duckdb

sensor_file = "stations.csv"
congestion_file = "congestion.parquet"
training_file = "training.parquet"


def sql(s):
    print(duckdb.sql(s))


def convert_csv_to_parquet(input_path, output_path):
    df = pd.read_csv(input_path, delimiter=";")
    df.to_parquet(output_path)


def initialize():
    sql(f"""
           COPY (
               select meta.MSR_id, MedianCarSpeed, Latitude, Longitude, Direction, Canton, Name, Direction, Street
               from (
                   select MSR_id, median(CarSpeed) as MedianCarSpeed
                   from merged.parquet
                   group by MSR_id
               ) as merged, SensorLocationMetaData.csv as meta
               where merged.MSR_id = meta.MSR_id
           ) TO '{sensor_file}' (HEADER)
       """)


def store_congested():
    congested = duckdb.sql(f"""
    select MSR_id, CarSpeed, CarFlow, MedianCarSpeed, round(CarSpeed / MedianCarSpeed, 2) as fraction, dayofweek(time) as weekday, hour(time) * 60 + minute(time) as minutes from (
                select merged.MSR_id as MSR_id, CarSpeed, MedianCarSpeed, CarFlow, strptime(TimeStamp, '%Y-%m-%dT%H:%M:%S.000000Z') as time
                from merged.parquet as merged join {sensor_file} as sensors on merged.MSR_id = sensors.MSR_id
            )
    where fraction < 0.7
    """)

    congested.to_parquet(congestion_file, compression='gzip')

    # sql("""
    #     select MSR_id, min(CarSpeed), max(CarSpeed)
    #     from (
    #         select MSR_id, time, median(CarSpeed) as CarSpeed
    #         from (
    #             select MSR_id, CarSpeed, CarFlow, time_bucket(INTERVAL '30 minutes', strptime(TimeStamp, '%Y-%M-%dT%H:%M:%S.000000Z')) as time
    #             from merged.parquet
    #         )
    #         group by MSR_id, time
    #     ) group by MSR_id
    # """)


def plot_for_point(msr_id, time_start="2023-01-05", time_end="2023-01-06"):
    for col in ["CarSpeed", "CarFlow"]:
        data = duckdb.sql(f"""
        select mean({col}) as Value, time from (
            select CarSpeed, CarFlow, time_bucket(INTERVAL '30 minutes', strptime(TimeStamp, '%Y-%M-%dT%H:%M:%S.000000Z')) as time
            from merged.parquet where MSR_id = '{msr_id}'
        )
        group by time
        order by time
        """)

        df = data.df()
        ax = df.set_index('time').loc[time_start:time_end]['Value'].plot()

        save(ax, f"{msr_id}_{col}.pdf")


def exploration():
    print(duckdb.sql("""
    select median(CarSpeed) as median, MSR_id, max(CarSpeed) / median(CarSpeed) as diff
    from merged.parquet
    group by MSR_id
    having diff is not null and median > 80
    order by diff
    """))

    pass


def store_parquet():
    files = glob.glob("dataset/*/*.csv")

    num = 0
    for batch in tqdm(list(chunks(files, 100))):
        csv_stream = pd.concat([pd.read_csv(file) for file in batch])
        csv_stream.to_parquet(f"dataset/parquet/{num}.parquet")
        num += 1


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def save(ax, name="plot.pdf"):
    fig = ax.get_figure()
    fig.savefig(name)


def generate_training():
    df = duckdb.sql(f"""
      select MSR_id, round(CarSpeed / MedianCarSpeed, 2) < 0.7 as congestion, time from (
                  select merged.MSR_id as MSR_id, CarSpeed, MedianCarSpeed, CarFlow, time_bucket(INTERVAL '30 minutes', strptime(TimeStamp, '%Y-%M-%dT%H:%M:%S.000000Z')) as time
                  from merged.parquet as merged join {sensor_file} as sensors on merged.MSR_id = sensors.MSR_id
              )
        where congestion is not null
      """)

    df.to_parquet(training_file, compression="gzip")


def evaluation1():
    start_time = datetime.datetime.strptime("2023-09-04 07:55:00", "%Y-%m-%d %H:%M:%S")
    sensor_id = "CH:0056.05"
    minutes = start_time.hour * 60 + start_time.minute
    threshold = 20

    sql(f"""
    select * from congestion.parquet
    where 1 = 1
    and MSR_id = 'CH:0056.05'
    and congestion.weekday not in (5, 6)
    and congestion.minutes < {minutes + threshold}
    and congestion.minutes > {minutes - threshold}
    and MSR_id = '{sensor_id}'
    order by congestion.minutes
    """)

def evaluation2():
    start_time = datetime.datetime.strptime("2023-09-07 21:55:00", "%Y-%m-%d %H:%M:%S")
    sensor_id = "CH:0342.01"
    weekday = start_time.weekday()
    minutes = start_time.hour * 60 + start_time.minute

    time_query = f"""
          and congestion.weekday = {weekday}
          and congestion.minutes = {minutes}
          """

    threshold = 60

    sql(f"""
    select * from congestion.parquet
    where 1 = 1
    and MSR_id = 'CH:0056.05'
    and congestion.minutes > {minutes - threshold}
    and congestion.minutes < {minutes + threshold}
    order by congestion.minutes
    """)


if __name__ == '__main__':
    evaluation1()
    evaluation2()

    if not pathlib.Path(sensor_file).is_file():
        initialize()

    if not pathlib.Path(congestion_file).is_file():
        store_congested()

    if not pathlib.Path(training_file).is_file():
        generate_training()

    # store_parquet()

    plot_for_point("CH:0542.02")

    # exploration()

    store_congested()
