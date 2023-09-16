import glob
import os
import matplotlib
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm
import duckdb


def exploration():
    print(duckdb.sql("""
    select median(CarSpeed) as median, MSR_id, max(CarSpeed) / median(CarSpeed) as diff
    from merged.parquet
    group by MSR_id
    having diff is not null and median > 80
    order by diff
    """))

    data = duckdb.sql("""
    select mean(CarSpeed) as CarSpeed, time from (
        select CarSpeed, time_bucket(INTERVAL '1 minutes', strptime(TimeStamp, '%Y-%M-%dT%H:%M:%S.000000Z')) as time
        from merged.parquet where MSR_id = 'CH:0139.01'
    )
    group by time
    order by time
        """)

    df = data.df()
    ax = df.set_index('time').loc['2023-01-05':'2023-01-07']['CarSpeed'].plot()
    #
    save(ax, "duck.pdf")

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


def main():
    df = pd.read_csv("../dataset/gubrist.csv")
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])

    ax = df.set_index('TimeStamp').loc['2023-08-11':'2023-08-12']['CarSpeed'].plot()

    save(ax)

    # ax = df['CarSpeed'].plot.hist(bins=10)
    #
    # x = round(df['CarSpeed'].dropna().value_counts().sort_values())
    # print(x)

    # save(ax)

    pass


if __name__ == '__main__':
    # main()

    # store_parquet()

    exploration()
