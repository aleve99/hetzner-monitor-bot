from hcloud import Client
from dotenv import dotenv_values
from datetime import datetime
from pathlib import Path
from functools import reduce

import matplotlib.pyplot as plt
import pandas as pd


MAX_METRICS_VALUES = 500
WINDOW_SIZE = 10

# Define anomaly thresholds
SIGMA = 3
HIGH_ANOMALY_THRESHOLD = SIGMA  # Z-score threshold for high usage anomalies
LOW_ANOMALY_THRESHOLD = -SIGMA  # Z-score threshold for low usage anomalies
SUSTAINED_ANOMALY_THRESHOLD = 2  # Absolute Z-score threshold for sustained anomalies
SUSTAINED_PERIOD = 10  # Number of consecutive points to consider as a sustained anomaly

config = dotenv_values()

hetzner_client = Client(token=config["HCLOUD_TOKEN"])
server = hetzner_client.servers.get_by_name(config["SERVER_NAME"])

def analyze(df: pd.DataFrame) -> pd.DataFrame:
    new_df = df.copy(deep=True)

    # Calculate rolling statistics
    new_df['rolling_mean'] = new_df['load'].rolling(window=WINDOW_SIZE).mean()
    new_df['rolling_std'] = new_df['load'].rolling(window=WINDOW_SIZE).std()

    # Calculate z-scores
    new_df['z_score'] = (new_df['load'] - new_df['rolling_mean']) / new_df['rolling_std']

    # Detect anomalies
    new_df['is_high_anomaly'] = new_df['z_score'] > HIGH_ANOMALY_THRESHOLD
    new_df['is_low_anomaly'] = new_df['z_score'] < LOW_ANOMALY_THRESHOLD
    new_df['is_anomaly'] = new_df['is_high_anomaly'] | new_df['is_low_anomaly']
    new_df['is_sustained_anomaly'] = abs(new_df['z_score']).rolling(window=SUSTAINED_PERIOD).mean() > SUSTAINED_ANOMALY_THRESHOLD

    return new_df

def get_cpu_load(
    start: datetime,
    end: datetime, 
    step: int | None = None
) -> pd.DataFrame:

    response = server.get_metrics(
        type="cpu",
        start=start,
        end=end,
        step=step
    )

    df = pd.DataFrame(
        data=response.metrics.time_series['cpu']["values"],
        columns=["datetime", "load"],
        dtype="float"
    )
    
    df['datetime'] = pd.to_datetime(
        df['datetime'].astype(int), unit='s', utc=True
    )

    return df

def save_cpu_plot(path: Path, load_df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(load_df['datetime'], load_df['load'], label='CPU Load')
    ax.plot(load_df['datetime'], load_df['rolling_mean'], label='Rolling Mean', color='orange')
    ax.fill_between(load_df['datetime'], 
                    load_df['rolling_mean'] - 2*load_df['rolling_std'], 
                    load_df['rolling_mean'] + 2*load_df['rolling_std'], 
                    alpha=0.2, color='orange', label=f'{SIGMA}σ Range')
    ax.scatter(load_df[load_df['is_high_anomaly']]['datetime'], 
               load_df[load_df['is_high_anomaly']]['load'], 
               color='red', label='High Anomalies')
    ax.scatter(load_df[load_df['is_low_anomaly']]['datetime'], 
               load_df[load_df['is_low_anomaly']]['load'], 
               color='blue', label='Low Anomalies')
    ax.set_title('CPU Load Over Time with Anomaly Detection')
    ax.set_xlabel('Time')
    ax.set_ylabel('CPU Load (%)')
    ax.legend()
    ax.grid(True)
    fig.savefig(path, dpi=300)
    plt.close(fig)

def get_disk_load(
    start: datetime,
    end: datetime, 
    step: int | None = None
) -> pd.DataFrame:

    response = server.get_metrics(
        type="disk",
        start=start,
        end=end,
        step=step
    )

    dfs = []
    for key in response.metrics.time_series:
        col_name = '_'.join(key.split(".")[-2:])

        dfs.append(pd.DataFrame(
            data=response.metrics.time_series[key]["values"],
            columns=["datetime", col_name],
            dtype="float"
        ))

    df = reduce(lambda left, right: pd.merge(left, right, on='datetime', how='outer'), dfs)

    df['datetime'] = pd.to_datetime(
        df['datetime'].astype(int), unit='s', utc=True
    )

    return df


if __name__ == "__main__":
    from datetime import timedelta

    end = datetime.now().astimezone()
    start = end - timedelta(minutes=10)

    df = get_disk_load(start, end)
    print(df)