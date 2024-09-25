from hcloud import Client
from hcloud.servers.domain import MetricsType
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
SUSTAINED_PERIOD = 20  # Number of consecutive points to consider as a sustained anomaly

config = dotenv_values()

hetzner_client = Client(token=config["HCLOUD_TOKEN"])
server = hetzner_client.servers.get_by_name(config["SERVER_NAME"])

def get_stats(
    type: MetricsType,
    start: datetime,
    end: datetime, 
    step: int | None = None
) -> pd.DataFrame:

    response = server.get_metrics(
        type=type,
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

def analyze(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    new_df = df.copy(deep=True)

    # Calculate rolling statistics
    new_df['rolling_mean'] = new_df[col_name].rolling(window=WINDOW_SIZE).mean()
    new_df['rolling_std'] = new_df[col_name].rolling(window=WINDOW_SIZE).std()

    # Calculate z-scores
    new_df['z_score'] = (new_df[col_name] - new_df['rolling_mean']) / new_df['rolling_std']

    # Detect anomalies
    new_df['is_high_anomaly'] = new_df['z_score'] > HIGH_ANOMALY_THRESHOLD
    new_df['is_low_anomaly'] = new_df['z_score'] < LOW_ANOMALY_THRESHOLD
    new_df['is_anomaly'] = new_df['is_high_anomaly'] | new_df['is_low_anomaly']
    new_df['is_sustained_anomaly'] = abs(new_df['z_score']).rolling(window=SUSTAINED_PERIOD).mean() > SUSTAINED_ANOMALY_THRESHOLD

    return new_df

def save_cpu_plot(path: Path, df: pd.DataFrame) -> None:
    tdf = df.copy(deep=True)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(tdf['datetime'], tdf['cpu'], label='CPU Load')
    ax.plot(tdf['datetime'], tdf['rolling_mean'], label='Rolling Mean', color='orange')
    ax.fill_between(tdf['datetime'], 
                    tdf['rolling_mean'] - 2*tdf['rolling_std'], 
                    tdf['rolling_mean'] + 2*tdf['rolling_std'], 
                    alpha=0.2, color='orange', label=f'{SIGMA}σ Range')
    ax.scatter(tdf[tdf['is_high_anomaly']]['datetime'], 
               tdf[tdf['is_high_anomaly']]['cpu'], 
               color='red', label='High Anomalies')
    ax.scatter(tdf[tdf['is_low_anomaly']]['datetime'], 
               tdf[tdf['is_low_anomaly']]['cpu'], 
               color='blue', label='Low Anomalies')
    ax.set_title('CPU Load Over Time with Anomaly Detection')
    ax.set_xlabel('Time')
    ax.set_ylabel('CPU Load (%)')
    ax.legend()
    ax.grid(True)
    fig.savefig(path, dpi=200)
    plt.close(fig)

def save_disk_plot(path: Path, df: pd.DataFrame) -> None:
    tdf = df.copy(deep=True)
    fig, (ax1, ax2) = plt.subplots(nrows=2, figsize=(12, 6), sharex=True)

    tdf['rolling_iops_read'] = tdf['iops_read'].rolling(window=WINDOW_SIZE).mean()
    tdf['rolling_iops_write'] = tdf['iops_write'].rolling(window=WINDOW_SIZE).mean()
    tdf['rolling_bandwidth_read'] = tdf['bandwidth_read'].rolling(window=WINDOW_SIZE).mean() / 1024
    tdf['rolling_bandwidth_write'] = tdf['bandwidth_write'].rolling(window=WINDOW_SIZE).mean() / 1024

    ax1.plot(tdf['datetime'], tdf['rolling_iops_read'], label='Rolling IOPS Read', color='blue')
    ax1.plot(tdf['datetime'], tdf['rolling_iops_write'], label='Rolling IOPS Write', color='orange')
    ax1.plot(tdf['datetime'], tdf['iops_read'], color='blue', linewidth=1, label='IOPS Read', alpha=0.3)
    ax1.plot(tdf['datetime'], tdf['iops_write'], color='orange', linewidth=1, label='IOPS Write', alpha=0.3)
    ax1.set_ylabel('IOPS', color='black')
    ax1.tick_params(axis='y', labelcolor='black')
    ax1.set_title('Disk IOPS Over Time with Rolling Averages')
    ax1.legend()
    ax1.grid(True)

    ax2.plot(tdf['datetime'], tdf['rolling_bandwidth_read'], label='Rolling Bandwidth Read', color='green')
    ax2.plot(tdf['datetime'], tdf['rolling_bandwidth_write'], label='Rolling Bandwidth Write', color='red')
    ax2.plot(tdf['datetime'], tdf['bandwidth_read'] / 1024, color='green', linewidth=1, label='Bandwidth Read', alpha=0.3)
    ax2.plot(tdf['datetime'], tdf['bandwidth_write'] / 1024, color='coral', linewidth=1, label='Bandwidth Write', alpha=0.3)
    ax2.set_ylabel('Bandwidth (kB)', color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    ax2.set_title('Disk Bandwidth Over Time with Rolling Averages')
    ax2.legend()
    ax2.grid(True)
    
    ax2.set_xlabel('Time')
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)

def save_network_plot(path: Path, df: pd.DataFrame) -> None:
    tdf = df.copy(deep=True)
    fig, (ax1, ax2) = plt.subplots(nrows=2, figsize=(12, 6), sharex=True)

    tdf['rolling_pps_in'] = tdf['pps_in'].rolling(window=WINDOW_SIZE).mean()
    tdf['rolling_pps_out'] = tdf['pps_out'].rolling(window=WINDOW_SIZE).mean()
    tdf['rolling_bandwidth_in'] = tdf['bandwidth_in'].rolling(window=WINDOW_SIZE).mean() / 1024
    tdf['rolling_bandwidth_out'] = tdf['bandwidth_out'].rolling(window=WINDOW_SIZE).mean() / 1024

    ax1.plot(tdf['datetime'], tdf['rolling_pps_in'], label='Rolling PPS In', color='blue')
    ax1.plot(tdf['datetime'], tdf['rolling_pps_out'], label='Rolling PPS Out', color='orange')
    ax1.plot(tdf['datetime'], tdf['pps_in'], color='blue', linewidth=1, label='PPS In', alpha=0.3)
    ax1.plot(tdf['datetime'], tdf['pps_out'], color='orange', linewidth=1, label='PPS Out', alpha=0.3)
    ax1.set_ylabel('Packets per Second (PPS)', color='black')
    ax1.tick_params(axis='y', labelcolor='black')
    ax1.set_title('Network PPS Over Time with Rolling Averages')
    ax1.legend()
    ax1.grid(True)

    ax2.plot(tdf['datetime'], tdf['rolling_bandwidth_in'], label='Rolling Bandwidth In', color='green')
    ax2.plot(tdf['datetime'], tdf['rolling_bandwidth_out'], label='Rolling Bandwidth Out', color='red')
    ax2.plot(tdf['datetime'], tdf['bandwidth_in'] / 1024, color='green', linewidth=1, label='Bandwidth In', alpha=0.3)
    ax2.plot(tdf['datetime'], tdf['bandwidth_out'] / 1024, color='coral', linewidth=1, label='Bandwidth Out', alpha=0.3)
    ax2.set_ylabel('Bandwidth (kB)', color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    ax2.set_title('Network Bandwidth Over Time with Rolling Averages')
    ax2.legend()
    ax2.grid(True)
    
    ax2.set_xlabel('Time')
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)

if __name__ == "__main__":
    from datetime import timedelta

    end = datetime.now().astimezone()
    start = end - timedelta(minutes=10)

    df = get_stats('disk', start, end)
    save_disk_plot("disk.png", df)
    print(df)