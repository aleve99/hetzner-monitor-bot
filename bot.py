from datetime import datetime, timedelta
from dotenv import dotenv_values
from pathlib import Path

from hcloud import Client
from hcloud.servers.client import BoundServer

import time, telebot, logging, threading, signal
import pandas as pd
import matplotlib.pyplot as plt

plt.switch_backend('agg')

RUNNING = True
MAX_VALUES_MONITOR = 500

config = dotenv_values()

tmp = Path("./tmp")

if not tmp.exists():
    tmp.mkdir()

bot = telebot.TeleBot(
    token=config["TELEGRAM_TOKEN"],
    parse_mode="MARKDOWN"
)

chat_id = int(config['TELEGRAM_CHAT_ID'])

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

hetzner_client = Client(token=config["HCLOUD_TOKEN"])
server = hetzner_client.servers.get_by_name(config["SERVER_NAME"])

@bot.message_handler(commands=["start", "help"])
def send_welcome(message: telebot.types.Message):
    if message.from_user.id == chat_id:
        bot.send_message(chat_id, "Bot running!")

def get_cpu_load(
    server: BoundServer, 
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

def monitor_cpu():
    filename_fig = "load_plot.png"
    lookback_period_s = 3600  # 1 hour
    step = lookback_period_s / MAX_VALUES_MONITOR  # 1 minute
    window_size = 10  # 10-minute rolling window

    while RUNNING:
        past_end = datetime.now().astimezone()
        past_start = past_end - timedelta(seconds=lookback_period_s)

        try:
            past_load = get_cpu_load(server, past_start, past_end, step=step)
        except:
            time.sleep(60)
            continue

        # Calculate rolling statistics
        past_load['rolling_mean'] = past_load['load'].rolling(window=window_size).mean()
        past_load['rolling_std'] = past_load['load'].rolling(window=window_size).std()

        # Calculate z-scores
        past_load['z_score'] = (past_load['load'] - past_load['rolling_mean']) / past_load['rolling_std']

        # Define anomaly thresholds
        high_anomaly_threshold = 3  # Z-score threshold for high usage anomalies
        low_anomaly_threshold = -2  # Z-score threshold for low usage anomalies
        sustained_anomaly_threshold = 2  # Absolute Z-score threshold for sustained anomalies
        sustained_period = 5  # Number of consecutive points to consider as a sustained anomaly

        # Detect anomalies
        past_load['is_high_anomaly'] = past_load['z_score'] > high_anomaly_threshold
        past_load['is_low_anomaly'] = past_load['z_score'] < low_anomaly_threshold
        past_load['is_anomaly'] = past_load['is_high_anomaly'] | past_load['is_low_anomaly']
        past_load['is_sustained_anomaly'] = abs(past_load['z_score']).rolling(window=sustained_period).mean() > sustained_anomaly_threshold

        # Check for anomalies in the most recent data point
        latest_point = past_load.iloc[-1]
        is_high_anomaly = latest_point['is_high_anomaly']
        is_low_anomaly = latest_point['is_low_anomaly']
        is_sustained_anomaly = latest_point['is_sustained_anomaly']

        logger.info(f"Load: current {round(latest_point['load'], 2)}%, Z-score: {round(latest_point['z_score'], 2)}")

        if is_high_anomaly or is_low_anomaly or is_sustained_anomaly:
            if is_high_anomaly:
                anomaly_type = "High CPU usage spike"
            elif is_low_anomaly:
                anomaly_type = "Low CPU usage detected"
            else:
                anomaly_type = "Sustained unusual behavior"
            
            logger.warning(f"{anomaly_type} detected in CPU usage!")

            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(past_load['datetime'], past_load['load'], label='CPU Load')
            ax.plot(past_load['datetime'], past_load['rolling_mean'], label='Rolling Mean', color='orange')
            ax.fill_between(past_load['datetime'], 
                            past_load['rolling_mean'] - 2*past_load['rolling_std'], 
                            past_load['rolling_mean'] + 2*past_load['rolling_std'], 
                            alpha=0.2, color='orange', label='2σ Range')
            ax.scatter(past_load[past_load['is_high_anomaly']]['datetime'], 
                       past_load[past_load['is_high_anomaly']]['load'], 
                       color='red', label='High Anomalies')
            ax.scatter(past_load[past_load['is_low_anomaly']]['datetime'], 
                       past_load[past_load['is_low_anomaly']]['load'], 
                       color='blue', label='Low Anomalies')
            ax.set_title('CPU Load Over Time with Anomaly Detection')
            ax.set_xlabel('Time')
            ax.set_ylabel('CPU Load (%)')
            ax.legend()
            ax.grid(True)
            fig.savefig(tmp / filename_fig)
            plt.close(fig)

            with open(tmp / filename_fig, 'rb') as photo:
                bot.send_photo(
                    chat_id=config["TELEGRAM_CHAT_ID"],
                    photo=photo,
                    caption=f"{anomaly_type} detected! Current load: {round(latest_point['load'], 2)}%, Z-score: {round(latest_point['z_score'], 2)}"
                )

        time.sleep(60)
    
    logger.info("Closing monitor cpu")

def sigint_handler(signum, frame):
    global RUNNING
    RUNNING = False
    logger.info("Exiting program...")
    bot.stop_bot()
    plt.close('all')

if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)

    monitor_cpu_thread = threading.Thread(name="monitor_cpu", target=monitor_cpu)
    monitor_cpu_thread.start()

    bot.infinity_polling()