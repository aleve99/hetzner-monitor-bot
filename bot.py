from datetime import datetime, timedelta
from dotenv import dotenv_values
from pathlib import Path

from hcloud import Client
from hcloud.servers.client import BoundServer

import matplotlib.pyplot
import time, telebot, logging, threading, signal, matplotlib
import pandas as pd
import matplotlib.pyplot as plt

matplotlib.use('agg')

RUNNING = True
MAX_VALUES_MONITOR = 500

config = dotenv_values()

tmp = Path("./tmp")

if not tmp.exists():
    tmp.mkdir()

bot = telebot.TeleBot(
    token=config["TELEGRAM_TOKEN"],
    parse_mode="MARKDOWN",
    threaded=False
)

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
    bot.send_message(message.from_user.id, f"user chat id: {message.from_user.id}")

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
    lookback_period_s = 500
    step = lookback_period_s // MAX_VALUES_MONITOR

    while RUNNING:
        past_end = datetime.now().astimezone()
        past_start = past_end - timedelta(seconds=lookback_period_s)
        past_load = get_cpu_load(server, past_start, past_end, step=step)

        avg_load = past_load['load'].mean()
        std_dev = past_load['load'].std()
        
        load = past_load.iloc[-1]['load']
        lb = avg_load - (2 * std_dev)

        logger.info(f"Load: current {round(load, 4)}, average {round(avg_load, 4)}%, std dev {round(std_dev, 4)}% (lb {round(lb, 4)}%)")

        if load < lb :
            logger.warning(f"Current load {round(load, 4)}% below critical threshold!")

            fig, ax = plt.subplots(figsize=(10, 5))
            ax.plot(past_load['datetime'], past_load['load'], label='CPU Load')
            ax.set_title('CPU Load Over Time')
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
                    caption=f"Current load {round(load, 4)}% below critical threshold!"
                )

        time.sleep(10)
    
    logger.info("Closing monitor cpu")

def sigint_handler(signum, frame):
    global RUNNING
    RUNNING = False
    logger.info("Exiting program...")
    bot.stop_bot()
    matplotlib.pyplot.close()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)

    monitor_cpu_thread = threading.Thread(name="monitor_cpu", target=monitor_cpu)
    monitor_cpu_thread.start()

    bot.infinity_polling()
