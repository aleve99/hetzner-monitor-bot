from datetime import datetime, timedelta
from dotenv import dotenv_values
from pathlib import Path
from collections import deque

import telebot
from telebot import types

from get_stats import get_cpu_load, analyze, save_cpu_plot, MAX_METRICS_VALUES
from get_text import get_cpu_stats_text

import time, logging, threading, signal
import pandas as pd
import matplotlib.pyplot as plt

plt.switch_backend('agg')

RUNNING = True
CHECK_INTERVAL_S = 20
LOAD_PLOT_FILENAME = "load.png"

config = dotenv_values()

tmp = Path("./tmp")

tmp.mkdir(exist_ok=True)

bot = telebot.TeleBot(
    token=config["TELEGRAM_TOKEN"],
    parse_mode="Markdown"
)

messages: deque[int] = deque([], 10)

chat_id = int(config['TELEGRAM_CHAT_ID'])

telebot.logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

cpu_cmd = "\U0001F39B " + "CPU"
disk_cmd = "\U0001F4BE " + "DISK"
network_cmd = "\U0001F4E1 " + "NETWORK"

menu_markup = types.ReplyKeyboardMarkup()
cpu_kb = types.KeyboardButton(cpu_cmd)
disk_kb = types.KeyboardButton(disk_cmd)
network_kb = types.KeyboardButton(network_cmd)
menu_markup.row(cpu_kb, disk_kb, network_kb)

menu_cmd = "\U0001F519 Back"
cpu_plot_cmd = "\U0001F4C8 Plot"
cpu_stats_menu_markup = types.ReplyKeyboardMarkup()
cpu_stats_menu_markup.row(
    types.KeyboardButton(cpu_plot_cmd),
    types.KeyboardButton(menu_cmd)
)

cpu_stats_update_markup = types.InlineKeyboardMarkup()
cpu_stats_update_markup.row(
    types.InlineKeyboardButton(text=("Update"), callback_data="cpu_stats_update")
)

cpu_plot_markup = types.InlineKeyboardMarkup()
cpu_plot_markup.row(
    types.InlineKeyboardButton(text="30m", callback_data="cpuplot_30m"),
    types.InlineKeyboardButton(text="1h", callback_data="cpuplot_1h"),
    types.InlineKeyboardButton(text="3h", callback_data="cpuplot_3h"),
    types.InlineKeyboardButton(text="6h", callback_data="cpuplot_6h"),
    types.InlineKeyboardButton(text="12h", callback_data="cpuplot_12h"),
    types.InlineKeyboardButton(text="1d", callback_data="cpuplot_1d")
)

disk_load = types.InlineKeyboardMarkup()
disk_load.row(
    types.InlineKeyboardButton(text="Plot")
)

disk_load_plot = types.InlineKeyboardMarkup()
disk_load_plot.row(
    types.InlineKeyboardButton(text=("30m"), callback_data="diskplot_30m"),
    types.InlineKeyboardButton(text=("1h"), callback_data="diskplot_1h"),
    types.InlineKeyboardButton(text=("3h"), callback_data="diskplot_3h"),
    types.InlineKeyboardButton(text=("6h"), callback_data="diskplot_6h"),
    types.InlineKeyboardButton(text=("12h"), callback_data="diskplot_12h"),
    types.InlineKeyboardButton(text=("1d"), callback_data="diskplot_1d"),
)

network_load = types.InlineKeyboardMarkup()
network_load.row(
    types.InlineKeyboardButton(text="Plot")
)

network_load_plot = types.InlineKeyboardMarkup()
network_load_plot.row(
    types.InlineKeyboardButton(text=("30m"), callback_data="networkplot_30m"),
    types.InlineKeyboardButton(text=("1h"), callback_data="networkplot_1h"),
    types.InlineKeyboardButton(text=("3h"), callback_data="networkplot_3h"),
    types.InlineKeyboardButton(text=("6h"), callback_data="networkplot_6h"),
    types.InlineKeyboardButton(text=("12h"), callback_data="networkplot_12h"),
    types.InlineKeyboardButton(text=("1d"), callback_data="networkplot_1d"),
)

@bot.message_handler(commands=["start"])
def welcome_user(message: types.Message):
    if message.from_user.id == chat_id:
        bot.send_message(chat_id, f"Hello, {message.from_user.full_name}!" ,reply_markup=menu_markup)

@bot.message_handler(func=lambda message: message.text == menu_cmd)
def main_menu(message: types.Message):
    if message.from_user.id == chat_id:
        bot.send_message(chat_id, f"Main menu:" ,reply_markup=menu_markup)


@bot.message_handler(func=lambda message: message.text == cpu_cmd)
def command_cpu(message: types.Message):
    if message.from_user.id == chat_id:
        bot.send_chat_action(message.from_user.id, action="typing")
        
        text = get_cpu_stats_text()
        
        bot.send_message(message.from_user.id, text=text, reply_markup=cpu_stats_update_markup)
        bot.send_message(message.from_user.id, text="Cpu menu:", reply_markup=cpu_stats_menu_markup)

@bot.callback_query_handler(func=lambda call: call.data == "cpu_stats_update")
def cpu_stats_update(call: types.CallbackQuery):
    if call.from_user.id == chat_id:
        text = get_cpu_stats_text()

        bot.edit_message_text(text, call.from_user.id, call.message.id, reply_markup=cpu_stats_update_markup)

@bot.message_handler(func=lambda message: message.text == cpu_plot_cmd)
def cpu_plot(call: types.CallbackQuery):
    if call.from_user.id == chat_id:
        bot.send_chat_action(call.from_user.id, "upload_photo")
        lookback_period_s = 30 * 60 # 30 minutes default
        step = lookback_period_s // MAX_METRICS_VALUES
        end = datetime.now().astimezone()
        start = end - timedelta(seconds=lookback_period_s)

        load = analyze(
            get_cpu_load(start, end, step)
        )

        save_cpu_plot(tmp / LOAD_PLOT_FILENAME, load)
        with open(tmp / LOAD_PLOT_FILENAME, 'rb') as image:
            bot.send_photo(
                chat_id=call.from_user.id,
                photo=image,
                caption=f"*{start.strftime('%Y-%m-%d %H:%M')} -> {end.strftime('%Y-%m-%d %H:%M')}*",
                reply_markup=cpu_plot_markup
            )

@bot.callback_query_handler(func=lambda call: isinstance(call.data, str) and call.data.startswith('cpuplot_'))
def cpu_plot_update(call: types.CallbackQuery):
    if call.from_user.id == chat_id:
        p = call.data[-1]
        n = int(call.data.split('_')[-1][:-1])
        if p == "m": # case minutes
            lookback_period_s = n * 60
        elif p == "h": # case hours
            lookback_period_s = n * 60 * 60
        elif p == "d": # case days
            lookback_period_s = n * 24 * 60 * 60
        
        step = lookback_period_s // MAX_METRICS_VALUES
        end = datetime.now().astimezone()
        start = end - timedelta(seconds=lookback_period_s)

        load = analyze(
            get_cpu_load(start, end, step)
        )

        save_cpu_plot(tmp / LOAD_PLOT_FILENAME, load)
        with open(tmp / LOAD_PLOT_FILENAME, 'rb') as image:
            bot.edit_message_media(
                media=types.InputMediaPhoto(
                    media=image,
                    caption=f"*{start.strftime('%Y-%m-%d %H:%M')} -> {end.strftime('%Y-%m-%d %H:%M')}*",
                    parse_mode="Markdown"
                ), 
                chat_id=call.from_user.id, 
                message_id=call.message.id,
                reply_markup=cpu_plot_markup
            )

def sleep_wait_run():
    i = 0
    while RUNNING and i < CHECK_INTERVAL_S:
        time.sleep(1)
        i += 1

def monitor_cpu():
    lookback_period_s = 2000
    step = lookback_period_s // MAX_METRICS_VALUES

    while RUNNING:
        past_end = datetime.now().astimezone()
        past_start = past_end - timedelta(seconds=lookback_period_s)

        try:
            load = get_cpu_load(past_start, past_end, step=step)
            load = analyze(load)
        except Exception as e:
            logger.error(e)
            sleep_wait_run()
            continue
        
        # Check for anomalies in the most recent data point
        latest_point = load.iloc[-1]
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

            save_cpu_plot(tmp / LOAD_PLOT_FILENAME, load)

            with open(tmp / LOAD_PLOT_FILENAME, 'rb') as photo:
                bot.send_photo(
                    chat_id=config["TELEGRAM_CHAT_ID"],
                    photo=photo,
                    caption=f"{anomaly_type} detected! Current load: {round(latest_point['load'], 2)}%, Z-score: {round(latest_point['z_score'], 2)}"
                )

        sleep_wait_run()
    
    logger.info(f"{threading.current_thread().name} closed!")

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