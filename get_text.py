from datetime import datetime, timedelta

from get_stats import get_cpu_load


def get_cpu_stats_text():
    end = datetime.now().astimezone()
    start = end - timedelta(seconds=500)
    cpu_load_df = get_cpu_load(start=start, end=end)
    current_value = cpu_load_df['load'].iloc[-1]
    text = f"*CPU load stats:*\n" + \
     f" _Data points_: {cpu_load_df['load'].count()}\n" + \
     f" _Load (act)_: {current_value:.2f}%\n" + \
     f" _Load (avg)_: {cpu_load_df['load'].mean():.2f}%\n" + \
     f" _Load (min)_: {cpu_load_df['load'].min():.2f}%\n" + \
     f" _Load (max)_: {cpu_load_df['load'].max():.2f}%\n" + \
     f" _Load (std)_: {cpu_load_df['load'].std():.2f}%"
    
    return text