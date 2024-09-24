from datetime import datetime, timedelta

from get_stats import get_stats, MAX_METRICS_VALUES

STATS_TIMEFRAME_S = 2000
STEP = STATS_TIMEFRAME_S // MAX_METRICS_VALUES

def get_cpu_stats_text():
    end = datetime.now().astimezone()
    start = end - timedelta(seconds=STATS_TIMEFRAME_S)
    df = get_stats('cpu', start, end, STEP)

    current_value = df['cpu'].iloc[-1]

    text = f"*CPU load stats:*\n" + \
     f" _Load (act)_: {current_value:.2f}%\n" + \
     f" _Load (avg)_: {df['cpu'].mean():.2f}%\n" + \
     f" _Load (min)_: {df['cpu'].min():.2f}%\n" + \
     f" _Load (max)_: {df['cpu'].max():.2f}%\n" + \
     f" _Load (std)_: {df['cpu'].std():.2f}%"
    
    return text

def get_disk_stats_text():
    end = datetime.now().astimezone()
    start = end - timedelta(seconds=STATS_TIMEFRAME_S)
    df = get_stats('disk', start, end, STEP)

    text = f"*DISK stats:*\n" + \
     f" _IOPS read (avg)_: {df['iops_read'].mean():.2f} iop/s\n" + \
     f" _IOPS write (avg)_: {df['iops_write'].mean():.2f} iop/s\n" + \
     f" _IOPS read (std)_: {df['iops_read'].std():.2f} iop/s\n" + \
     f" _IOPS write (std)_: {df['iops_write'].std():.2f} iop/s\n" + \
     f" _LOAD read (avg)_: {df['bandwidth_read'].mean() / 1024:.2f} kB/s\n" + \
     f" _LOAD write (avg)_: {df['bandwidth_write'].mean() / 1024:.2f} kB/s\n" + \
     f" _LOAD read (std)_: {df['bandwidth_read'].std() / 1024:.2f} kB/s\n" + \
     f" _LOAD write (std)_: {df['bandwidth_write'].std() / 1024:.2f} kB/s\n"
    
    return text

def get_network_stats_text():
    end = datetime.now().astimezone()
    start = end - timedelta(seconds=STATS_TIMEFRAME_S)
    df = get_stats('network', start, end, STEP)

    text = f"*NETWORK stats:*\n" + \
     f" _PPS in (avg)_: {df['pps_in'].mean():.2f} packets/s\n" + \
     f" _PPS out (avg)_: {df['pps_out'].mean():.2f} packets/s\n" + \
     f" _PPS in (std)_: {df['pps_in'].std():.2f} packets/s\n" + \
     f" _PPS out (std)_: {df['pps_out'].std():.2f} packets/s\n" + \
     f" _LOAD in (avg)_: {df['bandwidth_in'].mean() / 1024:.2f} kB/s\n" + \
     f" _LOAD out (avg)_: {df['bandwidth_out'].mean() / 1024:.2f} kB/s\n" + \
     f" _LOAD in (std)_: {df['bandwidth_in'].std() / 1024:.2f} kB/s\n" + \
     f" _LOAD out (std)_: {df['bandwidth_out'].std() / 1024:.2f} kB/s\n"
    
    return text