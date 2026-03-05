# save as gen_big_log.py and run: python gen_big_log.py
import random, time
from datetime import datetime, timedelta

levels = ["DEBUG","INFO","WARNING","ERROR"]
sources = ["api","db","auth","worker","cache"]
start = datetime(2026,3,4,10,0,0)

n = 100_000
with open("big.log","w",encoding="utf-8") as f:
    t = start
    for i in range(1, n+1):
        t += timedelta(milliseconds=random.randint(1,1200))
        lvl = random.choices(levels, weights=[25,55,15,5])[0]
        src = random.choice(sources)
        msg = f"event_id={i} detail={random.randint(1,9999)}"
        f.write(f"{t:%Y-%m-%d %H:%M:%S},{t.microsecond//1000:03d} {lvl} {src}: {msg}\n")
print("Wrote big.log")