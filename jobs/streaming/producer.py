"""Phát lại điểm GPS vào Kafka theo nhịp thời gian (--speedup).

    python3 /opt/jobs/streaming/producer.py --replay /data/replay/2022-07-05_2022-07-05_2000xe --speedup 60
"""
import argparse
import glob
import os
import signal
import time

import pyarrow.parquet as pq
from confluent_kafka import Producer


def parse_args():
    ap = argparse.ArgumentParser(description="Phát lại điểm GPS vào Kafka")
    ap.add_argument("--replay", required=True, help="thư mục do prepare_replay.py tạo")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default="gps-raw")
    ap.add_argument("--speedup", type=float, default=60.0, help="hệ số tua nhanh; 0 = không chờ")
    ap.add_argument("--max-rate", type=float, default=0, help="message/giây tối đa; 0 = không giới hạn")
    ap.add_argument("--start-hour", help="bỏ qua các giờ trước giờ này (yyyyMMddHH)")
    ap.add_argument("--limit", type=int, default=0, help="dừng sau N message (thử nhanh)")
    ap.add_argument("--report-every", type=float, default=10.0, help="in tiến độ mỗi N giây")
    return ap.parse_args()


def hour_dirs(root, start_hour=None):
    dirs = sorted(glob.glob(os.path.join(root, "recv_hour=*")))
    if start_hour:
        dirs = [d for d in dirs if d.rsplit("=", 1)[1] >= start_hour]
    if not dirs:
        raise SystemExit(f"Không thấy thư mục recv_hour=* trong {root}")
    return dirs


def main():
    args = parse_args()
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
        "acks": "1",
    })
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    errors = {"n": 0}

    def on_delivery(err, _msg):
        if err is not None:
            errors["n"] += 1

    sent = 0
    wall0 = time.monotonic()
    data0 = None
    last_report, last_sent = wall0, 0
    data_now = None

    print(f"Phát {args.replay} -> {args.bootstrap}/{args.topic}, speedup={args.speedup}, "
          f"max_rate={args.max_rate or '∞'}", flush=True)
    for d in hour_dirs(args.replay, args.start_hour):
        for f in sorted(glob.glob(os.path.join(d, "*.parquet"))):
            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(batch_size=20000, columns=["key", "recv_ts", "value"]):
                keys = batch.column("key").to_pylist()
                ts = batch.column("recv_ts").cast("timestamp[us]").cast("int64").to_pylist()
                vals = batch.column("value").to_pylist()
                for k, t_us, v in zip(keys, ts, vals):
                    t = t_us / 1e6
                    if data0 is None:
                        data0 = t
                    data_now = t
                    # chờ tới đúng thời điểm của message
                    if args.speedup > 0:
                        wait = (t - data0) / args.speedup - (time.monotonic() - wall0)
                        if wait > 0.005:
                            producer.poll(0)
                            time.sleep(min(wait, 1.0))
                            while (t - data0) / args.speedup > time.monotonic() - wall0 and not stop["flag"]:
                                time.sleep(0.05)
                    if args.max_rate > 0:
                        ahead = sent / args.max_rate - (time.monotonic() - wall0)
                        if ahead > 0.005:
                            time.sleep(ahead)
                    while True:
                        try:
                            producer.produce(args.topic, key=str(k), value=v, on_delivery=on_delivery)
                            break
                        except BufferError:  # queue đầy
                            producer.poll(0.1)
                    sent += 1
                    if sent % 1000 == 0:
                        producer.poll(0)
                    now = time.monotonic()
                    if now - last_report >= args.report_every:
                        rate = (sent - last_sent) / (now - last_report)
                        print(f"  đã gửi {sent:,} | {rate:,.0f} msg/s | giờ dữ liệu "
                              f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data_now + 7 * 3600))} | lỗi {errors['n']}",
                              flush=True)
                        last_report, last_sent = now, sent
                    if stop["flag"] or (args.limit and sent >= args.limit):
                        break
                if stop["flag"] or (args.limit and sent >= args.limit):
                    break
            if stop["flag"] or (args.limit and sent >= args.limit):
                break
        if stop["flag"] or (args.limit and sent >= args.limit):
            break

    producer.flush(30)
    el = time.monotonic() - wall0
    print(f"Xong: {sent:,} message trong {el:,.0f}s ({sent / max(el, 1e-9):,.0f} msg/s), lỗi gửi {errors['n']}",
          flush=True)


if __name__ == "__main__":
    main()
