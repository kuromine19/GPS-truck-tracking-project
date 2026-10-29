#!/usr/bin/env python3
"""Sinh dữ liệu GPS xe tải GIẢ LẬP, đúng định dạng file dump PostgreSQL mẫu
(mỗi xe một bảng, mỗi giờ một dòng INSERT, mảng JSON các điểm t0/t1/cb/lt/ln/st/ag/km/vl/de).

Chỉ dùng thư viện chuẩn Python, chạy thẳng trên máy (WSL), không cần container.

Ví dụ:
    # Có OSRM (xe chạy theo đường thật):
    python3 scripts/generate_fake_dump.py --vehicles 10 --start-date 2026-10-11 --days 1 \
        --osrm http://localhost:5000 --output data/fake/fake_10xe_1ngay.sql

    # Chưa có OSRM (xe đi đường thẳng, CHỈ để thử code):
    python3 scripts/generate_fake_dump.py --vehicles 10 --days 1 --no-osrm \
        --output data/fake/test.sql

Kết quả kèm theo file <output>.truth.json ghi lại "đáp án": loại xe, các đoạn cố ý vượt tốc độ,
số lỗi đã chèn – dùng để kiểm tra các job làm sạch và phát hiện vượt tốc độ.

Hành vi mô phỏng (dựa trên 2 mẫu thật của nhóm):
  - Xe nội tỉnh: mỗi ngày 2–5 điểm giao quanh kho, dừng 15–60 phút mỗi điểm, chiều về kho.
  - Xe liên tỉnh: chạy giữa các thành phố, dỡ hàng 1–2,5 giờ, nghỉ 30–45 phút sau mỗi ~4 giờ lái.
  - Xe chạy: 10–30 giây một điểm. Xe đỗ: ~1 điểm/phút, t0 chỉ cập nhật ~2 phút một lần (trùng t0).
  - Độ trễ t1 - t0: 2–20 giây, thỉnh thoảng 30–120 giây.
  - Lỗi chèn ngẫu nhiên: điểm nhảy, giờ thiết bị sai, tọa độ (0,0), bản ghi trùng.
  - Mã trạng thái st: 0 = đỗ, 1 = chạy (GIẢ ĐỊNH, mã thật chưa xác nhận).
  - Biển số dạng "51FK0001" – chữ FK để không trùng biển số thật.
"""
import argparse
import bisect
import json
import math
import os
import random
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta

# tên: (vĩ độ, kinh độ, mã tỉnh cho biển số giả)
CITIES = {
    "Ha Noi": (21.0285, 105.8542, 29),
    "Hai Phong": (20.8449, 106.6881, 15),
    "Vinh": (18.6796, 105.6813, 37),
    "Hue": (16.4637, 107.5909, 75),
    "Da Nang": (16.0544, 108.2022, 43),
    "Quy Nhon": (13.7820, 109.2197, 77),
    "Pleiku": (13.9833, 108.0000, 81),
    "Buon Ma Thuot": (12.6667, 108.0500, 47),
    "Nha Trang": (12.2388, 109.1967, 79),
    "TP HCM": (10.7769, 106.7009, 51),
    "Can Tho": (10.0452, 105.7469, 65),
}

TRUCK_SPEED_FACTOR = 0.85   # xe tải chạy chậm hơn tốc độ đường mà OSRM tính cho xe con
TRUCK_MAX_KMH = 90
EARTH_R = 6371000.0


# ---------------------------------------------------------------- hình học
def haversine_m(a, b):
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_R * math.asin(math.sqrt(h))


def bearing_deg(a, b):
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    y = math.sin(lon2 - lon1) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)
    return int((math.degrees(math.atan2(y, x)) + 360) % 360)


def offset_point(rng, p, min_km, max_km):
    d = rng.uniform(min_km, max_km) * 1000
    ang = rng.uniform(0, 2 * math.pi)
    dlat = d * math.cos(ang) / 111320
    dlon = d * math.sin(ang) / (111320 * math.cos(math.radians(p[0])))
    return (p[0] + dlat, p[1] + dlon)


# ---------------------------------------------------------------- tìm đường
class Router:
    def __init__(self, osrm_url, rng):
        self.url = osrm_url.rstrip("/") if osrm_url else None
        self.rng = rng
        self.osrm_ok = 0
        self.fallbacks = 0

    def route(self, a, b):
        """Trả về (danh sách tọa độ [(lat, lon)], vận tốc từng đoạn km/h)."""
        if self.url:
            try:
                return self._osrm(a, b)
            except (urllib.error.URLError, KeyError, ValueError, IndexError, TimeoutError) as exc:
                self.fallbacks += 1
                if self.fallbacks <= 3:
                    print(f"  ! OSRM lỗi ({exc}), dùng đường thẳng cho chuyến này", file=sys.stderr)
        return self._straight(a, b)

    def _osrm(self, a, b):
        url = (f"{self.url}/route/v1/driving/{a[1]:.6f},{a[0]:.6f};{b[1]:.6f},{b[0]:.6f}"
               "?overview=full&geometries=geojson&annotations=speed")
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.load(resp)
        if data.get("code") != "Ok":
            raise ValueError(data.get("code"))
        r = data["routes"][0]
        coords = [(lat, lon) for lon, lat in r["geometry"]["coordinates"]]
        speeds = r["legs"][0]["annotation"]["speed"]          # m/s, mỗi đoạn giữa 2 nút
        if len(speeds) != len(coords) - 1:                     # phòng khi số đoạn không khớp
            avg = r["distance"] / max(r["duration"], 1)
            speeds = [avg] * (len(coords) - 1)
        self.osrm_ok += 1
        return coords, [max(s * 3.6, 5.0) for s in speeds]

    def _straight(self, a, b):
        dist = haversine_m(a, b)
        n = max(2, int(dist / 200))
        coords = [(a[0] + (b[0] - a[0]) * i / n, a[1] + (b[1] - a[1]) * i / n) for i in range(n + 1)]
        base = 60.0 if dist > 20000 else 35.0
        return coords, [base] * n


# ---------------------------------------------------------------- xe
class Vehicle:
    def __init__(self, idx, kind, city, rng, router, err):
        lat, lon, prov = CITIES[city]
        self.kind, self.city, self.rng, self.router, self.err = kind, city, rng, router, err
        self.code = f"{prov:02d}FK{idx:04d}"
        self.de = f"{prov:02d}FK-{idx:04d}"
        self.cb = f"TMS.GPS.SERVICE{rng.randint(1, 40)}-VTracking2Processor-FAKE{idx:04d}"
        self.home = offset_point(rng, (lat, lon), 1, 8)
        self.pos = self.home
        self.odo_km = rng.uniform(1000, 200000)
        self.heading = rng.randint(0, 359)
        self.records = []
        self.overspeed = []          # đáp án: các đoạn cố ý vượt tốc độ
        self.injected = {"gps_jump": 0, "clock_error": 0, "zero_coord": 0, "duplicate": 0}

    # ---------- ghi 1 điểm (có chèn lỗi ngẫu nhiên)
    def emit(self, t0, t1, pos, st, speed):
        rng, lat, lon = self.rng, pos[0], pos[1]
        if st == 1 and rng.random() < self.err["jump"]:
            lat += rng.choice((-1, 1)) * rng.uniform(0.03, 0.08)
            self.injected["gps_jump"] += 1
        elif rng.random() < self.err["zero"]:
            lat = lon = 0.0
            self.injected["zero_coord"] += 1
        if rng.random() < self.err["clock"]:
            t0 = datetime(2025, 12, 2, t0.hour, t0.minute, t0.second)
            self.injected["clock_error"] += 1
        rec = {"t0": t0, "t1": t1, "lt": lat, "ln": lon, "st": st,
               "ag": self.heading, "km": self.odo_km, "vl": int(round(speed))}
        self.records.append(rec)
        if rng.random() < self.err["dup"]:
            self.records.append(dict(rec, t1=t1 + timedelta(seconds=rng.uniform(30, 60))))
            self.injected["duplicate"] += 1

    def lag(self):
        if self.rng.random() < 0.03:
            return timedelta(seconds=self.rng.uniform(30, 120))
        return timedelta(seconds=self.rng.uniform(2, 20))

    # ---------- đỗ xe: ~1 điểm/phút, t0 cập nhật mỗi 2 phút
    def park(self, start, end):
        rt = start
        while rt < end:
            fix = start + timedelta(seconds=((rt - start).total_seconds() // 120) * 120)
            self.emit(fix, rt + timedelta(seconds=self.rng.uniform(2, 10)), self.pos, 0, 0)
            rt += timedelta(seconds=self.rng.uniform(55, 65))

    # ---------- chạy xe theo tuyến
    def drive(self, dest, t, end):
        rng = self.rng
        coords, speeds = self.router.route(self.pos, dest)
        if len(coords) < 2:
            return t
        n = len(speeds)
        speeds = [min(s * TRUCK_SPEED_FACTOR * rng.uniform(0.85, 1.05), TRUCK_MAX_KMH) for s in speeds]
        seg_d = [haversine_m(coords[i], coords[i + 1]) for i in range(n)]

        # cố ý vượt tốc độ 2–6 km trên một đoạn đường nhanh
        boosted = set()
        fast = [i for i in range(n) if speeds[i] >= 40]
        if fast and rng.random() < 0.35:
            i, target, acc = rng.choice(fast), rng.uniform(2000, 6000), 0.0
            factor = rng.uniform(1.25, 1.45)
            while i < n and acc < target:
                speeds[i] = min(speeds[i] * factor, 125)
                boosted.add(i)
                acc += seg_d[i]
                i += 1

        cum_t, cum_d = [0.0], [0.0]
        for i in range(n):
            cum_t.append(cum_t[-1] + seg_d[i] / (speeds[i] / 3.6))
            cum_d.append(cum_d[-1] + seg_d[i])

        odo0, s, rest_off, last_rest = self.odo_km, 0.0, 0.0, 0.0
        rest_every = rng.uniform(3.5, 4.5) * 3600
        ep_start = ep_end = None
        while s < cum_t[-1]:
            now = t + timedelta(seconds=s + rest_off)
            if now >= end:
                return end
            if self.kind == "inter" and s - last_rest > rest_every:      # nghỉ giữa đường
                rest = rng.uniform(30, 45) * 60
                self.park(now, min(now + timedelta(seconds=rest), end))
                rest_off += rest
                last_rest = s
                continue
            j = min(bisect.bisect_right(cum_t, s) - 1, n - 1)
            f = (s - cum_t[j]) / max(cum_t[j + 1] - cum_t[j], 1e-9)
            a, b = coords[j], coords[j + 1]
            self.pos = (a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f)
            self.heading = bearing_deg(a, b)
            self.odo_km = odo0 + (cum_d[j] + seg_d[j] * f) / 1000
            if j in boosted:
                ep_start = ep_start or now
                ep_end = now
            self.emit(now, now + self.lag(), self.pos, 1, max(speeds[j] + rng.uniform(-3, 3), 1))
            s += rng.uniform(10, 30)

        if ep_start:
            self.overspeed.append({"start": ep_start.isoformat(timespec="seconds"),
                                   "end": ep_end.isoformat(timespec="seconds")})
        self.pos = coords[-1]
        self.odo_km = odo0 + cum_d[-1] / 1000
        return t + timedelta(seconds=cum_t[-1] + rest_off)

    # ---------- kế hoạch một ngày
    def plan(self):
        rng = self.rng
        if self.kind == "intra":
            stops = [(offset_point(rng, self.home, 3, 35), rng.uniform(15, 60)) for _ in range(rng.randint(2, 5))]
            return stops + [(self.home, 0)]
        if haversine_m(self.pos, self.home) < 20000:
            others = [c for c in CITIES if c != self.city]
            target = CITIES[rng.choice(others)]
            dest = offset_point(rng, (target[0], target[1]), 2, 10)
            plan = [(dest, rng.uniform(60, 150))]
            if haversine_m(self.home, dest) < 300000 and rng.random() < 0.5:
                plan.append((self.home, 0))                         # chuyến ngắn: về trong ngày
            return plan
        return [(self.home, 0)]

    def next_departure(self, t):
        hour = self.rng.uniform(5.5, 8.0) if self.kind == "intra" else self.rng.uniform(4.0, 20.0)
        dep = datetime(t.year, t.month, t.day) + timedelta(hours=hour)
        return dep if dep > t else dep + timedelta(days=1)

    def simulate(self, start, end):
        t = start
        while t < end:
            dep = min(self.next_departure(t), end)
            self.park(t, dep)
            t = dep
            for dest, dwell in self.plan():
                if t >= end:
                    break
                t = self.drive(dest, t, end)
                if dwell and t < end:
                    stop_end = min(t + timedelta(minutes=dwell), end)
                    self.park(t, stop_end)
                    t = stop_end


# ---------------------------------------------------------------- ghi file dump
def fmt_point(r, v):
    return ('{"t0":"%s","t1":"%s","cb":"%s","lt":%.6f,"ln":%.6f,"st":%d,"ag":%d,"km":%.3f,"vl":%d,"de":"%s"}'
            % (r["t0"].strftime("%Y-%m-%dT%H:%M:%S"), r["t1"].strftime("%Y-%m-%dT%H:%M:%S"), v.cb,
               r["lt"], r["ln"], r["st"], r["ag"], r["km"], r["vl"], v.de))


def write_vehicle(f, v, rng):
    f.write(f'\nCREATE TABLE public."{v.code}" (\n    "VehicleCode" text,\n    "Date" bigint,\n'
            '    "Details" json[],\n    "Total" integer,\n    "CreatedTime" timestamp without time zone\n);\n\n')
    recs = sorted(v.records, key=lambda r: r["t1"])
    i = 0
    while i < len(recs):
        hour = recs[i]["t1"].replace(minute=0, second=0, microsecond=0)
        grp = []
        while i < len(recs) and recs[i]["t1"].replace(minute=0, second=0, microsecond=0) == hour:
            grp.append(recs[i])
            i += 1
        arr = "{" + ",".join('"' + fmt_point(r, v).replace('"', '\\"') + '"' for r in grp) + "}"
        created = (hour + timedelta(seconds=rng.uniform(50, 110))).strftime("%Y-%m-%d %H:%M:%S.%f")
        f.write(f"INSERT INTO public.\"{v.code}\" VALUES ('{v.code}', {hour:%Y%m%d%H}, "
                f"'{arr}', {len(grp)}, '{created}');\n")


def main():
    ap = argparse.ArgumentParser(description="Sinh dữ liệu GPS xe tải giả lập dạng dump PostgreSQL")
    ap.add_argument("--vehicles", type=int, default=10)
    ap.add_argument("--start-date", default="2026-10-11")
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--intra-ratio", type=float, default=0.6, help="tỉ lệ xe nội tỉnh")
    ap.add_argument("--osrm", default="http://localhost:5000")
    ap.add_argument("--no-osrm", action="store_true", help="không dùng OSRM, xe đi đường thẳng")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--output", required=True)
    ap.add_argument("--error-scale", type=float, default=1.0, help="nhân tỉ lệ lỗi chèn (0 = không chèn lỗi)")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    err = {k: p * args.error_scale for k, p in
           {"jump": 0.002, "zero": 0.0003, "clock": 0.0005, "dup": 0.003}.items()}
    router = Router(None if args.no_osrm else args.osrm, rng)
    start = datetime.fromisoformat(args.start_date)
    end = start + timedelta(days=args.days)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    truth = {"generator": "generate_fake_dump.py", "seed": args.seed, "start": start.isoformat(),
             "end": end.isoformat(), "status_codes": {"0": "parked", "1": "moving"}, "vehicles": []}

    with open(args.output, "w", encoding="utf-8") as f:
        f.write("--\n-- DỮ LIỆU GIẢ LẬP – sinh bởi scripts/generate_fake_dump.py, KHÔNG phải dữ liệu thật\n--\n")
        for idx in range(1, args.vehicles + 1):
            kind = "intra" if rng.random() < args.intra_ratio else "inter"
            v = Vehicle(idx, kind, rng.choice(list(CITIES)), rng, router, err)
            v.simulate(start, end)
            write_vehicle(f, v, rng)
            truth["vehicles"].append({"code": v.code, "kind": kind, "home_city": v.city,
                                      "points": len(v.records), "overspeed_episodes": v.overspeed,
                                      "injected_errors": v.injected})
            print(f"  {v.code} ({kind:5s}, {v.city:13s}): {len(v.records):6,} điểm, "
                  f"{len(v.overspeed)} đoạn vượt tốc độ")

    with open(args.output + ".truth.json", "w", encoding="utf-8") as f:
        json.dump(truth, f, ensure_ascii=False, indent=2)

    total = sum(x["points"] for x in truth["vehicles"])
    print(f"\nĐã ghi {total:,} điểm của {args.vehicles} xe vào {args.output} "
          f"({os.path.getsize(args.output) / 1e6:.1f} MB)")
    print(f"Đáp án: {args.output}.truth.json")
    if router.url:
        print(f"OSRM: {router.osrm_ok} tuyến thành công, {router.fallbacks} tuyến phải dùng đường thẳng")


if __name__ == "__main__":
    main()
