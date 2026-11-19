"""Tạo index ES + data view Kibana cho streaming."""
import argparse
import json
import os
import urllib.error
import urllib.request

DATE = {"type": "date", "format": "epoch_millis||strict_date_optional_time"}
KW = {"type": "keyword"}

INDICES = {
    "gps-positions": {
        "vehicle_key": KW, "gps_time": DATE, "recv_time": DATE, "speed": {"type": "float"},
        "heading": {"type": "integer"}, "provider": KW, "location": {"type": "geo_point"},
        "moving": {"type": "boolean"},
    },
    "gps-events": {
        "event_id": KW, "vehicle_key": KW, "event_type": KW, "phase": KW, "severity": KW,
        "start_time": DATE, "end_time": DATE, "duration_s": {"type": "float"},
        "max_speed": {"type": "float"}, "speed_limit": {"type": "float"}, "n_points": {"type": "integer"},
        "location": {"type": "geo_point"}, "hour_of_day": {"type": "integer"},
    },
    "gps-traffic": {
        "cell_id": KW, "window_start": DATE, "window_end": DATE, "location": {"type": "geo_point"},
        "n_points": {"type": "integer"}, "n_vehicles": {"type": "integer"},
        "avg_speed": {"type": "float"}, "n_moving_points": {"type": "integer"},
    },
}
TIME_FIELD = {"gps-positions": "gps_time", "gps-events": "start_time", "gps-traffic": "window_start"}


def call(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--es-url", default=os.environ.get("ES_URL", "http://elasticsearch:9200"))
    ap.add_argument("--kibana-url", default=os.environ.get("KIBANA_URL", "http://kibana:5601"))
    ap.add_argument("--recreate", action="store_true", help="XOÁ index cũ rồi tạo lại (mất dữ liệu ES)")
    a = ap.parse_args()

    for idx, props in INDICES.items():
        if a.recreate:
            call("DELETE", f"{a.es_url}/{idx}")
        st, body = call("PUT", f"{a.es_url}/{idx}", {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "5s"},
            "mappings": {"properties": props},
        })
        if st < 300:
            print(f"[ES] tạo index {idx}")
        elif "resource_already_exists_exception" in body:
            print(f"[ES] index {idx} đã có")
        else:
            raise SystemExit(f"[ES] lỗi tạo {idx}: {st} {body[:300]}")

    for idx, tf in TIME_FIELD.items():
        st, body = call("POST", f"{a.kibana_url}/api/data_views/data_view", {
            "data_view": {"id": idx, "title": idx, "name": idx, "timeFieldName": tf},
            "override": True,
        }, headers={"kbn-xsrf": "true"})
        print(f"[Kibana] data view {idx}: {'ok' if st < 300 else f'lỗi {st} {body[:200]}'}")


if __name__ == "__main__":
    main()
