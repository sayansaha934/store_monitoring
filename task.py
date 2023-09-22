from celery import Celery, Task
from env import REDIS_URL
from db import SESSION
from models import *
from schema import *
from datetime import datetime, timedelta, time
from pytz import timezone, utc
import pandas as pd

app = Celery(
    "worker",
    backend=REDIS_URL,
    broker=REDIS_URL,
)


def batch(list_data, bs):
    return [list_data[i : i + bs] for i in range(0, len(list_data), bs)]


class ReportGenerateTask(Task):
    name = "report_generate_task"
    track_started = True

    def __init__(self):
        pass

    def run(self, report_id):
        try:
            report = SESSION.query(Report).where(Report.id == report_id).first()
            all_stores = SESSION.query(StoreTimezone).all()
            store_timezone = {i.store_id: i.timezone_str for i in all_stores}

            all_batch_store_ids = batch([i.store_id for i in all_stores], 500)
            current_time = datetime(2023, 1, 25, 18, 13, 22, 479220)
            final_data = []
            for batch_store_ids in all_batch_store_ids:
                business_hours = self.get_business_hours(store_ids=batch_store_ids)
                store_status = self.get_store_status(store_ids=batch_store_ids)
                for _store_id in batch_store_ids:
                    _data = {}
                    uptime = self.get_uptime_or_downtime(
                        current_time=current_time,
                        business_hours=business_hours.get(_store_id, []),
                        store_status=store_status.get(_store_id, []),
                        tzone=store_timezone.get(_store_id, "America/Chicago"),
                        status_type="uptime",
                    )
                    downtime = self.get_uptime_or_downtime(
                        current_time=current_time,
                        business_hours=business_hours.get(_store_id, []),
                        store_status=store_status.get(_store_id, []),
                        tzone=store_timezone.get(_store_id, "America/Chicago"),
                        status_type="downtime",
                    )
                    _data["store_id"] = _store_id
                    _data["uptime_last_hour(in minutes)"] = uptime.get("last_hour") * 60
                    _data["uptime_last_day(in hours)"] = uptime.get("last_day")
                    _data["uptime_last_week(in hours)"] = uptime.get("last_week")
                    _data["downtime_last_hour(in minutes)"] = (
                        downtime.get("last_hour") * 60
                    )
                    _data["downtime_last_day(in hours)"] = downtime.get("last_day")
                    _data["downtime_last_week(in hours)"] = downtime.get("last_week")
                    final_data.append(_data)
            df = pd.DataFrame(final_data)
            file_path = f"Static/Report_{report_id}.csv"
            df.to_csv(file_path, header=True, index=None)

            report.status = "Complete"
            report.file = file_path

            SESSION.add(report)
            SESSION.commit()
        except Exception as e:
            print(e)
            report.status = "Failed"
            SESSION.add(report)
            SESSION.commit()

        SESSION.close()
        return True

    def get_store_status(self, store_ids):
        store_status = (
            SESSION.query(StoreStatus).filter(StoreStatus.store_id.in_(store_ids)).all()
        )
        data = StoreStatusSchema(many=True).dump(store_status)
        grouped_data = {}
        for i in data:
            if i["store_id"] not in grouped_data:
                grouped_data[i["store_id"]] = []
            grouped_data[i["store_id"]].append(i)

        return grouped_data

    def get_business_hours(self, store_ids):
        business_hours = (
            SESSION.query(BusinessHours)
            .filter(BusinessHours.store_id.in_(store_ids))
            .all()
        )
        data = BusinessHoursSchema(many=True).dump(business_hours)
        grouped_data = {}
        for i in data:
            if i["store_id"] not in grouped_data:
                grouped_data[i["store_id"]] = []
            grouped_data[i["store_id"]].append(i)

        return grouped_data

    def get_uptime_or_downtime(
        self, current_time, business_hours, store_status, tzone, status_type
    ):
        status_data = {}
        for duration in ["last_hour", "last_day", "last_week"]:
            hours = 0
            if duration == "last_hour":
                delta = timedelta(hours=1)
            elif duration == "last_day":
                delta = timedelta(days=1)
            elif duration == "last_week":
                delta = timedelta(weeks=1)
            r1 = current_time - delta
            r2 = current_time
            timestamp_status_map = {
                self.get_timestamp_object(i.get("timestamp_utc")): i.get("status")
                for i in store_status
            }

            for _ts, _status in timestamp_status_map.items():
                if (r1 <= _ts <= r2) and self.is_in_business_hours(
                    _timestamp=_ts, business_hours=business_hours, tzone=tzone
                ):
                    if status_type == "uptime" and _status == "active":
                        hours += 1
                    elif status_type == "uptime" and _status == "inactive":
                        hours += 1
            status_data[duration] = hours
        return status_data

    def get_timestamp_object(self, ts):
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")

    def is_in_business_hours(self, _timestamp, business_hours, tzone):
        tz = timezone(tzone)
        day_business_hour = [
            i for i in business_hours if i["day"] == _timestamp.weekday()
        ]

        if day_business_hour:
            for row in business_hours:
                day, start_time_local, end_time_local = (
                    row.get("day"),
                    row.get("start_time_local"),
                    row.get("end_time_local"),
                )
                start_time_utc = (
                    tz.localize(datetime.strptime(start_time_local, "%H:%M:%S"))
                    .astimezone(utc)
                    .time()
                )
                end_time_utc = (
                    tz.localize(datetime.strptime(end_time_local, "%H:%M:%S"))
                    .astimezone(utc)
                    .time()
                )

                if start_time_utc <= _timestamp.time() <= end_time_utc:
                    return True
        else:
            start_time_utc = time(0, 0, 0)
            end_time_utc = time(23, 59, 59)
            if start_time_utc <= _timestamp.time() <= end_time_utc:
                return True
        return False


app.register_task(ReportGenerateTask)
