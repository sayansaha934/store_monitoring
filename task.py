from celery import Celery, Task
from env import REDIS_URL
from db import SESSION
from models import *
from schema import *
from datetime import datetime, timedelta
from pytz import timezone, utc
import pandas as pd

app = Celery(
    "worker",
    backend=REDIS_URL,
    broker=REDIS_URL,
)


class ReportGenerateTask(Task):
    name = "report_generate_task"
    track_started = True

    def __init__(self):
        pass

    def run(self, report_id):
        report = SESSION.query(Report).where(Report.id == report_id).first()
        all_stores = SESSION.query(StoreTimezone).all()
        current_time = datetime(2023, 1, 25, 18, 13, 22, 479220)
        final_data = []
        for store in all_stores:
            _data = {}
            business_hours = self.get_business_hours(store_ids=[store.store_id])
            store_status = self.get_store_status(store_ids=[store.store_id])

            uptime = self.get_uptime_or_downtime(
                current_time,
                business_hours,
                store_status,
                tzone=store.timezone_str,
                status_type="uptime",
            )
            downtime = self.get_uptime_or_downtime(
                current_time,
                business_hours,
                store_status,
                tzone=store.timezone_str,
                status_type="downtime",
            )
            _data["store_id"] = store.store_id
            _data["uptime_last_hour(in minutes)"] = uptime.get("last_hour")
            _data["uptime_last_day(in hours)"] = uptime.get("last_day")
            _data["uptime_last_week(in hours)"] = uptime.get("last_week")
            _data["downtime_last_hour(in minutes)"] = downtime.get("last_hour")
            _data["downtime_last_day(in hours)"] = downtime.get("last_day")
            _data["downtime_last_week(in hours)"] = downtime.get("last_week")
            final_data.append(_data)
            # break

        df = pd.DataFrame(final_data)
        df.to_csv("test.csv", header=True, index=None)

        report.status = "Completed"

        SESSION.add(report)
        SESSION.commit()
        SESSION.close()

    def get_store_status(self, store_ids):
        store_status = (
            SESSION.query(StoreStatus).filter(StoreStatus.store_id.in_(store_ids)).all()
        )
        return StoreStatusSchema(many=True).dump(store_status)

    def get_business_hours(self, store_ids):
        business_hours = (
            SESSION.query(BusinessHours)
            .filter(BusinessHours.store_id.in_(store_ids))
            .all()
        )
        return BusinessHoursSchema(many=True).dump(business_hours)

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
        return datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%f')

    def is_in_business_hours(self, _timestamp, business_hours, tzone):
        tz = timezone(tzone)
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

            if (
                day == _timestamp.weekday()
                and start_time_utc <= _timestamp.time() <= end_time_utc
            ):
                return True
        return False


app.register_task(ReportGenerateTask)
