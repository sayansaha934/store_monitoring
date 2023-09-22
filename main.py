from fastapi import FastAPI
from models import Report
from db import SESSION
from task import ReportGenerateTask
from fastapi.responses import JSONResponse, FileResponse
from pathlib import Path

app = FastAPI()


@app.post("/trigger_report")
def trigger_action():
    try:
        report = Report(status="Running")
        SESSION.add(report)
        SESSION.commit()
        _task = ReportGenerateTask()
        _task.delay(report_id=report.id)
        SESSION.close()
        return JSONResponse(
            status_code=200,
            content={
                "message": "Report Generation Started",
                "report_id": str(report.id),
            },
        )
    except Exception as e:
        SESSION.close()
        print(e)
        return JSONResponse(
            status_code=500, content={"message": "Something went wrong"}
        )


@app.get("/get_report")
def get_report(report_id: str):
    try:
        report = SESSION.query(Report).filter(Report.id == report_id).first()
        SESSION.close()
        if report.status == "Complete":
            return FileResponse(
                report.file,
                media_type="application/octet-stream",
                filename=Path(report.file).name,
            )

        return JSONResponse(status_code=200, content={"status": report.status})
    except Exception as e:
        SESSION.close()
        print(e)
        return JSONResponse(
            status_code=500, content={"message": "Something went wrong"}
        )
