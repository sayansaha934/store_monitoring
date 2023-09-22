# store_monitoring
To find out the uptime for a particular time period, Collect all time stamps from store status within that time period and exclude those which are outside of business hours.
As polling is done every hour, for every timestamp in that range which status is active add 1 hour, finally we will get uptime for that time period. Apply same to calculate downtime.
# Requirements
- Python
- Flask
- PostgreSQL
- Celery
- Redis
# Architecture
- We will call ```/trigger_report``` endpoint which will start report generating in background using celery and return the report id
- ```/get_report``` endpoint check the status of the report generation in database, When report generation will be done then it will give the file in response
# How to use
- ``` pip install -r requirements.txt ```
- ```alembic revision --autogenerate``` (For db migration)
- ```alembic upgrade head``` (For db migration)
- ```docker-compose up -d``` (To setup local db and redis)
- ```uvicorn main:app --reload``` (To start python backend)
- ```celery -A task.app worker --loglevel=info``` (To start celery)
