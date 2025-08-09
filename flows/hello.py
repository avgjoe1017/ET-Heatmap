from prefect import flow, task
import datetime as dt

@task
def say(msg: str) -> str:
    now_utc = dt.datetime.now(dt.timezone.utc)
    print(f"[{now_utc.isoformat()}] {msg}")
    return msg

@flow(name="hello-flow")
def hello_flow():
    say.submit("Prefect is running.")
    return "ok"

if __name__ == "__main__":
    hello_flow()
