from random import choice, randint, shuffle

import polars as pl
from locust import HttpUser, between, events, task
from locust.runners import MasterRunner, WorkerRunner

students = []
teachers = []
total_workers = 0


class UserLogin(HttpUser):
    host = "http://localhost:8000"
    wait_time = between(1, 2)

    def on_start(self):
        global students,teachers, total_workers
        headers = {"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
        shuffle(students)
        shuffle(teachers)
        if randint(0, 1) == 0:
            user = choice(students)
        else:
            user = choice(teachers)
        token = self.client.post(
            "/auth/",
            data={
                "username": user["email"],
                "password": user["password"],
            },
            headers=headers,
        ).json()
        self.auth = {"Authorization": f"{token['token_type']} {token['access_token']}"}

    @task
    def get_students_by_limit(self):
        self.client.get(f"/student/?limit={randint(1,100)}", headers=self.auth)



def setup_test_users(environment, msg, **kwargs):
    global students,teachesr, total_workers
    students,teachers, total_workers = msg.data
    environment.runner.send_message("acknowledge_users", "Data received")


def on_acknowledge(msg, **kwargs):
    print(msg.data)


@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    if not isinstance(environment.runner, MasterRunner):
        environment.runner.register_message("test_users", setup_test_users)
    if not isinstance(environment.runner, WorkerRunner):
        environment.runner.register_message("acknowledge_users", on_acknowledge)


@events.test_start.add_listener
def on_test_start(environment, **_kwargs):
    if not isinstance(environment.runner, WorkerRunner):
        students= pl.read_csv("./data/student_200.csv", separator=",").to_dicts()
        teachers = pl.read_csv("./data/teachers_200.csv", sep=",").to_dicts()
        total_workers = environment.runner.worker_count
        environment.runner.send_message("test_users", (students,teachers, total_workers))

