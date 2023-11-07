from pathlib import Path
from random import choice, randint, shuffle

import polars as pl
from locust import HttpUser, between, events, task
from locust.runners import MasterRunner, WorkerRunner

my_users = []
total_workers = 0


class UserLogin(HttpUser):
    host = "http://localhost:8000"
    wait_time = between(1, 2)

    def on_start(self):
        global my_users, total_workers
        headers = {"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
        shuffle(my_users)
        shufle_data = my_users[0 : len(my_users) // total_workers]
        user = choice(shufle_data)
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
    global my_users, total_workers
    my_users, total_workers = msg.data
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
        filename = Path(environment.parsed_options.file).absolute()
        all_users = pl.read_csv(filename, separator=";").to_dicts()
        total_workers = environment.runner.worker_count
        environment.runner.send_message("test_users", (all_users, total_workers))


@events.init_command_line_parser.add_listener
def init_parser(parser):
    parser.add_argument("--file", type=str, help="It's working", default="", include_in_web_ui=False)
