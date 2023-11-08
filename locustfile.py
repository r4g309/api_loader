from random import choice, randint, shuffle

import polars as pl
from locust import HttpUser, between, events, task
from locust.runners import MasterRunner, WorkerRunner
from string import ascii_lowercase
students = []
teachers = []
total_workers = 0

def random_string(length:int):
    return "".join(choice(ascii_lowercase) for _ in range(length))

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
        )
        if token.status_code == 200:
            token = token.json()
            self.auth = {"Authorization": f"{token['token_type']} {token['access_token']}"}
            self.valid_token = True
        else:
            self.valid_token = False
            self.auth =""
            
    @task
    def get_students_by_limit(self):
        if self.valid_token:
            self.client.get(f"/student/?limit={randint(1,100)}", headers=self.auth)

    @task
    def get_student_by_code(self):
        if self.valid_token:
            global students
            code = choice(students)["code"]
            self.client.get(f"/student/code/{code}",headers=self.auth)

    @task
    def get_student_by_email(self):
        if self.valid_token:
            global students
            email = choice(students)["email"]
            self.client.get(f"/student/email/{email}",headers=self.auth)

    @task
    def update_student_name(self):
        global students
        if self.valid_token:
            user = choice(students)
            response = self.client.get(f"/student/email/{user['email']}",headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
                print(user_id)
                self.client.put(f"/student/update/{user_id}",headers=self.auth,json={
                    "name":random_string(10)
                })

    @task
    def update_student_name(self):
        global students
        if self.valid_token:
            user = choice(students)
            response = self.client.get(f"/student/email/{user['email']}",headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
                self.client.put(f"/student/update/{user_id}",headers=self.auth,json={
                    "last_name":random_string(8)
                })
    @task
    def update_student_name_and_last_name(self):
        global students
        if self.valid_token:
            user = choice(students)
            response = self.client.get(f"/student/email/{user['email']}",headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
                self.client.put(f"/student/update/{user_id}",headers=self.auth,json={
                    "name":random_string(10),
                    "last_name":random_string(8)
                })
    @task
    def update_student_activate(self):
        global students
        if self.valid_token:
            user = choice(students)
            response = self.client.get(f"/student/email/{user['email']}",headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
                self.client.put(f"/student/update/{user_id}",headers=self.auth,json={
                    "is_active":True
                })
    @task
    def deactivate_student(self):
        global students
        if self.valid_token:
            user = choice(students)
            response = self.client.get(f"/student/email/{user['email']}",headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
                self.client.put(f"/student/deactivate/{user_id}",headers=self.auth)

    @task   
    def get_teachers_by_limit(self):
        if self.valid_token:
            self.client.get(f"/teacher/?limit={randint(1,100)}",headers=self.auth)

    @task
    def get_teacher_by_email(self):
        global teachers
        if self.valid_token:
            user = choice(teachers)["email"]
            self.client.get(f"/teacher/email/{user}", headers=self.auth)

    @task
    def put_teacher_name(self):
        global teachers
        if self.valid_token:
            user = choice(teachers)["email"]
            response = self.client.get(f"/teacher/email/{user}", headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
                self.client.put(f"/teacher/update/{user_id}", data={"name":random_string(15)}, headers=self.auth)
    @task
    def put_teacher_last_name(self):
        global teachers
        if self.valid_token:
            user = choice(teachers)["email"]
            response = self.client.get(f"/teacher/email/{user}", headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
                self.client.put(f"/teacher/update/{user_id}", data={"last_name":random_string(15)}, headers=self.auth) 
    @task
    def put_teacher_work_hours(self):
        global teachers
        if self.valid_token:
            user = choice(teachers)["email"]
            response = self.client.get(f"/teacher/email/{user}", headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
                self.client.put(f"/teacher/update/{user_id}", data={"work_hours":choice([None,randint(1,16)])}, headers=self.auth)
    
    @task
    def put_teacher_name(self):
        global teachers
        if self.valid_token:
            user = choice(teachers)["email"]
            response = self.client.get(f"/teacher/email/{user}", headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
                self.client.put(f"/teacher/update/{user_id}", data={"name":random_string(15),"last_name":random_string(20), "work_hours":randint(1,100) }, headers=self.auth)
        
    @task
    def put_teacher_deactivate(self):
        global teachers
        if self.valid_token:
            user = choice(teachers)["email"]
            response = self.client.get(f"/teacher/email/{user}", headers=self.auth)
            if response.status_code == 200:
                data = response.json()
                user_id = data[0]["id"]
            self.client.put(f"/teacher/deactivate/{user_id}",headers=self.auth)

    @task
    def get_list_disciplines(self):
        if self.valid_token:
            self.client.get(f"/utils/list_discipline?limit={randint(1,20)}&skip={randint(1,3)}")

def setup_test_users(environment, msg, **kwargs):
    global students,teachers, total_workers
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
        teachers = pl.read_csv("./data/teachers_200.csv", separator=",").to_dicts()
        total_workers = environment.runner.worker_count
        environment.runner.send_message("test_users", (students,teachers, total_workers))

