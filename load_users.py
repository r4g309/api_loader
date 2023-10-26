import asyncio
import os

import httpx
import polars as pl
import psycopg2
from dotenv import load_dotenv
from rich import progress
from rich.progress import TaskID

temp_file_name = "temp_file.csv"
csv_file = "./user_600.csv"
ENDPOINT_URL = "https://sig_catolica_api-1-w5327817.deta.app/"


async def activate_user_requests(
    session: httpx.AsyncClient, pb: progress.Progress, task_id: TaskID, confirmation, password
):
    response = await session.post(
        f"{ENDPOINT_URL}auth/confirm/{confirmation}",
        headers={"accept": "application/json", "Content-Type": "application/json"},
        json={"password": password},
    )

    pb.update(task_id=task_id, advance=1)
    return response


async def activate_users(rows, df, pb):
    df_new = pl.DataFrame(rows, schema=["email", "confirmation"])
    df_new = df.join(df_new, on="email")
    client = httpx.AsyncClient(verify=False, timeout=30.0)
    _t = pb.add_task("Activando usuarios", total=len(df_new))
    result = await asyncio.gather(
        *[activate_user_requests(client, pb, _t, row["confirmation"], row["password"]) for row in df_new.to_dicts()]
    )
    await client.aclose()
    return result


def get_users_to_activate(cursor, emails):
    cursor.execute(
        "SELECT email,confirmation from students where email = ANY(%s) AND confirmation IS NOT NULL", (emails,)
    )
    return cursor.fetchall()


async def main():
    with progress.Progress() as pb:
        _t = pb.add_task("Cargando archivo", total=1)
        df = pl.read_csv(csv_file, separator=";")
        pb.update(task_id=_t, completed=1)
        _t = pb.add_task("Cargando variables de entorno y validando", total=1)
        load_dotenv("./.env")
        pg_conn = {
            "dbname": os.getenv("DATABASE_NAME"),
            "user": os.getenv("USER_DB"),
            "password": os.getenv("PASSWORD"),
            "port": os.getenv("PORT"),
            "host": os.getenv("HOST"),
        }
        assert all(value for _, value in pg_conn.items()), "Missing env variables"
        pb.update(task_id=_t, completed=1)

        dns = (
            f"postgres://{pg_conn['user']}"
            f":{pg_conn['password']}"
            f"@{pg_conn['host']}"
            f":{pg_conn['port']}"
            f"/{pg_conn['dbname']}?sslmode=require"
        )
        _t = pb.add_task("Conectando a la base de datos", total=1)
        conn = psycopg2.connect(dns)
        pb.update(task_id=_t, completed=1)

        _t = pb.add_task("Validando duplicados", total=1)
        codes = df.select(pl.col("code")).to_series().to_list()
        emails = df.select(pl.col("email")).to_series().to_list()

        assert len(emails) == len(set(emails)), "Duplicated emails"
        assert len(codes) == len(set(codes)), "Duplicated codes"
        pb.update(task_id=_t, completed=1)

        _t = pb.add_task("Cargando usuarios", total=1)
        df.drop("password").write_csv(temp_file_name)
        temp_file = open(temp_file_name, "rb")
        response = httpx.post(
            url=f"{ENDPOINT_URL}load/students",
            headers={"accept": "application/json"},
            files={"file": (temp_file_name, temp_file, "text/csv")},
            verify=False,
            timeout=30.0,
        )

        assert response.status_code == 200, f"Error cargando el archivo {response.json()}"
        pb.update(task_id=_t, completed=1)
        temp_file.close()

        cursor = conn.cursor()
        rows = get_users_to_activate(cursor, emails)
        if len(rows) == 0:
            pb.update(task_id=_t, description="No hay usuarios para activar", completed=1)
            return
        results = await activate_users(rows, df, pb)
        if not (all(result.status_code == 200 for result in results)):
            rows = get_users_to_activate(cursor, emails)
            await activate_users(rows, df, pb)

    os.remove(temp_file_name)
    cursor.close()
    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
