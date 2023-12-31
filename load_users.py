import asyncio
import os
from argparse import ArgumentParser
from pathlib import Path
from typing import Literal

import httpx
import polars as pl
import psycopg2
from dotenv import load_dotenv
from rich.console import Console

temp_file_name = "temp_file.csv"


async def activate_user_requests(
    endpoint_url, session: httpx.AsyncClient, confirmation, password
):
    response = await session.post(
        f"{endpoint_url}/auth/confirm/{confirmation}",
        headers={"accept": "application/json", "Content-Type": "application/json"},
        json={"password": password},
        timeout=120.0,
    )
    return response


async def activate_users(
    endpoint_url,
    rows,
    df,
):
    df_new = pl.DataFrame(rows, schema=["email", "confirmation"])
    df_new = df.join(df_new, on="email")
    client = httpx.AsyncClient(verify=False, timeout=120.0)
    await asyncio.gather(
        *[
            activate_user_requests(
                endpoint_url, client, row["confirmation"], row["password"]
            )
            for row in df_new.to_dicts()
        ]
    )
    await client.aclose()



def get_users_to_activate(cursor, emails, model: Literal["students", "teachers"]):
    cursor.execute(
        f"SELECT email,confirmation from {model} where email = ANY(%s) AND confirmation IS NOT NULL",
        (
            emails,
        ),
    )
    return cursor.fetchall()


async def main(csv_file: str, endpoint_url: str, model: str):
    console = Console()
    with console.status("Cargando usuarios") as status:
        console.log("Validando que el endpoint este activo")
        try:
            response = httpx.get(f"{endpoint_url}", verify=False, timeout=30.0)
        except httpx.ConnectError:
            console.log(f"Error conectando al endpoint {endpoint_url}")
            return
        console.log("Cargando archivo")
        df = pl.read_csv(csv_file, separator=",")
        console.log("Cargando variables de entorno y validando")
        load_dotenv("./.env")
        pg_conn = (
            os.getenv("DATABASE_NAME", None),
            os.getenv("USER_DB"),
            os.getenv("PASSWORD"),
            os.getenv("PORT"),
            os.getenv("HOST"),
        )
        assert all(value is not None for value in pg_conn), "Missing env variables"

        dns = (
            f"postgresql://{os.getenv('USER_DB')}"
            f":{os.getenv('PASSWORD')}"
            f"@{os.getenv('HOST')}"
            f":{os.getenv('PORT')}"
            f"/{os.getenv('DATABASE_NAME')}"
        )
        console.log("Conectando a la base de datos")
        conn = psycopg2.connect(dns)

        console.log("Validando duplicados")
        if model == "students":
            codes = df.select(pl.col("code")).to_series().to_list()
            assert len(codes) == len(set(codes)), "Duplicated codes"

        emails = df.select(pl.col("email")).to_series().to_list()
        assert len(emails) == len(set(emails)), "Duplicated emails"
        console.log("Cargando usuarios")
        df.drop("password").write_csv(temp_file_name)
        respose = httpx.post(
            f"{endpoint_url}/auth/",
            verify=False,
            timeout=30.0,
            data={"username": "admin@example.com", "password": "12345678"},
        ).json()
        user_token = f"{respose['token_type']} {respose['access_token']}"
        if model == "teachers":
            disciplines = [
                "Infraestructura y gestión de la información",
                "Ingeniería de software",
                "Ciencias de la computación",
                "Ciencias naturales",
                "Comunicación",
                "Matemáticas y estadística",
            ]
            disciplines = [{"name": discipline} for discipline in disciplines]
            response = httpx.post(
                url=f"{endpoint_url}/utils/add_discipline",
                headers={"accept": "application/json", "Authorization": user_token},
                json=disciplines,
                verify=False,
                timeout=30.0,
            )
            assert (
                response.status_code == 200
            ), f"Error cargando las disciplinas {response.json()}"

        temp_file = open(temp_file_name, "rb")
        response = httpx.post(
            url=f"{endpoint_url}/load/{model}",
            headers={"accept": "application/json", "Authorization": user_token},
            files={"file": (temp_file_name, temp_file, "text/csv")},
            verify=False,
            timeout=30.0,
        )
        temp_file.close()
        try:
            assert (
                response.status_code == 200
            ), f"Error cargando el archivo {response.json()}"
        except Exception:
            console.log(f"Error cargando el archivo {response.text}")
            return

        console.log("Activando usuarios")
        cursor = conn.cursor()
        rows = get_users_to_activate(cursor, emails, model)
        if len(rows) == 0:
            status.update("No hay usuarios para activar")
            os.remove(temp_file_name)
            return
        await activate_users(endpoint_url, rows, df)

    os.remove(temp_file_name)
    cursor.close()
    conn.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("file", type=Path, help="Archivo con los usuarios")
    parser.add_argument("model", choices=["teachers", "students"])
    parser.add_argument(
        "--endpoint",
        type=str,
        help="Endpoint de la API",
        default="http://localhost:8000",
    )
    args = parser.parse_args()
    asyncio.run(main(args.file.absolute(), args.endpoint, args.model))
