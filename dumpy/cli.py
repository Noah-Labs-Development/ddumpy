import logging
from datetime import datetime
from os import PathLike, environ
from pathlib import Path

import click
import docker

from dumpy.helpers.docker import docker_network
from dumpy.postgresql import (
    exec_pg_dump,
    exec_pg_restore,
    exec_pg_script,
    pg_container,
    wait_for_postgresql,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


LOCAL_PASSWD = environ.get("PASSWD", None)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("host", type=click.STRING)
@click.argument("db_name", type=click.STRING)
@click.argument("out_path", type=click.Path())
@click.argument("db_user", type=click.STRING, default="nl_admin")
def dump(host: str, db_name: str, out_path: PathLike, db_user: str):
    """Dumps the database to a file"""
    client = docker.from_env()
    out_path = Path(out_path).resolve()

    if out_path.is_dir():
        out_path = out_path / f"{db_name}-{int(datetime.utcnow().timestamp())}.sql"

    passwd = None
    if host == "localhost":
        passwd = LOCAL_PASSWD

    exec_pg_dump(client, out_path, host, db_name, db_user=db_user, passwd=passwd)


@cli.command()
@click.argument("host")
@click.argument("dump", type=click.Path(exists=True))
@click.argument("db_user", type=click.STRING, default="nl_admin")
def restore(host: str, dump: PathLike, db_user: str):
    """Restores the database from a dump"""
    client = docker.from_env()
    dump = Path(dump).resolve()
    passwd = None

    if host == "localhost":
        passwd = LOCAL_PASSWD

    exec_pg_restore(
        client, dump, host, db_user=db_user, clean=True, comunicate=True, passwd=passwd
    )


@cli.command()
@click.argument("host", type=click.STRING)
@click.argument("db_name", type=click.STRING)
@click.argument("script", type=click.Path(exists=True))
@click.argument("db_user", type=click.STRING, default="nl_admin")
def exec_live(host: str, db_name: str, script: PathLike, db_user: str):
    """Executes a script on a live database"""
    client = docker.from_env()
    script_path = Path(script).resolve()
    passwd = None

    if host == "localhost":
        passwd = LOCAL_PASSWD

    exec_pg_script(client, host, db_name, script_path, db_user=db_user, passwd=passwd)


@cli.command()
@click.argument("dump", type=click.Path(exists=True))
@click.argument("db_name", type=click.STRING)
@click.argument("script", type=click.Path(exists=True))
def exec_file(dump: PathLike, db_name: str, script: PathLike):
    """Executes a script on a database dump"""
    dump_path = Path(dump).resolve()
    script = Path(script).resolve()
    client = docker.from_env()
    out_filepath = dump_path.with_name(f"{dump_path.name}.clean")

    with docker_network(client, "pg_dump_job", check_duplicate=True) as network:
        with pg_container(
            client,
            name="dump_cleanup_postgres",
            environment={"POSTGRES_HOST_AUTH_METHOD": "trust"},
            network=network.name,
        ) as container:
            wait_for_postgresql(container)
            container_name: str = container.name  # type: ignore

            exec_pg_restore(
                client,
                dump_path,
                container_name,
                network=network.name,
                comunicate=False,
            )
            exec_pg_script(
                client, container_name, db_name, script, network=network.name
            )
            exec_pg_dump(
                client, out_filepath, container_name, db_name, network=network.name
            )
