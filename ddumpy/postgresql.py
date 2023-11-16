import itertools
import logging
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, NamedTuple

import docker
from docker.errors import ContainerError
from docker.models.containers import Container

from .helpers.docker import comunicate_with_container, docker_container

log = logging.getLogger()


class PgRestoreError(NamedTuple):
    error: str
    command: str


class PgRestoreParser:
    REGEX = re.compile(r"pg_restore: error: (.*)\nCommand was: (.*)\n")
    TRIVIAL_ERR_REGEX = re.compile(
        r"(role \".*\" does not exist)|(schema \"public\" already exists)"
    )

    @classmethod
    def parse_stderr(cls, text: str) -> tuple[list[PgRestoreError], str]:
        restore_errors: list[PgRestoreError] = []
        matched_spans: list[tuple[int, int]] = []
        for match in cls.REGEX.finditer(text):
            restore_errors.append(PgRestoreError(*match.groups()))
            matched_spans.append(match.span())

        if len(matched_spans) == 0:
            return ([], text)

        # dumbass algorithm, but oh well
        not_errors = ""
        not_errors += text[: matched_spans[0][0]]
        for span, next_span in itertools.pairwise(matched_spans):
            not_errors += text[span[1] : next_span[0]]
        not_errors += text[matched_spans[-1][1] :]

        return (restore_errors, not_errors)

    @classmethod
    def filter_trivial_errors(
        cls, errors: Iterable[PgRestoreError]
    ) -> Iterable[PgRestoreError]:
        return filter(lambda x: cls.TRIVIAL_ERR_REGEX.search(x.error) is None, errors)


def wait_for_postgresql(c: Container):
    all_logs = ""
    for log_entry in c.logs(stream=True):
        log_chunk: str = log_entry.decode()
        all_logs += log_chunk
        if "database system is ready to accept connections" in all_logs:
            log.info("Postgres container ready")
            break


@contextmanager
def pg_container(
    client: docker.DockerClient,
    *,
    extra_hosts={"host.docker.internal": "host-gateway"},
    stdin_open=True,
    tty=True,
    detach=True,
    **kwargs,
):
    with docker_container(
        client,
        "postgres",
        extra_hosts=extra_hosts,
        stdin_open=stdin_open,
        tty=tty,
        detach=detach,
        **kwargs,
    ) as container:
        yield container


def exec_pg_script(
    client: docker.DockerClient,
    host: str,
    db_name: str,
    script_path: Path,
    db_user="postgres",
    passwd: str | None = None,
    comunicate=True,
    network: str | None = None,
):
    container_env = {"PGPASSWORD": passwd} if passwd else None
    if host == "localhost":
        host = "host.docker.internal"

    mounted_script_path = Path("/mnt") / script_path.name
    volume_bind = f"{str(script_path.parent)}:{str(mounted_script_path.parent)}:ro"

    with pg_container(
        client,
        command=f"psql --host {host} --username {db_user} -d {db_name} -f {str(mounted_script_path)}",
        name="postgres_psql",
        volumes=[volume_bind],
        environment=container_env,
        network=network,
    ) as container:
        if comunicate:
            comunicate_with_container(container)


def exec_pg_dump(
    client: docker.DockerClient,
    dump_path: Path,
    host: str,
    db_name: str,
    db_user: str = "postgres",
    network: str | None = None,
    passwd: str | None = None,
):
    container_env = {"PGPASSWORD": passwd} if passwd else None
    if host == "localhost":
        host = "host.docker.internal"

    mounted_dump_path = Path("/mnt") / dump_path.name
    volume_bind = f"{str(dump_path.parent)}:{str(mounted_dump_path.parent)}:z"
    print(volume_bind)

    with pg_container(
        client,
        command=f"pg_dump --host {host} --port 5432 --username {db_user} -F c -Cc --file {str(mounted_dump_path)} {db_name}",
        name="postgres_pg_dump",
        volumes=[volume_bind],
        network=network,
        environment=container_env,
    ) as container:
        comunicate_with_container(container)
        log.info(f"Dumped {db_name} DB to {dump_path}")


def hanlde_pg_restore_errors(exc: ContainerError):
    restore_errors, rest = PgRestoreParser.parse_stderr(exc.stderr.decode())
    log.warning(rest)
    non_trivial_errors = list(PgRestoreParser.filter_trivial_errors(restore_errors))
    if len(non_trivial_errors) != 0:
        for err in non_trivial_errors:
            log.warning(err)
        raise Exception(
            f"Found {len(non_trivial_errors)} Non-trivial errors. Command was: {exc.command}"
        )


def exec_pg_restore(
    client: docker.DockerClient,
    dump_path: Path,
    host: str,
    db_user="postgres",
    passwd: str | None = None,
    network: str | None = None,
    comunicate=True,
    clean=False,
):
    container_env = {"PGPASSWORD": passwd} if passwd else None
    if host == "localhost":
        host = "host.docker.internal"

    mounted_dump_path = Path("/mnt") / dump_path.name
    volume_bind = f"{str(dump_path.parent)}:{str(mounted_dump_path.parent)}:ro"

    try:
        with pg_container(
            client,
            command=f"pg_restore --host {host} --port 5432 --username {db_user} -C {'-c' if clean else ''}  -d postgres {str(mounted_dump_path)}",
            name="postgres_pg_restore",
            volumes=[volume_bind],
            network=network,
            environment=container_env,
        ) as container:
            if comunicate:
                comunicate_with_container(container)
            log.info(f"Restored {host} with {dump_path}")
    except ContainerError as exc:
        hanlde_pg_restore_errors(exc)
