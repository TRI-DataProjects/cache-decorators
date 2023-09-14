class ConnectionArgs:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs


class DuckCacher(CacherBase):
    def __init__(
        self,
        connection: DuckDBPyConnection,
        con_args: ConnectionArgs,
        pre_process: Callable | None = None,
        post_process: Callable[[Table], Table] | None = None,
    ) -> None:
        self._pre_process = pre_process
        self._post_process = post_process
        self.con_args = con_args.args
        self.con_kwargs = con_args.kwargs
        self.connection = connection.cursor()
        self.connection.sql('CREATE SCHEMA IF NOT EXISTS "cache_statistics";')
        self.connection.sql(
            'CREATE TABLE IF NOT EXISTS "cache_statistics"."stats"('
            " r_id VARCHAR NOT NULL,"
            " created TIMESTAMP NOT NULL DEFAULT get_current_timestamp(),"
            " modified TIMESTAMP NOT NULL DEFAULT get_current_timestamp(),"
            " accessed TIMESTAMP NOT NULL DEFAULT get_current_timestamp(),"
            " hits INT NOT NULL DEFAULT 0,"
            " PRIMARY KEY (r_id)"
            ");",
        )

    @contextmanager
    def _transaction_context(
        self,
        r_id: _ResourceID,
    ) -> Generator[DuckDBPyConnection, Any, None]:
        con = self.connection.cursor()
        con.begin()
        try:
            yield con
        except:
            con.rollback()
            raise
        else:
            con.commit()
        finally:
            con.close()

    def _resource_context(
        self,
        r_id: _ResourceID,
        timeout: float = -1,
    ) -> AbstractContextManager:
        return self._transaction_context(r_id)

    def _write_cache(self, r_id: _ResourceID, ctx: DuckDBPyConnection, data) -> None:
        ctx.sql(
            "CREATE OR REPLACE TABLE "  # noqa: S608
            f'"{r_id}"'
            " AS SELECT * FROM data;",
        )
        ctx.execute(
            'INSERT INTO "cache_statistics"."stats"'
            "(r_id) VALUES (?)"
            " ON CONFLICT (r_id)"
            " DO UPDATE SET modified = get_current_timestamp()",
            [str(r_id)],
        )

    def _read_cache(self, r_id: _ResourceID, ctx: DuckDBPyConnection) -> Table:
        ctx.execute(
            'UPDATE "cache_statistics"."stats"'
            " SET accessed = get_current_timestamp(),"
            " hits = hits + 1"
            " WHERE r_id = ?",
            [str(r_id)],
        )
        ctx.commit()
        return ibis.duckdb.connect(*self.con_args, **self.con_kwargs).table(str(r_id))

    def _resource_group(self, func_hash: str) -> Iterable[_ResourceID]:
        ids = (
            self.connection.execute(
                "SELECT table_name"
                " FROM information_schema.tables"
                " WHERE table_name LIKE ?;",
                [f"{func_hash}%"],
            )
            .arrow()["table_name"]
            .to_pylist()
        )
        for arg_hash in ids:
            yield _ResourceID(func_hash, arg_hash.split("_", 1)[1])

    def _uncache_resource(self, r_id: _ResourceID, ctx: DuckDBPyConnection) -> None:
        ctx.sql(
            f'DROP TABLE IF EXISTS "{r_id}"',
        )

    def _resource_stats(
        self,
        r_id: _ResourceID,
        ctx: DuckDBPyConnection,
    ) -> ResourceStats:
        ctx.execute(
            "SELECT created, modified, accessed, hits"
            ' FROM "cache_statistics"."stats"'
            " WHERE r_id = ?",
            [str(r_id)],
        ).fetchone()
        created, modified, accessed, hits = stats
        return ResourceStats(created, modified, accessed, hits)

    def resource_exists(self, r_id: _ResourceID) -> bool:
        return r_id in self._resource_group(r_id.func_hash)
