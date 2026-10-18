from clair import BuildPlan, RunMode, Table, TrouveAddress


class ReplaceWindowTrouve(Table):
    """An incremental TABLE that replaces each row of a recent window.

    A full refresh builds the table in the same way as a Trouve. An incremental
    run deletes each row of the window from the target, and inserts the rows of
    the window again. Thus a late row lands, and a changed row replaces its old
    version.
    """

    window_column: str

    def build_sql(
        self,
        effective_mode: RunMode,
        run_id: str,
        staging_address: TrouveAddress | None = None,
        target_column_names: list[str] | None = None,
    ) -> BuildPlan:
        if effective_mode == RunMode.FULL_REFRESH:
            return super().build_sql(effective_mode, run_id, staging_address)

        assert self.compiled is not None
        target = staging_address or self.physical_address
        window_rows = f"{self.physical_address}__window_{run_id}"
        create_window_rows = (
            f"CREATE OR REPLACE TABLE {window_rows} AS (\n{self.compiled.resolved_sql}\n)"
        )
        delete_window = (
            f"DELETE FROM {target}\n"
            f"WHERE {self.window_column} >= (SELECT MIN({self.window_column}) FROM {window_rows})"
        )
        insert_window = self.insert_statement(str(target), window_rows, target_column_names)
        return BuildPlan(
            statements=[create_window_rows, delete_window, insert_window],
            cleanup_statements=[f"DROP TABLE IF EXISTS {window_rows}"],
        )
