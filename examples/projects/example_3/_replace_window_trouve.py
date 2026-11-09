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
        target_address: TrouveAddress,
        target_column_names: list[str] | None = None,
    ) -> BuildPlan:
        if effective_mode == RunMode.FULL_REFRESH:
            return super().build_sql(effective_mode, run_id, target_address)

        compiled = self.require_compiled()
        window_rows = f"{compiled.physical_address}__window_{run_id}"
        create_window_rows = (
            f"CREATE OR REPLACE TABLE {window_rows} AS (\n{compiled.resolved_sql}\n)"
        )
        delete_window = (
            f"DELETE FROM {target_address}\n"
            f"WHERE {self.window_column} >= (SELECT MIN({self.window_column}) FROM {window_rows})"
        )
        insert_window = self.insert_statement(str(target_address), window_rows, target_column_names)
        return BuildPlan(
            statements=[create_window_rows, delete_window, insert_window],
            cleanup_statements=[f"DROP TABLE IF EXISTS {window_rows}"],
        )
