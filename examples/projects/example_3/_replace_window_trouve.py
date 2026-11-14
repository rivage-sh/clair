from clair import PlanContext, RunMode, Table


class ReplaceWindowTrouve(Table):
    """An incremental TABLE that replaces each row of a recent window.

    A full refresh builds the table in the same way as a Trouve. An incremental
    run deletes each row of the window from the target, and inserts the rows of
    the window again. Thus a late row lands, and a changed row replaces its old
    version.
    """

    window_column: str

    def window_rows_address(self, run_id: str) -> str:
        return f"{self.require_compiled().physical_address}__window_{run_id}"

    def build(self, context: PlanContext) -> list[str]:
        if context.effective_mode == RunMode.FULL_REFRESH:
            return super().build(context)

        window_rows = self.window_rows_address(context.run_id)
        create_window_rows = f"CREATE OR REPLACE TABLE {window_rows} AS (\n{self.resolved_sql()}\n)"
        delete_window = (
            f"DELETE FROM {context.target_address}\n"
            f"WHERE {self.window_column} >= (SELECT MIN({self.window_column}) FROM {window_rows})"
        )
        insert_window = self.insert_statement(
            str(context.target_address), window_rows, context.target_column_names
        )
        return [create_window_rows, delete_window, insert_window]

    def cleanup(self, context: PlanContext) -> list[str]:
        if context.effective_mode == RunMode.FULL_REFRESH:
            return []
        return [f"DROP TABLE IF EXISTS {self.window_rows_address(context.run_id)}"]
