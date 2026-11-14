from clair import PlanContext, RunMode, Table


class MirroredUpsert(Table):
    """An Upsert that keeps the production table object.

    clair tests the MERGE on a clone of production. The promotion then runs the
    same MERGE into production, from the same new rows table, and does not
    replace the table. A full refresh still replaces it.
    """

    def promote(self, context: PlanContext) -> list[str]:
        if context.effective_mode == RunMode.FULL_REFRESH:
            return super().promote(context)
        return [
            # prepare added each new column to the clone. Production needs them too.
            *self.add_column_statements(context.physical_address, context.added_columns),
            self.merge_statement(
                context.physical_address,
                self.new_rows_address(context.run_id),
            ),
        ]
