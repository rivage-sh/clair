from clair import PlanContext, Table


class AuditedTable(Table):
    """Keep each promoted candidate as a snapshot, and do not drop it."""

    def retire(self, context: PlanContext) -> list[str]:
        snapshot = f"{context.physical_address}__audit_{context.run_id}"
        return [f"ALTER TABLE {context.staging_address} RENAME TO {snapshot}"]
