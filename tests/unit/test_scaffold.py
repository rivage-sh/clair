"""Tests for clair.core.scaffold -- project initialisation."""

from __future__ import annotations

from pathlib import Path


from clair.core.scaffold import scaffold_project, write_environments_yml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EXPECTED_PROJECT_FILES = [
    "source/raw/orders.py",
]

DEFAULT_SOURCE_ARGS = dict(
    source_database_name="source",
    source_schema_name="raw",
    source_table_name="orders",
)


def _run_scaffold(tmp_path: Path) -> list[tuple[str, str]]:
    """Run scaffold_project with a fake home dir so we never touch the real one."""
    project_dir = tmp_path / "my_project"
    fake_home = tmp_path / "fake_home"
    return scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS, home_dir=fake_home)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestScaffoldCreatesAllExpectedFiles:
    def test_creates_all_project_files(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "my_project"
        fake_home = tmp_path / "fake_home"
        scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS, home_dir=fake_home)

        for relative_path in EXPECTED_PROJECT_FILES:
            full_path = project_dir / relative_path
            assert full_path.exists(), f"Missing: {relative_path}"

    def test_creates_environments_yml(self, tmp_path: Path) -> None:
        fake_home = tmp_path / "fake_home"
        scaffold_project(tmp_path / "proj", **DEFAULT_SOURCE_ARGS, home_dir=fake_home)

        environments_path = fake_home / ".clair" / "environments.yml"
        assert environments_path.exists()

    def test_returns_all_paths_as_created(self, tmp_path: Path) -> None:
        results = _run_scaffold(tmp_path)

        # 1 project file + 1 environments.yml
        assert len(results) == 2
        assert all(status == "created" for status, _ in results)


class TestFileContents:
    def test_orders_trouve_has_source_type(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "proj"
        scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS, home_dir=tmp_path / "home")

        content = (project_dir / "source/raw/orders.py").read_text()
        assert "TrouveType.SOURCE" in content

    def test_environments_yml_has_dev_environment(self, tmp_path: Path) -> None:
        fake_home = tmp_path / "home"
        scaffold_project(tmp_path / "proj", **DEFAULT_SOURCE_ARGS, home_dir=fake_home)

        content = (fake_home / ".clair" / "environments.yml").read_text()
        assert "dev:" in content
        assert "account:" in content
        assert "externalbrowser" in content
        assert "  routing:" not in content  # routing omitted by default; shown as comment only


class TestDoesNotOverwriteExistingFiles:
    def test_skips_existing_project_file(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "proj"
        orders_path = project_dir / "source" / "raw" / "orders.py"
        orders_path.parent.mkdir(parents=True)
        orders_path.write_text("# my custom file\n")

        results = scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS, home_dir=tmp_path / "home")

        assert orders_path.read_text() == "# my custom file\n"

        results_dict = {path: status for status, path in results}
        assert results_dict[str(orders_path)] == "skipped"

    def test_skips_existing_environments_yml(self, tmp_path: Path) -> None:
        fake_home = tmp_path / "home"
        environments_path = fake_home / ".clair" / "environments.yml"
        environments_path.parent.mkdir(parents=True)
        environments_path.write_text("# existing config\n")

        scaffold_project(tmp_path / "proj", **DEFAULT_SOURCE_ARGS, home_dir=fake_home)

        assert environments_path.read_text() == "# existing config\n"


class TestProjectDirCreation:
    def test_creates_project_dir_if_missing(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "brand_new" / "nested" / "project"
        assert not project_dir.exists()

        scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS, home_dir=tmp_path / "home")

        assert project_dir.exists()
        assert (project_dir / "source" / "raw" / "orders.py").exists()


class TestCustomSourceDbName:
    def test_source_directory_named_after_source_db(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "proj"
        scaffold_project(
            project_dir,
            source_database_name="mydb",
            source_schema_name="myschema",
            source_table_name="mytable",
            home_dir=tmp_path / "home",
        )

        assert (project_dir / "mydb" / "myschema" / "mytable.py").exists()


class TestWriteEnvironmentsYml:
    def test_writes_env_with_connection_fields(self, tmp_path: Path) -> None:
        env_data = {
            "account": "myorg-myaccount",
            "user": "me@example.com",
            "authenticator": "externalbrowser",
            "warehouse": "COMPUTE_WH",
            "role": "ANALYST",
        }
        path = write_environments_yml(
            env_data, env_name="dev", home_dir=tmp_path
        )

        assert path.exists()
        content = path.read_text()
        assert "dev:" in content
        assert "  account: myorg-myaccount" in content
        assert "  user: me@example.com" in content
        assert "  warehouse: COMPUTE_WH" in content
        assert "  role: ANALYST" in content

    def test_omits_routing(self, tmp_path: Path) -> None:
        env_data = {
            "account": "myorg-myaccount",
            "user": "me@example.com",
            "warehouse": "COMPUTE_WH",
        }
        path = write_environments_yml(env_data, home_dir=tmp_path)
        content = path.read_text()
        assert "  routing:" not in content

    def test_writes_to_correct_location(self, tmp_path: Path) -> None:
        env_data = {"account": "x", "user": "y", "warehouse": "z"}
        path = write_environments_yml(env_data, home_dir=tmp_path)
        assert path == tmp_path / ".clair" / "environments.yml"
