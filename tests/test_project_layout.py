from pathlib import Path


def test_second_milestone_directories_exist():
    required = [
        "research/notebooks",
        "research/reports",
        "research/factor_library",
        "research/scratch",
        "data/raw",
        "data/processed",
        "models/example_lgbm",
        "var",
    ]

    for path in required:
        assert Path(path).is_dir(), path
        assert (Path(path) / ".gitkeep").exists() or path == "research/notebooks"


def test_large_local_artifacts_are_ignored():
    ignore_text = Path(".gitignore").read_text()

    for pattern in [
        "data/**/*.parquet",
        "data/**/*.parquet.gzip",
        "models/**/*",
        "var/*",
        "!**/.gitkeep",
    ]:
        assert pattern in ignore_text
