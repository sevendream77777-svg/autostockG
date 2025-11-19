
import pathlib
BASE = pathlib.Path(__file__).resolve().parent
def find_latest(folder, prefix):
    files = sorted(folder.glob(f"{prefix}*"), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None
HOJ_DB_RESEARCH = find_latest(BASE / "HOJ_DB" / "RESEARCH", "HOJ_DB_RESEARCH")
HOJ_DB_REAL     = find_latest(BASE / "HOJ_DB" / "REAL", "HOJ_DB_REAL")
HOJ_ENGINE_RESEARCH = find_latest(BASE / "HOJ_ENGINE" / "RESEARCH", "HOJ_ENGINE_RESEARCH")
HOJ_ENGINE_REAL     = find_latest(BASE / "HOJ_ENGINE" / "REAL", "HOJ_ENGINE_REAL")
if __name__ == "__main__":
    for name in sorted(globals()):
        if name.isupper():
            print(name, "=", globals()[name])
