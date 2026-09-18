"""Offline evidence verification only; no remote access or benchmark execution."""
import hashlib
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def records(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def main():
    for name, digest in read(ROOT / "evidence-manifest.json").items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
    historical = records(REPO / "bench-tpcds/aligned-0917/records3.jsonl")
    refs = {r["case"]: r["digest"] for r in historical if r["mode"] == "NATIVE"}
    summary = read(ROOT / "results-summary.json")
    all_records = []
    for folder, key, modes, count in (
        ("resource_aligned", "fair", ("NATIVE", "FGAC", "MASK2"), 96),
        ("original_config", "original", ("NATIVE", "FGAC", "MASK16", "MASK1"), 48),
    ):
        base = ROOT / "evidence" / folder
        rows = records(base / "records.jsonl")
        assert len(rows) == count
        assert len({r["sequence"] for r in rows}) == len(rows)
        assert read(base / "done.json")["ok"]
        assert all(r["digest"] == refs[r["case"]] for r in rows)
        for case in {r["case"] for r in rows}:
            saved = next(r["stats"] for r in summary[key]["rows"] if r["case"] == case)
            for mode in modes:
                runs = [r for r in rows if r["case"] == case and r["mode"] == mode]
                formal = [r for r in runs if r["rep"] > 0]
                assert len(runs) == 4 and sorted(r["rep"] for r in formal) == [1, 2, 3]
                values = [r["total_ms"] for r in formal]
                stat = summary["serial"][case] if mode == "MASK1" else saved[mode]
                assert stat["median_ms"] == statistics.median(values)
                assert stat["min_ms"] == min(values) and stat["max_ms"] == max(values)
                if mode.startswith("MASK"):
                    assert all(r["delivery_bytes"] == 806898847 for r in runs)
                    if mode != "MASK1":
                        for baseline, field in (("FGAC", "vs_fgac_pct"), ("NATIVE", "vs_native_pct")):
                            actual = (stat["median_ms"] / saved[baseline]["median_ms"] - 1) * 100
                            assert abs(actual - stat[field]) < 1e-9
        all_records.extend(rows)
    assert len(all_records) == 144 and sum(r["rep"] > 0 for r in all_records) == 108
    assert read(ROOT / "evidence/environment/data-after.json")["ok"]
    assert read(ROOT / "evidence/faults/mask.json")["completed"]
    denials = read(ROOT / "evidence/faults/baseline-denials.json")
    assert all(c["ok"] == (c["mode"] == "FGAC") for c in denials["checks"])
    assert read(ROOT / "evidence/faults/baseline-denials-auth-enabled.json")["ok"]
    print("Verified: evidence SHA256; 144 matching digests; 108 formal queries; repetitions, medians, ranges and ratios; recorded fault/configuration outcomes.")


if __name__ == "__main__":
    main()
