#!/usr/bin/env python3
"""Check both project nixpkgs locks; optionally compare a consuming flake lock."""
import json
from pathlib import Path
import sys

root = Path(__file__).resolve().parents[1]


def pin(path):
    data = json.loads(path.read_text())
    name = data["nodes"][data["root"]]["inputs"]["nixpkgs"]
    return data["nodes"][name]["locked"]


paths = [root / "flake.lock", root / "devenv.lock"]
paths.extend(Path(p) for p in sys.argv[1:])
expected = pin(paths[0])
for path in paths[1:]:
    assert pin(path) == expected, f"nixpkgs lock differs: {path}"
assert expected["rev"] in (root / "devenv.yaml").read_text()
print(f"nixpkgs pins match: {expected['rev']} ({len(paths)} locks)")
