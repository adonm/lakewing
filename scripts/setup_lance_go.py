#!/usr/bin/env python3
"""Install the pinned lance-go native library atomically (lance =8.0.0)."""

import hashlib
import os
from pathlib import Path
import platform
import shutil
import tarfile
import tempfile
import urllib.request

VERSION = "v0.1.0"
BASE = f"https://github.com/gstamatakis95/lance-go/releases/download/{VERSION}"
# linux-amd64 is the only platform this repo serves from; extend as needed.
ASSETS = {
    ("Linux", "x86_64"): ("liblance_go-linux-amd64.tar.gz",
                          "7aa4e3f4d85f54741720014ae0a9df8d6653b0fcddb6522f87cb76bdbc754e5e"),
}


def main():
    root = Path(__file__).resolve().parent.parent
    dest = root / ".deps/lance-go"
    marker = f"{VERSION}\n"
    if (dest / "version").exists() and (dest / "version").read_text() == marker:
        return
    system = platform.system()
    arch = {"x86_64": "x86_64", "aarch64": "arm64", "arm64": "arm64"}[platform.machine()]
    if (system, arch) not in ASSETS:
        raise SystemExit(f"unsupported platform: {system}-{arch}")
    asset, sha256 = ASSETS[(system, arch)]
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".lance-go-", dir=dest.parent) as tmp:
        staging = Path(tmp)
        archive = staging / asset
        print(f"Downloading {BASE}/{asset}", flush=True)
        with urllib.request.urlopen(f"{BASE}/{asset}", timeout=300) as response:
            digest = hashlib.sha256()
            with archive.open("wb") as output:
                while True:
                    block = response.read(1 << 20)
                    if not block:
                        break
                    digest.update(block)
                    output.write(block)
        if digest.hexdigest() != sha256:
            raise SystemExit(f"checksum mismatch for {asset}: upstream moved, bump the pin")
        with tarfile.open(archive) as bundle:
            bundle.extractall(staging, filter="data")
        archive.unlink()
        if dest.exists():
            shutil.rmtree(dest)
        sub = staging / "liblance_go"
        target = sub if sub.is_dir() else staging
        dest.mkdir(exist_ok=True)
        for file in sorted(target.rglob("*")):
            rel = file.relative_to(target)
            if file.is_dir():
                (dest / rel).mkdir(exist_ok=True)
            else:
                os.replace(file, dest / rel)
        (dest / "version").write_text(marker)
    print(f"Installed lance-go native library {VERSION}")


if __name__ == "__main__":
    main()
