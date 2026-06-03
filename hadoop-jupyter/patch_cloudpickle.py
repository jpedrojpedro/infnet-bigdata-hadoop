#!/usr/bin/env python3

import shutil
import tempfile
import zipfile
from pathlib import Path

PATCH_MARKER = "co.co_posonlyargcount"

REPLACEMENT_OLD = """co.co_kwonlyargcount,
            co.co_nlocals,"""

REPLACEMENT_NEW = """co.co_kwonlyargcount,
            co.co_posonlyargcount,
            co.co_nlocals,"""


def patch_text(text: str):
    if PATCH_MARKER in text:
        return text, False

    if REPLACEMENT_OLD not in text:
        raise RuntimeError(
            "Could not find Spark 2.4 cloudpickle pattern to patch."
        )

    return text.replace(REPLACEMENT_OLD, REPLACEMENT_NEW), True


def patch_file(path: str):
    path = Path(path)

    if not path.exists():
        print(f"[SKIP] {path} does not exist")
        return

    content = path.read_text()

    patched_content, changed = patch_text(content)

    if not changed:
        print(f"[OK] Already patched: {path}")
        return

    backup = path.with_suffix(path.suffix + ".bak")

    if not backup.exists():
        shutil.copy2(path, backup)

    path.write_text(patched_content)

    print(f"[PATCHED] {path}")


def patch_zip(zip_path: str):
    zip_path = Path(zip_path)

    if not zip_path.exists():
        print(f"[SKIP] {zip_path} does not exist")
        return

    backup = zip_path.with_suffix(".zip.bak")

    if not backup.exists():
        shutil.copy2(zip_path, backup)

    with zipfile.ZipFile(zip_path, "r") as zin:

        temp_fd, temp_name = tempfile.mkstemp(suffix=".zip")

        with zipfile.ZipFile(temp_name, "w") as zout:

            modified = False

            for item in zin.infolist():

                data = zin.read(item.filename)

                if item.filename == "pyspark/cloudpickle.py":

                    text = data.decode("utf-8")

                    patched_text, changed = patch_text(text)

                    if changed:
                        modified = True
                        data = patched_text.encode("utf-8")

                zout.writestr(item, data)

    shutil.move(temp_name, zip_path)

    if modified:
        print(f"[PATCHED] {zip_path}")
    else:
        print(f"[OK] Already patched: {zip_path}")


if __name__ == "__main__":

    patch_file("/opt/spark/python/pyspark/cloudpickle.py")

    patch_zip("/opt/spark/python/lib/pyspark.zip")

    print("Done.")
