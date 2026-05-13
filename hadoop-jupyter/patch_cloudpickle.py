"""
Patches Spark 2.4.8's cloudpickle.py to work with Python 3.8+.
Python 3.8 added co_posonlyargcount to types.CodeType constructor.
"""
import sys

filepath = "/opt/spark/python/pyspark/cloudpickle.py"

with open(filepath, "r") as f:
    content = f.read()

# In Python 3.8+, types.CodeType requires co_posonlyargcount after co_kwonlyargcount.
# The else branch in _make_cell_set_template_code has:
#   co.co_kwonlyargcount,
#   co.co_nlocals,
# We need to insert co.co_posonlyargcount between them.

old = "co.co_kwonlyargcount,\n            co.co_nlocals,"
new = "co.co_kwonlyargcount,\n            co.co_posonlyargcount,\n            co.co_nlocals,"

if old not in content:
    print("WARNING: Pattern not found, file may already be patched or has unexpected format")
    sys.exit(0)

content = content.replace(old, new)

with open(filepath, "w") as f:
    f.write(content)

print("Successfully patched cloudpickle.py for Python 3.8+ compatibility")
