import re

#: Regular expression for Tier0 Run ID.

RX_RUNID     = re.compile(r"^[0-9]{6}$")
RX_STREAM     = re.compile(r".{1,20}")
RX_PRIMARY_DATASET     = re.compile(r".{1,20}")
