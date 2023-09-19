import re

#: Regular expression for Tier0 Run ID.

RX_RUN = re.compile(r"^[1-9][0-9]{1,6}$")
RX_STREAM = re.compile(r"[A-Z][0-9a-zA-Z]+")
RX_PRIMARY_DATASET = re.compile(r"[A-Z][0-9a-zA-Z]+")
RX_SCENARIO = re.compile(r"[a-zA-Z][0-9a-zA-Z]+")
RX_ERA = re.compile(r"[a-zA-Z][0-9a-zA-Z]+")
RX_EXPRESS_GLOBAL_TAG = re.compile(r"[1-9][0-9a-zA-Z]+")
RX_PROMPT_GLOBAL_TAG = re.compile(r"[1-9][0-9a-zA-Z]+")
