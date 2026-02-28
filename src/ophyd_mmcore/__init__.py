from importlib.metadata import version

try:
    __version__ = version("ophyd-mmcore")
except Exception:
    __version__ = "unknown"
