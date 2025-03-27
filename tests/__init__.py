"""
Package version and name.
"""

from importlib.metadata import PackageNotFoundError  # pragma: no cover
from importlib.metadata import version

try:
    # Change here if project is renamed and does not equal the package name
    DIST_NAME = __name__
    __version__ = version(DIST_NAME)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError
