"""
Things that don't change very often.
"""

from sqlglot.dialects.dialect import Dialects

DIALECTS = {
    "postgresql": Dialects.POSTGRES,
    "gsheets": Dialects.SQLITE,
}
