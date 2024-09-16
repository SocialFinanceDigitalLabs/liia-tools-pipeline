try:
    from enum import StrEnum
except ImportError:
    from backports.strenum import StrEnum


class ProcessNames(StrEnum):
    """Enum for process folders."""

    SESSIONS_FOLDER = "sessions"
    CURRENT_FOLDER = "current"


class SessionNames(StrEnum):
    """Enum for session folders."""

    INCOMING_FOLDER = "incoming"
    CLEANED_FOLDER = "cleaned"
    ENRICHED_FOLDER = "enriched"
    DEGRADED_FOLDER = "degraded"


class SessionNamesFixEpisodes(StrEnum):
    """Enum for fix episodes session folders."""

    INCOMING_FOLDER = "incoming"


class SessionNamesOrg(StrEnum):
    """Enum for org session folders."""

    INCOMING_FOLDER = "incoming"
