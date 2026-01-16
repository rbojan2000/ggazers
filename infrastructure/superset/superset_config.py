import os

# ------------------------------------------------------------------------------
# Core Superset config
# ------------------------------------------------------------------------------

# Set the maximum number of rows to display in the chart
DISPLAY_MAX_ROW = 40000  # Adjust this value as needed

# Set the default query limit
DEFAULT_SQLLAB_LIMIT = 40000

SECRET_KEY = os.getenv(
    "SUPERSET_SECRET_KEY",
    "AsAd@$#afgt4567!"
)

# ------------------------------------------------------------------------------
# Metadata database
# ------------------------------------------------------------------------------

SQLALCHEMY_DATABASE_URI = os.getenv(
    "SUPERSET__SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://ggazers:ggazers123@postgres:5432/ggazers"
)

# Prevent SQLite fallback
SQLALCHEMY_TRACK_MODIFICATIONS = False

# ------------------------------------------------------------------------------
# Security / sessions
# ------------------------------------------------------------------------------

SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

# ------------------------------------------------------------------------------
# Feature flags
# ------------------------------------------------------------------------------

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}
