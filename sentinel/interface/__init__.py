"""Interface layer — REST API, alerts, and report generation."""
from sentinel.interface.alerts import AlertManager
from sentinel.interface.reports import ReportGenerator

__all__ = ["AlertManager", "ReportGenerator"]
