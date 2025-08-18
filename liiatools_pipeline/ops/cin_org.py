from dagster import In, Out, op, get_dagster_logger
from fs.base import FS

from liiatools.cin_census_pipeline.reports import reports
from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesCINReports
from liiatools.common.data import DataContainer
from liiatools_pipeline.assets.common import shared_folder, workspace_folder


log = get_dagster_logger()

@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_cin_reports_session_folder() -> FS:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesCINReports
    )
    session_folder = session_folder.opendir(SessionNamesCINReports.INCOMING_FOLDER)

    reports_folder = workspace_folder().opendir("current/cin/PAN")
    pl.move_files_for_sharing(
        reports_folder, session_folder
    )

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def expanded_assessment_factors(session_folder: FS):
    log.info("Creating Export Directories...")
    export_folder = workspace_folder().makedirs(
        "current/cin", recreate=True
    )
    report_folder = export_folder.makedirs("REPORTS", recreate=True)

    pan_cin = pl.open_file(session_folder, "cin_cin.csv")
    expanded_assessment_factors = reports.expanded_assessment_factors(pan_cin)
    expanded_assessment_factors = DataContainer({"factors": expanded_assessment_factors})

    existing_report_files = report_folder.listdir("/")
    pl.remove_files("cin_factors", existing_report_files, report_folder)
    log.info("Exporting report factors to report folder...")
    expanded_assessment_factors.export(report_folder, "cin_", "csv")

    existing_shared_files = shared_folder().listdir("/")
    log.info("Exporting report factors to shared folder...")
    pl.remove_files(
        "PAN_cin_factors", existing_shared_files, shared_folder()
    )
    expanded_assessment_factors.export(shared_folder(), "PAN_cin_", "csv")


@op(
    ins={
        "session_folder": In(FS),
    },
)
def referral_outcomes(session_folder: FS):
    log.info("Creating Export Directories...")
    export_folder = workspace_folder().makedirs(
        "current/cin", recreate=True
    )
    report_folder = export_folder.makedirs("REPORTS", recreate=True)

    pan_cin = pl.open_file(session_folder, "cin_cin.csv")
    referral_outcomes = reports.referral_outcomes(pan_cin)
    referral_outcomes = DataContainer({"referrals": referral_outcomes})

    existing_report_files = report_folder.listdir("/")
    pl.remove_files("cin_referrals", existing_report_files, report_folder)
    log.info("Exporting report referrals to report folder...")
    referral_outcomes.export(report_folder, "cin_", "csv")

    existing_shared_files = shared_folder().listdir("/")
    log.info("Exporting report referrals to shared folder...")
    pl.remove_files(
        "PAN_cin_referrals", existing_shared_files, shared_folder()
    )
    referral_outcomes.export(shared_folder(), "PAN_cin_", "csv")


@op(
    ins={
        "session_folder": In(FS),
    },
)
def s47_journeys(session_folder: FS):
    log.info("Creating Export Directories...")
    export_folder = workspace_folder().makedirs(
        "current/cin", recreate=True
    )
    report_folder = export_folder.makedirs("REPORTS", recreate=True)

    pan_cin = pl.open_file(session_folder, "cin_cin.csv")
    s47_journeys = reports.s47_journeys(pan_cin)
    s47_journeys = DataContainer({"S47_journeys": s47_journeys})

    existing_report_files = report_folder.listdir("/")
    pl.remove_files("cin_S47_journeys", existing_report_files, report_folder)
    log.info("Exporting report s47_journeys to report folder...")
    s47_journeys.export(report_folder, "cin_", "csv")

    existing_shared_files = shared_folder().listdir("/")
    log.info("Exporting report s47_journeys to shared folder...")
    pl.remove_files(
        "PAN_cin_S47_journeys", existing_shared_files, shared_folder()
    )
    s47_journeys.export(shared_folder(), "PAN_cin_", "csv")
