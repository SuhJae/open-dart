from openDart.client import OpenDartClient
from openDart.endpoints.financials import CompanyFinancialReport

# Init
api = OpenDartClient(api_key="")

corp_id = api.corp_cache.get_id_by_stock_code("005380")

compony_info = api.company.get_company_summary(corp_id)
print(f"🔍 Company: {compony_info.corp_name} ({compony_info.corp_id})")
print(compony_info)

# Merged storage for all reports
all_reports: dict[str, CompanyFinancialReport] = {}

# Loop backwards from 2024 Q4 to 2015 Q1
for year in reversed(range(2015, 2025)):
    for quarter in reversed(range(1, 5)):
        try:
            reports = api.financials.get_financials(corp_id, fiscal_year=year, quarter=quarter)
        except Exception as e:
            print(f"Skipping {year} Q{quarter} due to error: {e}")
            continue

        for key, new_report in reports.items():
            if key not in all_reports:
                all_reports[key] = new_report
            else:
                # Merge into existing report
                existing = all_reports[key]
                for stmt_type, stmt_data in new_report.statements.items():
                    if stmt_type not in existing.statements:
                        existing.statements[stmt_type] = stmt_data
                    else:
                        for acc, new_entries in stmt_data.time_series_by_account.items():
                            existing.statements[stmt_type].time_series_by_account.setdefault(acc, []).extend(
                                new_entries)

# Sort all entries by date per account
for report in all_reports.values():
    for stmt_data in report.statements.values():
        for values in stmt_data.time_series_by_account.values():
            values.sort(key=lambda x: x.report_date)

# Print grouped trends
for key, report in all_reports.items():
    print(f"\n🔹 {key} | {report.consolidated_label}")
    for stmt_type, stmt_data in report.statements.items():
        print(f"  🔸 Statement: {stmt_type}")
        for account_name, time_series in stmt_data.time_series_by_account.items():
            print(f"    📈 {account_name}")
            for entry in time_series:
                if entry.start_date and entry.end_date:
                    print(f"      - {entry.start_date.date()} ~ {entry.end_date.date()}: {entry.amount:,.0f}")
                else:
                    print(f"      - {entry.report_date.date()}: {entry.amount:,.0f}")
