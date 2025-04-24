from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from . import BaseModel, BaseAPI


@dataclass
@dataclass
class FinancialValue:
    report_date: datetime  # Used for BS
    amount: float
    start_date: Optional[datetime] = None  # For IS
    end_date: Optional[datetime] = None  # For IS


@dataclass
class StatementAccounts:
    # Maps account name (e.g. "자산총계") to a list of values over time
    time_series_by_account: Dict[str, List[FinancialValue]] = field(default_factory=dict)


@dataclass
class CompanyFinancialReport(BaseModel):
    corp_id: str
    stock_code: Optional[str]
    fiscal_year: str
    currency_unit: str  # e.g. KRW
    consolidated_type: str  # e.g. "CFS" (연결재무제표), "OFS" (개별재무제표)
    consolidated_label: str  # e.g. "연결재무제표"

    # Maps statement type like "BS" (재무상태표), "IS" (손익계산서)
    statements: Dict[str, StatementAccounts] = field(default_factory=dict)

    def add_financial_value(
            self,
            statement_type: str,
            account_name: str,
            report_date: datetime,
            amount: float,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ):
        if statement_type not in self.statements:
            self.statements[statement_type] = StatementAccounts()

        time_series = self.statements[statement_type].time_series_by_account.setdefault(account_name, [])
        time_series.append(
            FinancialValue(
                report_date=report_date,
                amount=amount,
                start_date=start_date,
                end_date=end_date,
            )
        )

        time_series.sort(key=lambda entry: entry.report_date)

    @classmethod
    def group_reports_by_type(
            cls,
            corp_id: str,
            stock_code: str,
            raw_items: List[Dict],
    ) -> Dict[str, "CompanyFinancialReport"]:
        reports_by_type: Dict[str, CompanyFinancialReport] = {}

        for item in raw_items:
            consolidated_code = item["fs_div"]
            consolidated_name = item["fs_nm"]
            statement_code = item["sj_div"]
            account_name = item["account_nm"]
            currency = item.get("currency", "KRW")
            fiscal_year = item.get("bsns_year", "")

            report_key = f"{consolidated_code}-{statement_code}"

            if report_key not in reports_by_type:
                reports_by_type[report_key] = cls(
                    corp_id=corp_id,
                    stock_code=stock_code,
                    fiscal_year=fiscal_year,
                    currency_unit=currency,
                    consolidated_type=consolidated_code,
                    consolidated_label=consolidated_name,
                )

            raw_amount = item.get("thstrm_amount", "0").replace(",", "")

            # Handle date differently for IS vs BS
            raw_date = item.get("thstrm_dt", "").strip()
            try:
                if statement_code == "IS" and "~" in raw_date:
                    start_str, end_str = map(str.strip, raw_date.split("~"))
                    start_date = datetime.strptime(start_str, "%Y.%m.%d")
                    end_date = datetime.strptime(end_str, "%Y.%m.%d")
                    report_date = end_date  # Use end_date for sorting
                else:
                    start_date = end_date = None
                    report_date = datetime.strptime(raw_date.split(" ")[0], "%Y.%m.%d")

                amount = float(raw_amount)

                reports_by_type[report_key].add_financial_value(
                    statement_type=statement_code,
                    account_name=account_name,
                    report_date=report_date,
                    amount=amount,
                    start_date=start_date,
                    end_date=end_date,
                )

            except ValueError:
                continue  # Skip malformed dates/amounts

        return reports_by_type


class FinanciasAPI(BaseAPI):
    BASEURL = 'https://opendart.fss.or.kr/api/fnlttSinglAcnt.json'
    quarter_map = {
        1: '11013',  # Q1
        2: '11012',  # Q2
        3: '11014',  # Q3
        4: '11011',  # Annual
    }

    def get_financials(self, corp_id: str, fiscal_year: int = 2015, quarter: int = 1) -> Dict[
        str, CompanyFinancialReport]:

        if fiscal_year < 2015:
            raise ValueError("API only supports fiscal years from 2015 onwards.")
        if quarter not in self.quarter_map:
            raise ValueError("Quarter must be one of [1, 2, 3, 4].")

        params = {
            'corp_code': corp_id,
            'bsns_year': str(fiscal_year),
            'reprt_code': self.quarter_map[quarter],
        }

        raw_items = self._get(self.BASEURL, params).get("list", [])
        if not isinstance(raw_items, list) or not raw_items:
            raise ValueError("Invalid API response: expected non-empty list of financial entries.")

        stock_code = raw_items[0].get("stock_code", "")

        return CompanyFinancialReport.group_reports_by_type(
            corp_id=corp_id,
            stock_code=stock_code,
            raw_items=raw_items,
        )
