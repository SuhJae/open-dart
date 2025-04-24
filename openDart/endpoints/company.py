from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

from . import BaseModel, BaseAPI


@dataclass
class CompanySummary(BaseModel):
    corp_id: str
    corp_name: str
    corp_name_eng: Optional[str]
    stock_name: Optional[str]
    stock_code: Optional[str]
    ceo_name: Optional[str]
    corp_class: Optional[str]
    registration_number: Optional[str]
    business_number: Optional[str]
    address: Optional[str]
    homepage: Optional[str]
    ir_homepage: Optional[str]
    phone_number: Optional[str]
    fax_number: Optional[str]
    industry_code: Optional[str]
    established_date: Optional[datetime]  # Parsed to datetime
    closing_month: Optional[str]

    @classmethod
    def from_api(cls, data: Dict) -> "CompanySummary":
        est_date_str = data.get("est_dt")
        est_date = datetime.strptime(est_date_str, "%Y%m%d") if est_date_str else None

        return cls(
            corp_id=data.get("corp_code", ""),
            corp_name=data.get("corp_name", ""),
            corp_name_eng=data.get("corp_name_eng"),
            stock_name=data.get("stock_name"),
            stock_code=data.get("stock_code"),
            ceo_name=data.get("ceo_nm"),
            corp_class=data.get("corp_cls"),
            registration_number=data.get("jurir_no"),
            business_number=data.get("bizr_no"),
            address=data.get("adres"),
            homepage=data.get("hm_url"),
            ir_homepage=data.get("ir_url"),
            phone_number=data.get("phn_no"),
            fax_number=data.get("fax_no"),
            industry_code=data.get("induty_code"),
            established_date=est_date,
            closing_month=data.get("acc_mt")
        )


class CompanyAPI(BaseAPI):
    BASE_URL = "https://opendart.fss.or.kr/api/company.json"

    def __init__(self, api_key: str, *, client=None, async_client=None):
        super().__init__(api_key, client=client, async_client=async_client)

    def get_company_summary(self, corp_id: str) -> CompanySummary:
        params = {
            "corp_code": corp_id
        }

        result = self._get(self.BASE_URL, params)

        if not isinstance(result, dict) or result.get("status") != "000":
            raise ValueError(f"API error: {result.get('message', 'Unknown error')}")

        return CompanySummary.from_api(result)
