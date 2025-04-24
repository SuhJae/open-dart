from .cache.corp_code import CorpCodeCache
from .endpoints.company import CompanyAPI
from .endpoints.financials import FinanciasAPI


class OpenDartClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.financials = FinanciasAPI(api_key)
        self.company = CompanyAPI(api_key)
        self.corp_cache = CorpCodeCache(api_key)

    def get_corp_info_by_name(self, name: str):
        return self.corp_cache.get_by_name(name)
