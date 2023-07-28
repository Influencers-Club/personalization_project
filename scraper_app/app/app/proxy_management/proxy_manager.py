import requests

from app.core.config import settings


class ProxyManager:
    def __init__(self, proxy_str, logger):
        self.proxy_str = proxy_str
        self.logger = logger
        self.success_requests = 0
        self.error_requests = 0
        self.proxy_management_system_url = settings.PROXY_MANAGEMENT_SYSTEM_URL
        self.project_name = settings.PROJECT_NAME

    def deal_with_error(self, error):
        self.increase_error_requests()
        if self.is_proxy_error(error):
            self.insert_proxy_error(error)

    def is_proxy_error(self, error):
        return 'ProxyError' in error

    def insert_proxy_error(self, error):
        url = self.proxy_management_system_url + '/api/v1/proxy-errors/add-proxy-error'
        params = {'proxy_str': self.proxy_str, 'project_name': self.project_name, "proxy_err": error}
        try:
            resp = requests.get(url, params=params).json()
            if resp.get('message') != 'OK':
                self.logger.error(resp)
        except Exception as e:
            self.logger.error(e)

    def increase_error_requests(self):
        self.error_requests += 1

    def increase_success_requests(self):
        self.success_requests += 1

    def insert_proxy_calls_in_daily_statistic(self):
        url = self.proxy_management_system_url + '/api/v1/daily-proxy-calls/update_daily_calls'
        params = {'proxy_str': self.proxy_str, 'project_name': self.project_name,
                  "failed_calls": self.error_requests, "success_calls": self.success_requests}
        try:
            resp = requests.get(url, params=params).json()
            if resp.get('message') != 'OK':
                self.logger.error(resp)
            self.success_requests = 0
            self.error_requests = 0
        except Exception as e:
            self.logger.error(e)
