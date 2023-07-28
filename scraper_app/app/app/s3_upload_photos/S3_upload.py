import random
import boto3
import requests
from app.core.config import settings
from app.utils import get_static_proxies


class S3upload:
    def __init__(self):
        self.bucket = settings.AWS_BUCKET
        self.aws_access_key_id = settings.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY
        self.session = self.open_session()
        self.s3 = self.session.resource('s3')
        self.upload_bucket = self.s3.Bucket(self.bucket)
        self.bad_proxies = []
        self.proxies = get_static_proxies()

    def open_session(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        return session

    def choose_proxy(self):
        while True:
            proxy = random.choice(self.proxies)
            if proxy not in self.bad_proxies:
                return proxy

    def check_for_bad_proxy(self, err, proxy):
        if 'Connection to ' in err and 'timed out.' in err:
            self.bad_proxies.append(proxy)

    def upload_photo(self, picture_url, username, userid, logger):
        if self.aws_access_key_id:
            object_name = f"profile-pics/{username}--{userid}.jpg"
            cnt = 0
            r = None
            while True:
                proxy = self.choose_proxy()
                try:
                    r = requests.get(picture_url, stream=True, proxies={'https': proxy}, timeout=10)
                    break
                except Exception as err:
                    self.check_for_bad_proxy(str(err), proxy)
                    cnt += 1
                    if cnt % 3 == 0:
                        logger.error(f"Proxy: {proxy}, err: {err}")
                    if cnt == 10:
                        logger.error(f"FATAL Proxy: {proxy}, err: {err}")
                        break
            if r and r.status_code == 200:
                while True:
                    try:
                        self.upload_bucket.upload_fileobj(r.raw, object_name)
                        return True
                    except Exception as err:
                        cnt += 1
                        if cnt % 3 == 0:
                            logger.error(f"Upload: {proxy}, err: {err}")
                        if cnt == 10:
                            logger.error(f"FATAL Upload: {proxy}, err: {err}")
                            break
                return False
            else:
                return False
