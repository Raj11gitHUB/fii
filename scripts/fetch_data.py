import os
from datetime import date, timedelta
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


class download:
    def __init__(self):
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "*/*",
            "Referer": "https://www.nseindia.com",
        }

        self.session = requests.Session()
        self.session.headers.update(self.header)

    # CLIENT OI + VOLUME
    def client_oi(self, start_date, end_date, dow_path_client, copy_path_client):
        os.makedirs(copy_path_client, exist_ok=True)
        os.chdir(dow_path_client)

        url_prefix = "https://archives.nseindia.com/content/nsccl/"
        files = ["fao_participant_oi_", "fao_participant_vol_"]

        for url_suffix in files:
            for i in daterange(start_date, end_date):
                c = i.strftime("%d%m%Y")
                file_name = f"{url_suffix}{c}.csv"
                url = url_prefix + file_name

                print("Downloading:", url)
                resp = self.session.get(url)

                if resp.status_code != 200:
                    print("Failed:", resp.status_code)
                    continue

                with open(file_name, "wb") as f:
                    f.write(resp.content)

                try:
                    df = pd.read_csv(file_name, header=None)
                    df["Date"] = i.strftime("%d-%m-%Y")
                except:
                    print("Invalid CSV:", file_name)
                    continue

                categories = ["Client", "FII", "DII", "Pro"]

                for cat in categories:
                    part = df[df[0] == cat]
                    if not part.empty:
                        out_dir = f"{copy_path_client}/{url_suffix}{cat}.csv"
                        part.to_csv(out_dir, mode="a", index=False, header=False)


# MAIN AUTO SCRIPT
if __name__ == "__main__":
    d = download()

    # Always fetch yesterday
    today = date.today()
    yesterday = today - timedelta(days=1)

    start_date = yesterday
    end_date = yesterday

    dow_path = "data"         # download folder
    copy_path = "data"        # processed output

    d.client_oi(start_date, end_date, dow_path, copy_path)
