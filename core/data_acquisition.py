import io
import bs4
import requests
import pandas as pd
from datetime import datetime
from typing import Union

requests.adapters.DEFAULT_RETRIES = 10

class TaxiData:

    # columns we care about, all dataframes from 'clean_df()' will have these columns
    columns = [
        'pickup_datetime',
        'dropoff_datetime',
        'tip_amount',
        'fare_amount',
        'total_amount',
        'vendor_id',
        'passenger_count',
        'trip_distance',
        'dropoff_longitude',
        'dropoff_latitude',
        'pickup_longitude',
        'pickup_latitude',
        'payment_type',
        'tolls_amount',
    ]


    @classmethod
    def clean_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the dataframe to have self.columns

        Parameters
        ----------
        df: dataframe coming in from load_from_url() method

        Returns
        -------
        pd.DataFrame: cleaned to only have self.columns
        """
        df.columns = map(lambda col: col.lower().replace('_', '').replace('tpep', ''), df.columns)
        df = df.rename(columns={col.replace('_', ''): col for col in cls.columns})
        return df.loc[:, cls.columns]


    @classmethod
    def load_from_url(cls, url: str, n_bytes: int) -> pd.DataFrame:
        """
        Load the url to n_bytes and return the dataframe

        Parameters
        ----------
        url: str - url of datafile
        n_bytes: int - number of bytes to read

        Returns
        -------
        pd.DataFrame of the datafile
        """
        resp = requests.get(url, headers={'Range': 'bytes=0-{}'.format(n_bytes)})
        if not resp.ok:
            raise ConnectionError('Unable to read datafile from {}'.format(url))

        return pd.read_csv(io.BytesIO(resp.content))


    @classmethod
    def yield_s3_links(cls, to_date: Union[str, datetime]) -> str:
        """
        Yield Yellow cab taxi links to the S3 data file for each year and month, up through passed year and month

        Parameters
        ----------
        to_date: str or datetime object - Read latest dataset through to and including this time.
                                          if str, should be in format 'YYYY-MM'

        Returns
        -------
        yields str links to S3 datafile
        """
        to_date = datetime.strptime(to_date, '%Y-%m') if not isinstance(to_date, datetime) else to_date

        # Fetch page which contains links to data files
        resp = requests.get('http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml')
        soup = bs4.BeautifulSoup(resp.content, 'html5lib')

        # Each year, get links to each month
        for year_data in soup.findAll('div', attrs={'id': 'accordion'}):

            # for each month, yield if it is within the to_date boundary.
            for month_link in [link for link in year_data.findAll('a', href=True) if 'yellow' in link['href']]:
                year, month = month_link['href'][-11:-4].split('-')  # Each file ends with ...<year>-<month>.csv
                if datetime.strptime('{year}-{month}'.format(year=year, month=month), '%Y-%m') >= to_date:
                    yield month_link['href']

