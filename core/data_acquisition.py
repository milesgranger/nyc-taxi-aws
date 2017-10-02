import io
import bs4
import requests
import numpy as np
import pandas as pd
import dask.dataframe as dd
from datetime import datetime
from typing import Union

requests.adapters.DEFAULT_RETRIES = 10


class TaxiData:

    @staticmethod
    def clean_df(df) -> pd.DataFrame:
        """
        Clean the dataframe to have self.columns

        Parameters
        ----------
        df: dataframe coming in from load_from_url() method

        Returns
        -------
        pd.DataFrame: cleaned to only have self.columns
        """

        columns = [
            'pickup_datetime',
            'dropoff_datetime',
            'tip_amount',
            'fare_amount',
            'total_amount',
            'vendor_id',
            'passenger_count',
            'trip_distance',
            'payment_type',
            'tolls_amount',
        ]

        df.columns = [col.lower() for col in df.columns]
        df = df.rename(columns={'vendor_name': 'vendor_id',
                                'total_amt': 'total_amount',
                                'tolls_amt': 'tolls_amount',
                                'fare_amt': 'fare_amount',
                                'tip_amt': 'tip_amount',
                                'trip_pickup_datetime': 'pickup_datetime',
                                'trip_dropoff_datetime': 'dropoff_datetime'
                                })
        df.columns = map(lambda col:
                         col.strip().replace('_', '').replace('tpep', ''),
                         df.columns
                         )
        df = df.rename(columns={col.replace('_', ''): col for col in columns})
        df = df.loc[:, columns]

        # Convert date columns
        for date_col in ['pickup_datetime', 'dropoff_datetime']:
            df[date_col] = pd.to_datetime(df[date_col], yearfirst=True, errors='coerce')

        # Convert numberical columns
        def convert(val):
            try:
                return round(float(val), 2)
            except:
                return np.NaN
        for num_col in filter(lambda col: col.endswith('amount') or col.endswith('count') or col.endswith('distance'),
                              df.columns):
            df[num_col] = df[num_col].map(convert)

        # Fill all nulls with ''
        df = df.fillna(value='')

        return df

    @staticmethod
    def read_file(s3_input) -> dd.DataFrame:
        """
        Grab the remote s3 file
        s3_input: s3 uri to data file.
        """
        # From 2016-07 thru 2016-12, the header is missing a few columns, these are the correct ones for that
        # time period
        bad_2016_cols = ['vendorid',
                         'tpep_pickup_datetime',
                         'tpep_dropoff_datetime',
                         'passenger_count',
                         'trip_distance',
                         'pickup_longitude',
                         'pickup_latitude',
                         'ratecodeid',
                         'store_and_fwd_flag',
                         'dropoff_longitude',
                         'dropoff_latitude',
                         'payment_type',
                         'fare_amount',
                         'extra',
                         'mta_tax',
                         'tip_amount',
                         'tolls_amount',
                         'improvement_surcharge',
                         'total_amount']

        year, month = s3_input[-11:-4].split('-')
        if year == '2016' and month in ['07', '08', '09', '10', '11', '12']:
            df = dd.read_csv(s3_input,
                             dtype='object',
                             header=0,
                             names=bad_2016_cols,
                             error_bad_lines=False,
                             blocksize=int(128e6))
        else:
            df = dd.read_csv(s3_input,
                             dtype='object',
                             error_bad_lines=False,
                             blocksize=int(128e6))
        return df


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

