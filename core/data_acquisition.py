import io
import bs4
import requests
import pandas as pd


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

    def clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
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
        df = df.rename(columns={col.replace('_', ''): col for col in self.columns})
        return df.loc[:, self.columns]

    def load_from_url(self, url: str, n_bytes: int) -> pd.DataFrame:
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

    def yield_s3_links(self, to_year: str, to_month: str):
        """
        Yield Yellow cab taxi links to the S3 data file for each year and month, limited by
        passed params

        parameters
        ----------
        from_year: str of length 4
        from_month: str of length 2
        to_year: str of length 4
        from_month: str of length 2

        returns
        -------
        yields str links to S3 datafile
        """

        resp = requests.get('http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml')
        if not resp.ok:
            raise IOError('Failed to read content from nyc.gov')
        soup = bs4.BeautifulSoup(resp.content, 'html5lib')

        for year_data in soup.findAll('div', attrs={'id': 'accordion'}):

            for month_link in [link for link in year_data.findAll('a', href=True) if 'yellow' in link['href']]:

                year, month = month_link['href'][-11:-3].split('-')
                if year == '2014': break
                print('Year: {}, Month: {}, URL: {}'.format(year, month, month_link['href']))
                yield month_link['href']
            if year == '2014': break

