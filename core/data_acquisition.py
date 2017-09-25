import io
import bs4
import requests
import pandas as pd


class TaxiData:

    def __init__(self):
        """

        """
        pass

    def yield_s3_links(self, from_year: str, from_month: str, to_year: str, to_month: str):
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
            

