# -*- coding: utf-8 -*-

import os
import click
import pandas as pd
import dask.dataframe as dd
from core.data_acquisition import TaxiData

"""
Clean routine for Batch Service container
'milesg/tda-datacleaner:latest python /workdir/core/batch_service/clean_routine.py --s3-file s3://file.csv --output-bucket s3://output
"""


@click.command()
@click.option('--s3-file', help='s3 URI file location')
@click.option('--output-bucket', help='s3 URI for bucket this file should be written back to')
def clean_file(s3_file, output_bucket):
    print('Got this file and output bucket: {}, {}'.format(s3_file, output_bucket))
    if not s3_file or not output_bucket:
        raise ValueError('Must specify both input s3 file and s3 output')

    cr = CleanRoutine(s3_input=s3_file, s3_output_bucket=output_bucket)
    cr.process()


class CleanRoutine(TaxiData):
    """
    Handle full cleaning process
    1. Fetch datafile from s3 bucket
    2. Perform cleaning action
    3. Write out cleaned dataframe to s3 bucket.
    """

    def __init__(self, s3_input: str, s3_output_bucket: str) -> None:
        """
        Initialize object with the s3 URI for input file and output bucket

        Parameters
        ----------
        s3_input (str): S3 URI of input file
        s3_output_bucket (str): S3 URI of output bucket

        Returns
        -------
        None
        """
        self.s3_input = s3_input
        self.s3_output_bucket = s3_output_bucket

    def process(self):
        """
        Entry point for cleaning process
        """
        print('Fetching file: ', self.s3_input)
        df = self._read_file()
        print('Mapping partitions over cleaning...')
        df = df.map_partitions(self.clean_df)
        print('Saving file to ' + self.s3_output_bucket)
        df = df.repartition(npartitions=4)
        self._deposit_file(df)

    def _read_file(self) -> dd.DataFrame:
        """ 
        Grab the remote s3 file
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

        year, month = self.s3_input[-11:-4].split('-')
        if year == '2016' and month in ['07', '08', '09', '10', '11', '12']:
            df = dd.read_csv(self.s3_input,
                             dtype='object',
                             header=0,
                             names=bad_2016_cols,
                             error_bad_lines=False,
                             blocksize=int(128e6))
        else:
            df = dd.read_csv(self.s3_input,
                             dtype='object',
                             error_bad_lines=False,
                             blocksize=int(128e6))
        return df

    def _deposit_file(self, df: dd.DataFrame) -> None:
        """
        Deposit file to remote bucket
        """
        year, month = self.s3_input[-11:-4].split('-')
        fname = os.path.basename(self.s3_input).replace(year, '{year}-*'.format(year=year))
        fname = '{bucket}/{fname}.gz'.format(bucket=self.s3_output_bucket, fname=fname)
        print('Saving file as: ' + fname)
        df.to_csv(fname, compression='gzip', index=False)

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Use same project cleaning function
        """
        return super().clean_df(df)


if __name__ == '__main__':
    clean_file()
