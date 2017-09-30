import click
import s3fs
import pandas as pd
from core.data_acquisition import TaxiData


@click.command()
@click.option('--s3-file', help='s3 URI file location')
@click.option('--output-bucket', help='s3 URI for bucket this file should be written back to')
def clean_file(s3_file, output_bucket):

    cr = CleanRoutine(s3_input=s3_file, s3_output_bucket=output_bucket)
    cr.process()
    print(s3_file, output_bucket)


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

    def process(self):
        """
        Entry point for cleaning process
        """

    def _fetch_file(self):
        """ 
        Grab the remote s3 file
        """

    def _deposit_file(self):
        """
        Deposit file to remote bucket
        """

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return super().clean_df(df)


if __name__ == '__main__':
    clean_file()
