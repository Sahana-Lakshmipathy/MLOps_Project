import logging
import pandas as pd

from zenml import step


class IngestData:
   """
   Class to ingest data from a given path.
   """
   def __init__(self, data_path:str):
      """
      Constructor for IngestData class.
      """
      self.data_path = data_path


   def get_data(self):
      """
      Reads the data from the given path and returns it as a Pandas DataFrame.
      """
      logging.info(f"Reading data from {self.data_path}")
      return pd.read_csv(self.data_path)

@step
def ingest_data(data_path: str) -> None:
   """
   Reads the data from the given path and returns it as a Pandas DataFrame.
   Args: data_path: path to the data

   Returns: pd.DataFrame: the ingested data
   """
   try:
      ingest_data = IngestData(data_path)
      df = ingest_data.get_data()
      return df
   except Exception as e:
      logging.error(f"Error in ingesting data: {e}")
      raise e

