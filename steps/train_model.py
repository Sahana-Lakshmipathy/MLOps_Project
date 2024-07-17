import logging
import pandas as pd
from zenml import step

@step

def train_model(df: pd.DataFrame) -> None:
      """
      Trains a model on the given data.
      Args: df: the data to train the model on
      """
      pass