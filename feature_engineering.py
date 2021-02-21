import pandas as pd
from datetime import date
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
import csv

SPECS_MAP = {row['jp']: row['eng'] for row in csv.DictReader(open('spec_map.csv'))}

def specs_to_dict(l):
  """
  convert a list of specs into dictionary
  :param l: list of specs in string
  :return : a dictionary 
  """
  d = {}
  if type(l) is list:
    [d.update({SPECS_MAP[spec]: 1}) if spec in SPECS_MAP.keys() for spec in l]
  return d

def specs_to_features(col):
  """
  convert the spec column into binary features
  :param col: pandas.Series of the spec column
  :return : pandas dataframe of the specs features
  """  
  return pd.DataFrame([specs_to_dict(item.split('・')) if type(item) is str else {} for item in col]).fillna(0) # TODO: should I fillna here?

def stations_to_dict(col):
  """
  convert a column of tuples indicating distance to stations into numerical features
  :param col: pandas.Series of the stations column where each row is a list of tuples (<subway_line>, <station>, <walking distance>)
  :return : a dictionary where keys are the stations name, and values are the walking distance
  """
  d = {}
  if type(col) is list:
    [d.update({stations[1]: stations[2]}) for stations in col]
  return d

def stations_to_features(col):
  """
  convert the stations column into numerical features
  :param col: pandas.Series of the stations column
  :return : pandas dataframe of the stations features
  """  
  return pd.DataFrame([stations_to_dict(item) for item in col]) # TODO: what should I do with the NAs?


def category_to_feature(col):
  """
  transform a column of categorical data into dataframe of binary features
  :param col: pandas.Series of the category column to be converted
  :return : pandas.Dataframe of transformed binary features
  """
  # transform and map pokemon generations
  le = LabelEncoder()
  labels = le.fit_transform(col)

  # encode generation labels using one-hot encoding scheme
  ohe = OneHotEncoder()
  feature_arr = ohe.fit_transform(labels.reshape(-1, 1)).toarray()
  feature_labels = list(le.classes_)
  features = pd.DataFrame(feature_arr, columns=feature_labels)
  return features

def generate_features(df):
    """
    Given a pandas dataframe, concatenate all the newly generated features to the original dataframe
    :return : pandas dataframe of newly generated features
    """
    specs_df = specs_to_features(df['specs'])
    stations_df = stations_to_features(df['closest_stations'])
    property_df = category_to_feature(df['property_type'])
    structure_df = category_to_feature(df['structure'])
    
    return pd.concat([df, specs_df, stations_df, property_df, structure_df], axis=1)
    