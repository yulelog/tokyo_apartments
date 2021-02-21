import pandas as pd
from datetime import date
import pykakasi

KKS = pykakasi.kakasi()


def get_distance_to_station(closest_stations):
  '''
  transform the string of 'closest_stations' feature into a list of tuples 
  where the train line is at index 0, station at index 1, and walking time to station at index 2
  '''
  l = closest_stations.split('\n')
  l = [s.split() if any(map(str.isdigit, s)) else l.remove(s) for s in l]
  l = [i if ((i is not None) and (len(i) == 3)) else l.remove(i) for i in l]
  l = filter(None, l)
  return [(to_romaji(i[0]),to_romaji(i[1]), int(i[2][:-1])) for i in l]


def to_romaji(text):
    """
    Convert a japanese phrase into romaji
    :param text: japanese text in string
    :return : romaji string 
    """
    result = KKS.convert(text)
    return ''.join([i['hepburn'].capitalize() for  i in result])


def check_property_type(p):
  if '気泡コンクリート造' in p:
    return 'ALC'
  elif ('鉄骨' in p and '鉄筋' in p) or ('SRC' in p):
    return 'SRC'
  elif '鉄骨' in p:
    return 'S'
  elif ('鉄筋' in p) or ('RC' in p) or ('ＲＣ' in p):
    return 'RC'
  elif '木' in p:
    return 'W'
  elif 'その他' or 'コンクリート' in p:
    return 'Others'
  elif 'プレキャスト' in p:
    return 'PC'
  else:
    return p

def get_floor_properties(floor):
    """
    :param floor: expect to be a string
    """
    if type(floor) is str:
        floor = floor.split('/')
        unit_floor = floor[0].replace('階','').replace('地下','-').split('～')
        lowest_floor = min([int(n.strip()) for n in unit_floor]) if len(unit_floor)> 1 else int(unit_floor[0].strip())
        floor_number = max([int(n.strip()) for n in unit_floor]) - min([int(n.strip()) for n in unit_floor])+1 if len(unit_floor)> 1 else 1
        building_height = int(floor[1][floor[1].index('地上')+2:floor[1].index('階')])
        return {'floor': floor, 'unit_floor': unit_floor, 'lowest_floor': lowest_floor, 'floor_number': floor_number, 'building_height': building_height}
    else:
        return {'floor': floor}

def built_to_date(built_date):
  """
  calculate the length since built year till the date the property is available for rent online 
  :param built_date: a tuple of (year, month, 1)
  :return : number of years since the building is built
  """
  try:
    return date.today().year - built_date[0]
  except ValueError:
    return None

def clean_up(df):

    # Pick out the intuitively relevant attributes from the dataframe and translate them into English for easier access
    translate_dict = {
        'id': 'id', 
        '物件タイプ': 'property_type',  # categorical
        '構造': 'structure',  # categorical
        '間取り': 'floor_plan',  # split into two: number of bedrooms (numerical) and floor_plan (ordinal)
        '専有面積': 'size',  # numerical
        '賃料': 'monthly_rent',  # numerical (target variable)
        '最寄り駅': 'closest_stations',  # list of tuples with string and numerical value
        '階': 'floor',  # interval
        '築年月': 'built_date',  # date
        'こだわり条件': 'specs'  # list of strings
        }
    df = df.rename(columns=translate_dict)[translate_dict.values()]

    # Remove rows that do not contain values for the target variable: monthly_rent 
    # (they're from the webpage for building instead of individual property unit)
    df = df[df['monthly_rent'].isna()==False].reset_index(drop=True)
    df = df[df['monthly_rent'].str.contains('円' )].reset_index(drop=True)

    # Translate string into numbers for size, monthly_rent
    df['size'] = [float(i[:i.index('m')]) for i in df['size']]
    df['monthly_rent'] = [int(i[:i.index('円')].replace(',','')) for i in df['monthly_rent']]

    # remove commercial properties
    df = df[~df['floor_plan'].isin(['店舗/事務所', '店舗','事務所'])].reset_index(drop=True)

    # splitting floor_plan into two attributes: bedroom_num and floor_plan 
    floor_plan_map = {'K': 1, 'DK': 2, 'LDK': 3, 'SLDK': 4}
    df['bedroom_num'] = [int(i[0]) if ord(i[0])<=57 else 0 for i in df['floor_plan'] ] 
    df['floor_plan'] = [floor_plan_map[i[1:]] if ord(i[0])<=57 else 0 for i in df['floor_plan']]

    # transform the 'closest_stations' feature into a list of tuples indicating distance to the train line and station
    df['closest_stations'] = [get_distance_to_station(i) for i in df['closest_stations']]
    
    # floor
    df[['floor', 'unit_floor', 'lowest_floor', 'floor_number', 'building_height']] = pd.DataFrame([get_floor_properties(i) for i in df['floor']])
   
    # built_date
    df['built_date'] = [(int(i[:i.index('年')]), int(i[i.index('年')+1:i.index('月')]), 1) if type(i) is str else i for i in df['built_date']]  # assign built date to be the first of every month
    df['built_to_date'] = [built_to_date(d) for d in df['built_date']]

    # translate property type
    property_type = {
        'マンション': 'mansion', 
        '店舗/事務所': 'office', 
        '戸建/テラスハウス': 'house', 
        'アパート': 'apartment',
        'タワーマンション': 'tower_mansion'
        }
    df['property_type'] = [property_type[i] if i in property_type.keys() else None for i in df['property_type']]
    
    # translate structure
    df['structure'] = [check_property_type(i) if type(i) is str else i for i in df['structure']]

    return df

