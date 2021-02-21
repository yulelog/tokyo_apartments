import sys
import requests
from requests import get
from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
import shutil
import logging
from data_cleanup import clean_up
from feature_engineering import generate_features
from s3 import save_image, load_to_s3


logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# output_file_handler = logging.FileHandler(f"/home/ubuntu/logs/tokyo_apartments_{date.today().strftime('%Y%m%d')}.log")
stdout_handler = logging.StreamHandler(sys.stdout)
# LOGGER.addHandler(output_file_handler)
LOGGER.addHandler(stdout_handler)

HEADERS = requests.utils.default_headers()
HEADERS.update({
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
})


SITES = [
     'https://tomigaya.jp/', 
     'https://daikanyamafudosan.com/', 
     'http://nakame-re.com/',
     'https://aoyama-fudousan.com/',
     'http://tokyo-shinagawa.com/'
     ]


def get_property_list(url):
  """
  Given a subpage, get all the property listings from the page
  :param url: a property list url
  :return : list of urls to the property detail pages on the given property list page
  """
  page = get(url, headers=HEADERS)
  soup = BeautifulSoup(page.content, "lxml")
  try:
    result_list = soup.find('div',{'class':'result_list'})
    return [tag.get('href') for tag in result_list.findAll('a')]
  except AttributeError:  # when no result_list class found in the parsed BeautifulSoup object, return None
    return None


def get_property_details(root, property_id):
  """
  Given the property website and property id, scrape all the details about the property and store in a dictionary
  :param root: the root url of the property website
  :param property_id: the property id suffix after root url in string scraped from the property listing page
  :return : dictionary of the property details
  """
  page = get(root + property_id, headers=HEADERS)
  soup = BeautifulSoup(page.content, "lxml")

  details = soup.find('table')  # the property details are stored in the table
  if details is None:
    return {}
  
  id = property_id.replace('/id/','').replace('/','_')
  d = {'id': id}

  try:
    image_url = root + get_floorplan_link(soup)
    d.update({'floorplan_image': image_url})  # "floorplan_image" key saves the image link to the floor plan image
    save_image(image_url, id) # save down the floorplan image
  except IndexError: # when there's no image
    pass

  for p in details.findAll('tr'):  # each property detail is stored in individual rows, with the attribute name in the header cell <th>, and the attribute data in the data cell <td>
    try:
      d.update({p.find('th').text.strip(): p.find('td').text.strip()})
    except AttributeError:  # there's a hidden row in the table for Google Map, which we'd skip here 
      continue
  
  return d


def get_floorplan_link(soup):
  """
  Find the url to the property's floor plan image
  :param soup: BeautifulSoup object of parsed property detail page
  :return : the src tag containing url link to the floor plan image
  """
  return soup.findAll('img',{'class': 'sp-thumbnail'})[0].get('src')


def get_all_properties(root, subpage_tags):
  """
  Given a property website and its subpage tags, go through each subpage and generate a complete list of urls to available properties 
  :param root: the root url of the property website
  :param pages: list of sub-pages url tags on the given property site
  :return : list of urls to individual property pages
  """
  # initiate an empty property list
  property_list = []

  # go through each subpage tag from page 1 till when it stops returning property list
  for subpage in subpage_tags:
    page_number = 1
    properties = []
    while properties is not None:
      url = root+'/' + subpage + '/page:' + str(page_number)
      properties = get_property_list(url)
      try:
        property_list.extend(properties)
        page_number += 1
      except TypeError:  # when properties variable is returned as None, it cannot be extended to the original list
        LOGGER.info(f"Last page found for {subpage} was {page_number-1}")
        continue
  
  return property_list


def get_website_properties(root, subpage_tags):
  """
  Given a property website and its subpage tags, scrape all the available properties on the site and their details, store in a list of dictionaries
  :param root: the root url of the property website
  :param pages: list of sub-pages url tags on the given property site
  :return : a list of dictionaires containing property details on a given website
  """

  # initiate an empty list for property details
  property_details = []
  property_list = get_all_properties(root, subpage_tags)
  LOGGER.info(f"[INFO] {len(property_list)} total property listings found from the site.")

  # expecting that there would be redundant listings cross-listed under different sub-pages, we will set the property list to contain distinct properties
  property_list = set(property_list)
  LOGGER.info(f"[INFO] {len(property_list)} property listings remain after removing duplicates.")

  # variables for logging progress
  total_numbers = len(property_list)
  i = 0

  for property in property_list:
    property_details.append(get_property_details(root, property))
    
    # logging progress
    i +=1
    progress = round(i/total_numbers*100, 1)
    if (progress / 10).is_integer() :
        LOGGER.info(f"[INFO] {str(progress)}% of total properties scraped.")

  LOGGER.info("[INFO] Finished scrapping all property data")
  return property_details

def get_sitemap(url):
    """
    given a real estate site, get its sitemap of property locations
    :param url: url link to the page, string
    :return : sitemap in a list of strings
    """
    page = get(url, headers=HEADERS)
    soup = BeautifulSoup(page.content, "lxml")
    return [tag.get('href') for tag in soup.find('div',{'class':'sitemap'}).findAll('a')]


def main():
    """
    main function to scrape data from a series of property websites and save down the data in csv
    """
    for url in SITES:
        LOGGER.info(f"[INFO] Started scraping {url}")
        subpage_tags = get_sitemap(url)
        property_details = get_website_properties(url, subpage_tags)
        df = pd.DataFrame(property_details)
        filename = f"{url[url.index('//')+2:url.index('.')]}_{date.today().strftime('%Y%m%d')}.csv"
        load_to_s3(df, 'raw_data/'+filename)
        load_to_s3(generate_features(clean_up(df)),'processed_data/'+filename)
        LOGGER.info(f"[INFO] Finished scraping {url} and uploaded {filename} to s3")


if __name__ == '__main__':
    main()
