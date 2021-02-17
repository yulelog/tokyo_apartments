import requests
from requests import get
from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
import shutil
import logging
from data_cleanup import clean_up

# TODO: fix all the file paths: logs, data.csv, floorplan images to S3 buckets
PATH = '.'

logging.basicConfig(filename=f"{__name__}_{date.today().strftime('%Y%m%d')}.log", filemode='w', level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HEADERS = requests.utils.default_headers()
HEADERS.update({
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
})

SITES = [
    'https://tomigaya.jp/', 
    ]

# SITES = [
#     'https://tomigaya.jp/', 
#     'https://daikanyamafudosan.com/', 
#     'http://nakame-re.com/',
#     'https://aoyama-fudousan.com/',
#     'http://tokyo-shinagawa.com/'
#     ]


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

  id = property_id.replace('/id/','').replace('/','_')
  
  image_url = root + get_floorplan_link(soup)
  d = {'id': id, 'floorplan_image': image_url}  # "floorplan_image" key saves the image link to the floor plan image
  # save_image(image_url, id) # save down the floorplan image

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


def save_image(image_url, property_id):
    """
    Given a url to a property's floorplan image, save the image down locally with property id
    :param image_url: string of the url to the image
    :param property_id: the string of property id
    :return : nothing
    """
    # Open the url image, set stream to True, this will return the stream content.
    r = requests.get(image_url, stream = True)

    # Check if the image was retrieved successfully
    if r.status_code == 200:
        # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
        r.raw.decode_content = True
        
        filename = PATH + '/floorplans/' + property_id + '.jpg'
        # Open a local file with wb ( write binary ) permission.
        with open(filename,'wb') as f:
            shutil.copyfileobj(r.raw, f)
    else:
        LOGGER.error(f"[ERROR] Unable to download the floorplan image of property: {property_id}")


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
        filename = PATH + '/raw_data/' + url[url.index('//')+2:url.index('.')] + '_' + date.today().strftime('%Y%m%d') + '.csv'
        clean_up(df).to_csv(filename)
        LOGGER.info(f"[INFO] Finished scraping {url} and saved data to {filename}")


if __name__ == '__main__':
    main()