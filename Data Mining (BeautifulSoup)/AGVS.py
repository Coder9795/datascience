"""

AGVS Dealer information Scrapper

"""
# import required libraries

from datetime import datetime
import concurrent.futures
from collections import Counter
import math
import pandas as pd
import re

from bs4 import BeautifulSoup

# local imports
from common import get_connection, escape_name, beautify, call_scraper_api, end_display, start_display

# declare constants
URL = f"https://www.agvs-upsa.ch/de/verband/mitgliederverzeichnis/liste?distance&page="
APP_NAME = "AVGS Dealer Scraper"
TABLE_NAME = "agvs_dealers"
counter = 0
SPACE = 39
# div details
div_dict = {
    "title": "views-field-title",
    "name": "views-field-field-name",
    "bezeichnung": "views-field-field-bezeichnung",
    "bezeichnung-2": "views-field-field-name2",
    "strasse": "views-field-field-strasse",
    "plz": "views-field-field-plz",
    "ortschaft": "views-field-field-ortschaft",
    "postfach": "views-field-field-postfach",
    "telefon": "views-field-field-telefon",
    "fax": "views-field-field-fax",
    "email": "views-field-field-address-email",
    "webseite": "views-field-field-url",
    "sektion": "views-field-field-sektion",
    "kanton": "views-field-field-kanton",
    "aec-zertifiziert": "views-field-field-aecgaragist"
    }
dealers = []


def insert_into_agvs_dealers(values):
    """
    Inserts a value into table
    :param values:
    :return:
    """
    connection = get_connection()
    cursor = connection.cursor(buffered = True)
    names = list(values[0])
    cols = ", ".join(map(escape_name, names))
    placeholders = ", ".join(['%({})s'.format(name) for name in names])
    query = 'INSERT INTO {} ({}) VALUES ({}) on duplicate key update webseite = VALUES(webseite), removed_at = NULL' \
        .format('agvs_dealers', cols, placeholders)
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()
    connection.close()
    
    
def get_existing_agvs_dealer_list():
    connection = get_connection()
    try:
        cursor = connection.cursor(buffered = True)
        query = 'SELECT title FROM agvs_dealers'
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        connection.close()
        return result
    except Exception as e:
        beautify(SPACE, "Error", ": " + str(e))


def scrapper(url):
    """
    function to import all dealers from AVGS
    
    :param url: url
    :return: nothing
    """
    global counter
    counter = counter + 1
    print("Counter - ", counter, " ", url)
    response = call_scraper_api(url)
    # check if data fetched successfully
    if response.status_code == 200:
        # read contents
        soup = BeautifulSoup(response.content, "html.parser")
        
        # copy required details
        all_content_div = soup.find_all("div", attrs = {"class": "view-content"})
        for mainDiv in all_content_div:
            all_dealer_div = mainDiv.find_all("div", attrs = {"class": "views-row"})
            for div in all_dealer_div:
                value_dict = {}
                # find all fields
                for title in div_dict:
                    field_div = div.find("div", attrs = {"class": div_dict[title]})
                    value_dict[title] = field_div.find(class_ = "field-content").text if field_div else None
                    # check if page has information
                    if not value_dict["title"]:
                        break
                else:
                    # save results to table "AGVS"
                    dealers.append(value_dict)


def main():
    """
    
    :return:
    """
    start_time = datetime.now()
    # Application start display
    start_display(APP_NAME, start_time)
    
    # Scrapping Dealers
    beautify(SPACE, "Stage", ": Getting Pages\n")
    
    url = URL + str(0)
    response = call_scraper_api(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_count = soup.find("div", attrs = {"class": "view-footer"}).getText()
    pages = re.search(".* ([0-9]+)", page_count).group(1)
    pages = math.ceil(float(pages)/40)

    beautify(SPACE, "Number of pages", ": "+str(pages)+"\n")
    
    list_of_urls = []
    for i in range(int(pages)):
        url = URL + str(i)
        list_of_urls.append(url)
    existing_dealers = get_existing_agvs_dealer_list()

    beautify(SPACE, "Stage", ": Scrapping Dealers\n")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers = 20) as executor:
        executor.map(scrapper, list_of_urls)

    beautify(SPACE, "Stage", ": Data transfer to mysql \n")
    beautify(SPACE, "Inserting data to mysql", ": " + str(len(dealers)))
    insert_into_agvs_dealers(dealers)
    
    beautify(SPACE, "Status", ": Completed Successfully")
    beautify(SPACE, "Pages Scrapped", f": {counter}")

    try:
        beautify(SPACE, "\nStage", ": Checking records to delete")
    
        new_dealers = []
        for dealer in dealers:
            new_dealers.append(str(dealer.get('title')))
    
        existing_dealers_list = pd.DataFrame(existing_dealers, columns = ['title'])['title'].tolist()
        removed_dealer_list = list((Counter(existing_dealers_list) - Counter(new_dealers)).elements())
        beautify(SPACE, "existing_dealers_list in mysql", ": " + str(len(existing_dealers_list)))
        beautify(SPACE, "Updating removed_dealer_list in mysql", ": " + str(len(removed_dealer_list)))
    
        to_delete = []
        for i in range(0, len(removed_dealer_list)):
            data = {"title": removed_dealer_list[i]}
            to_delete.append(data)
    
        if len(to_delete) > 0:
            connection = get_connection()
            cursor = connection.cursor(buffered = True)
            names = list(to_delete[0])
            cols = ", ".join(map(escape_name, names))
            placeholders = ", ".join(['%({})s'.format(name) for name in names])
            query = 'INSERT INTO {} ({}) VALUES ({}) on duplicate key update removed_at = CURRENT_TIMESTAMP' \
                .format('agvs_dealers', cols, placeholders)
            cursor.executemany(query, to_delete)
            connection.commit()
            cursor.close()
            connection.close()
    
    except Exception as e:
        beautify(SPACE, ": " + str(e))
    
    # Application end display
    end_display(start_time)


if __name__ == "__main__":
    main()
