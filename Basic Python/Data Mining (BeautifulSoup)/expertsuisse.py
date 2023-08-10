"""

Expert Suisse Scrapper

"""

import threading
from datetime import datetime

from bs4 import BeautifulSoup

# local imports
from common import *

existing_vehicles = get_existing_tutti_list()

obj_config = Config()

# declare constants
APP_NAME = "Expert Suisse Scrapper"
URL = "https://www.expertsuisse.ch/mitglieder-finden?page={page}"
headers = {
    'Content-Type': 'application/json'
    }
THREAD_COUNT = 10


def get_individual_data(url):
    """

    :param url:
    :return:
    """
    response = call_limited_api(url)
    if response.status_code == 200:
        try:
            soup = BeautifulSoup(response.content, "html.parser")
            body = soup.find('div', {'class': 'popupbody'})
            name = body.find('h2').getText().replace('\t', '').replace('\n', '').strip() if body.find('h2') else None
            address = body.find('p').getText().replace('\t', '').replace('\n', '').replace('Adresse', '').strip() if body.find('p') else None
            contact_fields = body.find_all('div')
            phone, website, email = None, None, None
            for div in contact_fields:
                if 'Telefon' in div.getText():
                    phone = div.getText().replace('\t', '').replace('\n', '').replace('Telefon: ','').replace(' ', '').strip()
                elif 'E-Mailadresse' in div.getText():
                    email = div.getText().replace('\t', '').replace('\n', '').replace('E-Mailadresse: ', '').strip()
                elif 'Webseite' in div.getText():
                    website = div.getText().replace('\t', '').replace('\n', '').replace('Webseite: ', '').strip()
            json_data = {
                "source": 'expertsuisse',
                "name": name,
                "address": address,
                "email": email,
                "website": website,
                "url": url,
                "phone": phone
                }
            table_name = "affiliates"
            
            connection = get_connection()
            cursor = connection.cursor(buffered = True)
            names = list(json_data)
            cols = ", ".join(map(escape_name, names))
            placeholders = ", ".join(['%({})s'.format(name) for name in names])
            query = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, cols, placeholders)
            cursor.execute(query, json_data)
            connection.commit()
            cursor.close()
            connection.close()
        
        except Exception as e:
            beautify(SPACE, "Error", ": " + str(e))
    
    else:
        beautify(SPACE, "Error", ": Page not Found\n")


def main():
    """

    :return:
    """
    start_time = datetime.now()
    # Application start display
    start_display(APP_NAME, start_time)
    for i in range(1, 115):
        url = URL.format(page = i)
        beautify(SPACE, "URL", ": " + str(url))
        response = call_limited_api(url)
        soup = BeautifulSoup(response.content, "html.parser")
        items = len(soup.find('div', {'class': 'list'}).find_all('div', {'class': 'item'}))
        try:
            for j in range(1, items + 1, THREAD_COUNT):
                all_threads = []
                for i in range(j, min(j + THREAD_COUNT, items + 1)):
                    calling_url = soup.find('div', {'class': 'list'}).find_all('div', {'class': 'item'})[i].find(href = True)['href']
                    thread = threading.Thread(target = get_individual_data, args = (calling_url,))
                    thread.start()
                    all_threads.append(thread)
                for thread in all_threads:
                    thread.join()
            
            beautify(SPACE, "Status", ": Completed Successfully\n")
        except Exception as e:
            beautify(SPACE, "Error", "1 : " + str(e))
    # Application end display
    end_display(start_time)


if __name__ == "__main__":
    main()
