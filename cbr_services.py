import requests
from xml.etree import ElementTree


def get_daily_rate():
    try:
        response = requests.get('http://www.cbr.ru/scripts/XML_daily.asp?')
        tree = ElementTree.fromstring(response.content)
        for element in tree.findall("Valute"):
            if element.attrib == {'ID': 'R01235'}:
                curs = element.find('Value')
                curs = float(curs.text.replace(',', '.'))
                return curs
    except requests.RequestException as ex:
        print(ex)
