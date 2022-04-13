import requests
import xml.etree.ElementTree as et
from lxml import etree

main_link = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01" \
            "-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100 "


def main():
    download_xml()
    download_zip('eye.xml')


def download_xml():
    """
    Get file from the internet
    """
    r = requests.get(main_link, allow_redirects=True)
    open('eye.xml', 'wb').write(r.content)


def download_zip(file):
    """
    Get the link from the downloaded xml
    """
    tree = et.parse(file)
    root = tree.getroot()
    [print(response.attrib) for response in root.iter()]

    # for child in root:
    #     print(child.tag, child.attrib)


if __name__ == '__main__':
    main()
