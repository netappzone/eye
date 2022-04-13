import requests
import xml.etree.ElementTree as et

main_link = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
zip_results = []


def main():
    # download_xml()
    extract_zip('eye.xml')
    download_zip(zip_results[0])


def download_xml():
    """
    Get file from the internet
    """
    r = requests.get(main_link, allow_redirects=True)
    open('eye.xml', 'wb').write(r.content)


def extract_zip(file):
    """
    Get the link from the downloaded xml
    """
    tree = et.parse(file)
    root = tree.getroot()
    # [print(response.attrib) for response in root.iter()]

    for element in root.iter():
        key_values = {}
        if element.tag == 'str' and element.attrib['name'] == 'download_link':
            key_values['str name download_link'] = element.text
            zip_results.append(element.text)
    print(f'Second element: {zip_results[0]}')
    return zip_results[0]


def download_zip(url):
    """
    Get file from the internet
    """
    r = requests.get(url, allow_redirects=True)
    # Split URL to get the file name
    filename = url.split('/')[-1]

    # Writing the file to the local file system
    with open(filename, 'wb') as output_file:
        output_file.write(r.content)
    print('Downloading Completed')


if __name__ == '__main__':
    main()
