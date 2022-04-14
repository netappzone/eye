import xml.etree.ElementTree as et
from io import BytesIO

import os
import requests
import zipfile
import pandas as pd
import logger
import boto3



main_link = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
zip_results = []


def main():
    download_xml()
    extract_zip_link('eye.xml')
    download_zip(zip_results[0])
    convert_xml('DLTINS_20210117_01of01.xml', "./")


def download_xml():
    """
    Get file from the internet
    """
    r = requests.get(main_link, allow_redirects=True)
    open('eye.xml', 'wb').write(r.content)


def extract_zip_link(file):
    """
    Get the link from the downloaded xml
    """
    tree = et.parse(file)
    root = tree.getroot()

    for element in root.iter():
        key_values = {}
        if element.tag == 'str' and element.attrib['name'] == 'download_link':
            key_values['str name download_link'] = element.text
            zip_results.append(element.text)
    logger.info('zip file extracted')
    return zip_results[0]


def download_zip(url):
    """
    Get file from the internet
    """
    logger.info('Downloading Started')
    r = requests.get(url, allow_redirects=True)
    # Split URL to get the file name
    filename = url.split('/')[-1]
    # Writing the file to the local file system
    with open(filename, 'wb') as output_file:
        output_file.write(r.content)
    logger.info('Downloading Completed')

    # extracting the zip file contents
    with zipfile.ZipFile(BytesIO(r.content)) as zip_ref:
        zip_ref.extractall()
        zip_ref.close()


def convert_xml(xml_file, path):
    """ Convert XML to CSV File
    """
    try:
        # Checking if the path exists or not
        if not os.path.exists(path):
            # Creating the path
            logger.info("File path created")
            os.makedirs(path)

        # Extracting the csv file name from xml file
        csv_name = xml_file.split(os.sep)[-1].split(".")[0] + ".csv"

        # Creating csv file path
        csv_file = os.path.join(path, csv_name)

        logger.info("Xml file Loading")
        xml_iter = et.iterparse(xml_file, events=("start",))

        csv_columns = [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
        ]

        # Creating empty dataframe with the required column names
        df = pd.DataFrame(columns=csv_columns)

        # List to store the extacted data
        extracted_data = []

        logger.info("Xml file...")
        logger.info("Extracting data from xml...")
        # Traversing the xml data
        for event, element in xml_iter:

            # Checking for start of the tags
            if event == "start":

                # Checking for TermntdRcrd tag in which the required data is
                if "TermntdRcrd" in element.tag:

                    # Dictionary to store require data in single element
                    data = {}

                    # List of the required tags (FinInstrmGnlAttrbts, Issr)
                    reqd_elements = [
                        (elem.tag, elem)
                        for elem in element
                        if "FinInstrmGnlAttrbts" in elem.tag or "Issr" in elem.tag
                    ]

                    # Traversing through the required tags
                    for tag, elem in reqd_elements:

                        if "FinInstrmGnlAttrbts" in tag:

                            # Traversing through the child elements of
                            # FinInstrmGnlAttrbts element
                            for child in elem:

                                # Adding the extrcated data in the dictionary
                                if "Id" in child.tag:
                                    data[csv_columns[0]] = child.text
                                elif "FullNm" in child.tag:
                                    data[csv_columns[1]] = child.text
                                elif "ClssfctnTp" in child.tag:
                                    data[csv_columns[2]] = child.text
                                elif "CmmdtyDerivInd" in child.tag:
                                    data[csv_columns[3]] = child.text
                                elif "NtnlCcy" in child.tag:
                                    data[csv_columns[4]] = child.text

                        # Extracting Issr Tag value
                        else:
                            data[csv_columns[5]] = child.text

                    # Appending the single element extracted data in the list
                    extracted_data.append(data)

        logger.info("Xml file extraction completed")

        # Appending the extracted data in the data frame
        df = df.append(extracted_data, ignore_index=True)

        logger.info("Using Pandas to drop empty rows")
        # Removes empty rows from the dataframe
        df.dropna(inplace=True)

        logger.info("Creating the CSV file")
        # Creates csv file from the dataframe
        df.to_csv(csv_file, index=False)

        # returning the csv file path
        return csv_file

    except Exception as e:

        logger.error(f"Error occurred during extraction - {str(e)}")

    def upload_to_aws(
            file, r_name, key_id, access_key, bucket_name
    ):
        """Uploading CSV file to s3 bucket
        Param(s):
            file (str)                  :   Path of file to upload to s3 bucket
            region_name (str)           :   name of region s3 bucket is hosted
            aws_access_key_id (str)     :   AWS access key
            aws_secret_access_key (str) :   AWS secret access key
            bucket_name (str)           :   name of the bucket
        Return(s):
            true  or false (bool)
        """

        try:
            # Extracting the file name from the path
            filename_in_s3 = file.split(os.sep)[-1]

            logger.info("Creating the S3 object")
            # Connecting to the bucket with boto3
            s3 = boto3.resource(
                service_name="s3",
                region_name=r_name,
                aws_access_key_id=key_id,
                aws_secret_access_key=access_key,
            )

            logger.info("Uploading the file to s3 bucket")
            # Uploading the file to the s3
            s3.Bucket(bucket_name).upload_file(Filename=file, Key=filename_in_s3)

            logger.info("uploaded successfully")

            # returning True for successful upload
            return True
        except Exception as e:
            logger.error(f"Error occurred during upload - {str(e)}")


if __name__ == '__main__':
    main()
