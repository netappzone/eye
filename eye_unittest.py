import unittest
import os
from main import *


class MyTestCase(unittest.TestCase):
    path = "/"
    csvpath = "/"

    def test_download_xml(self):
        """Function to test download_xml function"""

        # Test the right url
        self.assertEqual(
            download_xml(MyTestCase.url),

        )

        # Test the incorrect url
        self.assertEqual(
            download_xml("http://myurl.com"),
        )

    def test_extract_zip_link(self):
        """Function to test extract_zip_link function"""

        # Path to the source xml
        file = "eye.xml"

        # Path to wrong source file
        wr_file = "eye.png"

        # Test for correct zip
        self.assertEqual(
            extract_zip_link(file),
            (
                "DLTINS_20210117_01of01.zip",
            ),
        )

        # Test for incorrect zip
        self.assertEqual(extract_zip_link(wr_file), None)

    def test_download_zip(self):
        """Function to test download_zip function"""

        # Path to the zipped file
        zipped_file = os.path.join(self.path, "DLTINS_20210117_01of01.zip")

        self.assertTrue(download_zip(zipped_file, self.path))

        # Test for incorrect zip file
        self.assertFalse(download_zip("ade.zip",))

    def test_convert_xml(self):
        """Function to test create_csv funtion"""

        # absolute path to xml file to parse
        xml_file = os.path.join(self.path, "DLTINS_20210117_01of01.xml")

        # absolute path to the csv file to create
        csv_file = os.path.join(self.csvpath, "DLTINS_20210117_01of01.csv")

        # Test for correct data
        self.assertEqual(convert_xml(xml_file, self.csvpath), csv_file)

        # Test for wrong xml file
        self.assertEqual(convert_xml("ade", self.csvpath), None)

    def test_upload_to_aws(self):
        """Function to test upload_to_aws function"""

        # absolute path to the csv file to create
        csv_file = os.path.join(self.csvpath, "DLTINS_20210117_01of01.csv")

        # Test for correct data
        self.assertTrue(
            upload_to_aws(
                csv_file,
                self.r_name,
                self.aws_key_id,
                self.aws_access_key,
                self.buc_name,
            )
        )

        # Test for non existent bucket
        self.assertFalse(
            upload_to_aws(
                csv_file,
                "europe",
                self.aws_key_id,
                self.aws_access_key,
                self.buc_name,
            )
        )


if __name__ == '__main__':
    unittest.main()
