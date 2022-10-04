import unittest
import logging
from main import SteelEye
import pathlib as pl


class TestSteelEye(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Create and configure logger
        logger = logging.getLogger(__name__)
        file_formatter = logging.Formatter('%(asctime)s : %(message)s')
        file_handler = logging.FileHandler('logs.log', mode='w')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Setting the level of logger to DEBUG
        logger.setLevel(logging.DEBUG)
        cls.obj = SteelEye(logger)

    def test_load_data(self):
        expected_output = '\n0\n0\n\n*\ntrue\n0\npublication_date:' \
                          '[2021-01-17T00:00:00Z TO 2021-01-19T23:59:59Z]' \
                          '\n100\nxml\n\n'
        bs_data = self.obj.load_data()
        self.assertEqual(bs_data.find('lst').text, expected_output)

    def test_find_zip_link(self):
        data = self.obj.load_data()
        result = self.obj.find_zip_link(data)
        expected_output = 'http://firds.esma.europa.eu/firds/' \
                          'DLTINS_20210117_01of01.zip'
        # print(result)
        self.assertEqual(result, expected_output)

    def test_extractZip(self):
        link = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
        expected_output = ['DLTINS_20210117_01of01.xml']
        self.assertEqual(self.obj.extract_zip(link), expected_output)

    def test_read_xml_doc(self):
        expected_output = 'Kreditanst.f.Wiederaufbau     Anl.v.2014 (2021)'
        link = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
        file_name = self.obj.extract_zip(link)[0]
        bs_content = self.obj.read_xml_doc(file_name)
        self.assertEqual(bs_content.find('FullNm').text, expected_output)

    def test_bs_to_csv(self):
        link = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
        file_name = self.obj.extract_zip(link)[0]
        bs_content = self.obj.read_xml_doc(file_name)
        self.obj.bs_to_csv(bs_content)
        path = pl.Path('Output.csv')
        self.assertTrue(path.is_file())
        self.assertTrue(path.parent.is_dir())


if __name__ == '__main__':
    unittest.main()
