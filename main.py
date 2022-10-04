""" importing different modules and packages which are required """
import os
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup


class SteelEye:
    """class SteelEye"""
    def __init__(self, custom_logger=None):
        if custom_logger:
            self.logger = custom_logger
        else:
            self.logger = logging.getLogger(__name__)
        file_formatter = logging.Formatter('%(asctime)s : %(message)s')
        file_handler = logging.FileHandler('logs.log', mode='w')
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

        # Setting the level of self.logger to DEBUG
        self.logger.setLevel(logging.DEBUG)

    def load_data(self):
        """ This function will load data from the xml link as bs4.BeautifulSoup
         file
        :return data: xml file converted into BeautifulSoup file
        """
        try:
            # url of xml file
            url = 'https://registers.esma.europa.eu/solr/' \
                  'esma_registers_firds_files/' \
                  'select?q=*&fq=publication_date:' \
                  '%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=' \
                  'xml&indent=true&start=0&rows=100'
            self.logger.info('Creating HTTP response object from given url.')
            # creating HTTP response object from given url
            response = requests.get(url, timeout=5).content
            data = BeautifulSoup(response, 'xml')
            self.logger.info('Data from the url is successfully fetched.')
            return data
        except Exception as exc:
            self.logger.exception('An exception has occurred in extract_zip '
                                  'function')
            self.logger.error(exc)
            raise exc

    def find_zip_link(self, data):
        """ This function will find the correct link from the data which was
        fetched from load_data() function
        :param  data: xml file converted into BeautifulSoup file
        :type   data: bs4.BeautifulSoup
        :return zip_link: link in form of string
        """
        self.logger.info('Started finding the url of zip file...')
        for tag in data.findAll('doc'):
            for string in tag.findAll('str'):
                if string['name'] == 'file_type':
                    for str_tag in tag.findAll('str'):
                        if str_tag['name'] == 'download_link':
                            zip_link = str_tag.text
                            self.logger.info('Url of the required zip file is '
                                             'found')
                            return zip_link
                    break
        self.logger.info('Zip file url is not found')
        return "Link not found"

    def extract_zip(self, zip_link: str):
        """ This function is first fetching the zip file, then extracting the
        file present in it.
        After that it is returning a list which contains the name of the files
        present in the zip.
        :param  zip_link: link to fetch zip file
        :type   zip_link: str
        :return file_names (list of strings)
        """
        try:
            with urlopen(zip_link) as resp:
                with ZipFile(BytesIO(resp.read())) as myzip:
                    self.logger.info('Extracting all the files now...')
                    files = []
                    for member in myzip.namelist():
                        files.append(member)
                        if not(os.path.exists(member) or os.
                                path.isfile(member)):
                            myzip.extract(member)
                            self.logger.info('Extracting Done!')
                        else:
                            self.logger.error('Extracted file already exists')
                    return files
        except Exception as exc:
            self.logger.exception('An exception has occurred in extract_zip '
                                  'function')
            self.logger.error(exc)
            raise exc

    def read_xml_doc(self, file_name):
        """ This function opens the xml file and encodes it.
        After that it returns a bs4.BeautifulSoup file which contains all the
        encoded data of the xml file.
        :param  file_name: name of the xml document
        :type   file_name: str
        :return bs_content: xml file converted into BeautifulSoup file
        """
        try:
            self.logger.info('Started reading the xml file')
            with open(file_name, "r", encoding='utf-8') as file:
                data = file.read()
                self.logger.info('Successfully read and copied the data from '
                                 'the xml file')
                # Combine the lines in the list into a string
                text = "".join(data)
                bs_content = BeautifulSoup(text, "xml")
                self.logger.info('xml data has been converted into '
                                 'BeautifulSoup file')
                file.close()
            return bs_content
        except Exception as exc:
            self.logger.exception('An exception has occurred in read_xml_doc'
                                  ' function')
            self.logger.error(exc)
            raise exc

    def bs_to_csv(self, bs_content):
        """ This function will first find the required data in different lists
        from the BeautifulSoup file which was generated from the xml file.
        After that, converting all the data into pandas DataFrame.
        Then, this function will create a Output.csv file in the current
        directory.
        :param  bs_content: xml file converted into BeautifulSoup file
        :type   bs_content: bs4.BeautifulSoup
        """
        try:
            self.logger.info('Fetching the required data from the '
                             'BeautifulSoup file')
            issr = bs_content.findAll('Issr')
            fullnm = bs_content.findAll('FullNm')
            clssfctntp = bs_content.findAll('ClssfctnTp')
            cmmdtyderivind = bs_content.findAll('CmmdtyDerivInd')
            ntnlccy = bs_content.findAll('NtnlCcy')
            attrbts = bs_content.find_all('FinInstrmGnlAttrbts')
            id_list = []
            for i in attrbts:
                id_list.append(i.find('Id'))
            data = []
            for i, x_id in enumerate(id_list):
                rows = [x_id.text, fullnm[i].text, clssfctntp[i].text,
                        cmmdtyderivind[i].text, ntnlccy[i].text, issr[i].text]
                data.append(rows)
            self.logger.info('Required data successfully fetched')
            dataframe = pd.\
                DataFrame(data, columns=['FinInstrmGnlAttrbts.Id',
                                         'FinInstrmGnlAttrbts.FullNm',
                                         'FinInstrmGnlAttrbts.ClssfctnTp',
                                         'FinInstrmGnlAttrbts.CmmdtyDerivInd',
                                         'FinInstrmGnlAttrbts.NtnlCcy',
                                         'Issr'])
            self.logger.info('Creating a csv file and writing the data into '
                             'it.')
            dataframe.to_csv('Output.csv')
            self.logger.info('Required csv file successfully created with all '
                             'the data')
        except Exception as exc:
            self.logger.exception('An exception has occurred in bs_to_csv '
                                  'function')
            self.logger.error(exc)
            raise exc


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    obj = SteelEye(logger)

    # loading data from xml file link as bs4.BeautifulSoup file data
    bs_data = obj.load_data()
    link = obj.find_zip_link(bs_data)
    if link != "Link not found":
        file_names = obj.extract_zip(link)
        content = obj.read_xml_doc(file_names[0])
        obj.bs_to_csv(content)
