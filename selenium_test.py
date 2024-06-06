import os
import datetime
import logging
import platform
import time
import re

import SnimTestSuite

from pyvirtualdisplay import Display
from selenium import webdriver


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s')


class SnimSeleniumManager(object):

    def __init__(self, baseurl):
        self.logger = logging.getLogger()
        self.log_path = os.getcwd()
        if "\\" in self.log_path:
            self.path = self.log_path.replace("\\", "/")
        else:
            self.path = os.getcwd()


        if platform.system() == "Linux":
            self.display = Display(visible=0, size=(1024, 780))
            self.display.start()

        service_log_path = "chromedriver.log"
        service_args = ['--verbose']
        prefs = {"download.default_directory": self.path,
                 "download.prompt_for_download": False,
                 "profile.default_content_setting_values.automatic_downloads": 1}

        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--disable-gpu --ignore-certificate-errors")

        if platform.system() == "Linux":
            options.add_argument("--headless --disable-gpu --ignore-certificate-errors")

        #options.binary_location = '/usr/bin/google-chrome-unstable'
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-setuid-sandbox")

        #'/usr/local/bin/chromedriver',
        self.driver = webdriver.Chrome(chrome_options=options, service_args=service_args,
                                       service_log_path=service_log_path)
        self.logger.info("new SnimSeleniumManager instance created...")
        self.driver.implicitly_wait(10)
        self._start_time = time.time()
        # Read base url from environment_Settings file
        baseurl = baseurl + '/sia/'
        self.driver.get(baseurl)

    def __del__(self):

        self.logger.debug("cleaning up SnimSeleniumManager instance...")
        self.driver.close()
        if platform.system() == "Linux":
            self.display.stop()
        run_time = time.time() - self._start_time
        self.logger.info("SnimSeleniumManager ran for {t}".format(t=run_time))

    def test_search_result(self, asset_name, asset_type, expected_count, testcase_id):
        self.driver.find_element_by_xpath("//*[@id='kitID']").clear()
        names = re.split(r'\s|,', asset_name)
        self.driver.find_element_by_xpath("//*[@id='kitID']").send_keys(asset_name)
        selection_path = "//*[@id='kitType']/option[text()='{t}']".format(t=asset_type)
        self.driver.find_element_by_xpath(selection_path).click()
        if len(names) > 5 :
            time.sleep(100)
        else:
            time.sleep(30)
        self.driver.find_element_by_xpath("//*[@id=\"btnRun\"]").click()

        if int(expected_count) > 30000:
            time.sleep(60)
        else:
            time.sleep(30)
        # Split asset name by space and ","
        asset_type = re.split(r'\s', asset_type)
        first_column_value = self.driver.find_element_by_xpath("//*[@id='sia_resultsarea']/tbody/tr[1]/td[1]").text
        row_count = len(self.driver.find_elements_by_xpath("//*[@id='sia_resultsarea']/tbody"))
        actual_text = self.driver.find_element_by_id('sia_resultsarea_info').text
        if row_count > 0 and first_column_value == asset_type[0] + "/" + names[0]:
            self.logger.info("TestCase : %s" %(testcase_id + "---" + actual_text + "---" + expected_count + "---" + "Search found"))
            return "passed"

    def test_search_result_OLT(self, asset_name, asset_type, expected_count, testcase_id, check_box_value):
        self.driver.find_element_by_xpath("//*[@id='kitID']").clear()
        self.driver.find_element_by_xpath("//*[@id='kitID']").send_keys(asset_name)
        dummy_path = "//*[@id='kitType']/option[text()='{t}']".format(t="AAS")
        self.driver.find_element_by_xpath(dummy_path).click()
        selection_path = "//*[@id='kitType']/option[text()='{t}']".format(t=asset_type)
        self.driver.find_element_by_xpath(selection_path).click()
        time.sleep(30)
        if check_box_value == "y":
            self.driver.find_element_by_xpath("//*[@id=\"extraOptionsYes\"]").click()
            self.driver.find_element_by_xpath("//*[@id=\"btnDisplayInfo\"]").click()
            time.sleep(60)
            self.driver.find_element_by_xpath("//*[@id=\"extraOptions_wrapper\"]/div[1]/a[1]/span").click()
            self.logger.info("TestCase : %s" %(testcase_id + "---" + asset_type + "---" + expected_count + "---" + "Pass"))
            return "passed"
        else:
            time.sleep(30)
            self.driver.find_element_by_xpath("//*[@id=\"extraOptionsNo\"]").click()
            self.logger.info("TestCase : %s" %(testcase_id + "---" + asset_type + "---" + expected_count + "---" + "Pass"))
            return "passed"

    def test_error_text(self, asset_name, asset_type, expected_error, testcase_id):
        self.driver.find_element_by_xpath("//*[@id='kitID']").clear()
        self.driver.find_element_by_xpath("//*[@id='kitID']").send_keys(asset_name)
        selection_path = "//*[@id='kitType']/option[text()='{t}']".format(t=asset_type)
        time.sleep(30)
        self.driver.find_element_by_xpath(selection_path).click()
        time.sleep(40)
        self.driver.find_element_by_xpath("//*[@id=\"btnRun\"]").click()
        time.sleep(40)
        actual_error = self.driver.find_element_by_xpath("//*[@id='sia_errorText']/ul/li").text
        print actual_error
        if not re.search("INTERNAL SERVER ERROR", actual_error):
            print "test"
            if re.search(expected_error, actual_error):
                self.logger.info("TestCase : %s" %(testcase_id + "---" + actual_error + "---" + expected_error + "---" + "Error Text"))
                return "passed"

    def test_warning_text(self, asset_name, asset_type, expected_warning, testcase_id):
        self.driver.find_element_by_xpath("//*[@id='kitID']").clear()
        self.driver.find_element_by_xpath("//*[@id='kitID']").send_keys(asset_name)
        selection_path = "//*[@id='kitType']/option[text()='{t}']".format(t=asset_type)
        self.driver.find_element_by_xpath(selection_path).click()
        time.sleep(30)
        self.driver.find_element_by_xpath("//*[@id=\"btnRun\"]").click()
        time.sleep(40)
        actual_warning = self.driver.find_element_by_xpath("//*[@id='sia_warningText']/ul/li").text
        time.sleep(40)
        if re.search(expected_warning , actual_warning):
            self.logger.info("TestCase : %s" %(testcase_id + "---" + actual_warning + "---" + expected_warning + "---" + "Warning Text"))
            return "passed"

    def test_export_to_csv(self, asset_name, asset_type, testcase_id):
        self.driver.find_element_by_xpath("//*[@id='kitID']").clear()
        self.driver.find_element_by_xpath("//*[@id='kitID']").send_keys(asset_name)
        selection_path = "//*[@id='kitType']/option[text()='{t}']".format(t=asset_type)
        self.driver.find_element_by_xpath(selection_path).click()
        time.sleep(30)
        self.driver.find_element_by_xpath("//*[@id=\"btnRun\"]").click()
        time.sleep(30)

        self.driver.find_element_by_xpath("//*[@id=\"sia_resultsarea_wrapper\"]/div[5]/a/span").click()
        time.sleep(60)
        filename = max([self.path+ "/" + f for f in os.listdir(self.path)], key=os.path.getctime)
        print("Reading the csv","/", filename)
        f = open(filename, "r")
        if(f.mode=='r'):
            data=f.read(40)
            if(data=='"Access Seeker ID","Impacted Service ID"'):
                print("found", data)
            else:
                print("Error while comparing: expected: \"Access Seeker ID\"\,\"Impacted Service ID\" ", )
                self.logger.info("EXPECTING: \"Access Seeker ID\"\,\"Impacted Service ID\"  ACTUAL: {a}.".format(a=data))
        f.close()
        self.logger.info("TestCase : %s" %(testcase_id + "------" + asset_type + "-----" + asset_name + "----" + "Export to CSV"))
        return "passed"
