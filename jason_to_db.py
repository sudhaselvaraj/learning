import json
import os
import MySQLdb
import yaml
from shutil import copy2, move
import datetime
import sys
import time

from collections import OrderedDict

class Json_Loader(object):


    def __init__(self, input_file, data_base_details, bundles_config_details, config_path_details):
        self.insert_flag = True
        self.input_file_name = input_file
        self.config_path_details = config_path_details
        self.bundles_cfg = bundles_config_details
        self.data_base_details = data_base_details
        self.sale_line_count=1
        self.add_product_count=1

    def insert_data_to_bundles_master_table(self,data_to_insert):
        ##TODO Change to api call
        try:
            host = self.data_base_details['mel_database']['host']
            port = self.data_base_details['mel_database']['port']
            user = self.data_base_details['mel_database']['username']
            passwd = self.data_base_details['mel_database']['password']
            db_name=self.data_base_details['mel_database_bp_robotics_process']['db']
            database_master = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name)
            cursor_master = database_master.cursor()
            table_name= 'bp_robotics_master_tsa_bundles_stg'

            sql_insert = "INSERT INTO " + table_name + " (siebel_order_id,stream, created, firstname, " \
                                                      "lastname, Cust_DOB, siebel_service_address_id,new_bigpond_account_username, new_bigpond_account_username_2, " \
                                                       "new_bigpond_account_username_3,new_bigpond_account_username_4,new_bigpond_account_username_5," \
                                                       "new_bigpond_account_pwd ,preferred_delivery_date, telstra_air,directory_listing,id_type,lic_dateofexpiry,lic_num, contact_email,cust_preferred_contact_method,cis_option, priority_assistance,billing_account_number, load_datetime, load_filename,connection_date1,connection_date2,connection_date3,hardware_repayment_option,installation_type,router_name,new_service_number_indicator)" " VALUES " \
                                                       "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(data_to_insert['siebel_order_id'],
                                                    data_to_insert['stream'], data_to_insert['created'], data_to_insert['firstname'], data_to_insert['lastname'],data_to_insert['dateOfBirth'], data_to_insert['addressId'],data_to_insert['username1'],data_to_insert['username2'],
                                                    data_to_insert['username3'],data_to_insert['username4'],data_to_insert['username5'],data_to_insert['password'],data_to_insert['deliveryDate'],data_to_insert['telstra_air'],data_to_insert['directoryListing'],data_to_insert['primaryIDType'],data_to_insert['primaryIDExpiryDate'],data_to_insert['primaryIDNumber'],
                                                    data_to_insert['emailAddress'],data_to_insert['PreferredContactMethod'],data_to_insert['cisOptIn'],data_to_insert['priority_assistance'],data_to_insert['billingAccount'],data_to_insert['load_datetime'], data_to_insert['connectionDate1'],data_to_insert['connectionDate2'],data_to_insert['connectionDate3'],
                                                    data_to_insert['hardware_repayment_option'],data_to_insert['installationType'],data_to_insert['routerName'],data_to_insert['newServiceNumberIndicator'])
            print(sql_insert)
            # cursor_master.execute(sql_insert)
            # database_master.commit()

        except MySQLdb.Error as e:
            # exceptionId = 15 : Undetermined error loading JSON
            #self.handle_exception(input_file, str(e), '15')
            raise

    def get_index(self, data, key):
        for i,product_items in enumerate(data):
            product_items = dict(product_items)
            for k, v in product_items.items():
                if isinstance(v, OrderedDict):
                    for found in self.get_index(v, key):
                        yield found
                if k == key:
                    yield int(i),v

    def parse_salelineproduct(self, data,  data_to_insert,product_string):
        for i, product in enumerate(data):
            product_dict = dict(product)
            for field_name, field_values in product_dict.items():
                if len(product_dict.items()) > 1:
                    if field_values is not None:
                        if  'Telstra Air - Opt' in field_values:
                            data_to_insert['telstra_air'] = field_values
                        elif 'Medical Priority Assistance - Opt' in field_values:
                            data_to_insert['priority_assistance'] = field_values
                        elif 'HRO' in field_values:
                            data_to_insert['hardware_repayment_option'] = field_values
                        else:
                            data_to_insert[field_name] = field_values
                    else:
                        data_to_insert[field_name] = field_values
                else:
                    if 'Telstra Air - Opt' in field_values:
                        data_to_insert['telstra_air'] = field_values
                    elif 'Medical Priority Assistance - Opt' in field_values:
                        data_to_insert['priority_assistance'] = field_values
                    elif 'HRO' in field_values:
                        data_to_insert['hardware_repayment_option'] = field_values
                    else:
                        if field_name == 'productName':
                            # Resolve multiple key found issue#productname
                            new_field_name = "additional_product_" +  str(i)
                        data_to_insert[new_field_name] = field_values

    def recursive_ordered_dict_to_dict(self, ordered_dict, data_to_insert):

        for key, value in ordered_dict.items():
            if isinstance(value, OrderedDict):
                self.recursive_ordered_dict_to_dict(value,data_to_insert)
            elif isinstance(value, list):
                for item in value:
                    self.recursive_ordered_dict_to_dict(item,data_to_insert)
            elif isinstance(value, dict):
                for k,v in value.items():
                    self.recursive_ordered_dict_to_dict(v,data_to_insert)
            else:
                if value:
                    if 'Telstra Air - Opt' in value:
                        data_to_insert['telstra_air'] = value
                    elif 'Medical Priority Assistance - Opt' in value:
                        data_to_insert['priority_assistance'] = value
                    elif 'HRO' in value:
                        data_to_insert['hardware_repayment_option'] = value
                    elif 'saleLineId' in key:
                        # Resolve multiple key found issue#productname
                        ind = str(self.sale_line_count)
                        new_field_name = "saleLineId_"+ind
                        data_to_insert[new_field_name] = value
                        self.sale_line_count += 1
                    elif 'productName' in key:
                        index_product = str(self.add_product_count)
                       # Resolve multiple key found issue#productname
                        new_field_name = "additional_product_" +  index_product
                        data_to_insert[new_field_name] = value
                        self.add_product_count += 1
                    else:
                        data_to_insert[key] = value
                else:
                    if 'saleLineId' in key:
                         # Resolve multiple key found issue#productname
                        index_salelineid = str(self.sale_line_count)
                        new_field_name = "saleLineId_" + index_salelineid
                        data_to_insert[new_field_name] = value
                        self.sale_line_count  += 1
                    elif 'productName' in key:
                        index_product = str(self.add_product_count)
                        # Resolve multiple key found issue#productname
                        new_field_name = "additional_product_" + index_product
                        data_to_insert[new_field_name] = value
                        self.add_product_count += 1
                    else:
                        data_to_insert[key] = value
        return data_to_insert

    def get_data_ready_load(self,data, file_set):

        data_to_insert ={}
        try:
            data_to_insert['stream'] = data['serviceRequest']['campaignTypeName']
            data_to_insert['created'] = data['serviceRequest']['dateSigned']
            data_to_insert['title'] = data['serviceRequest']['customerContact']['title']
            data_to_insert['firstname'] = data['serviceRequest']['customerContact']['firstName']
            data_to_insert['lastname'] = data['serviceRequest']['customerContact']['lastName']
            data_to_insert['dateOfBirth'] =  data['serviceRequest']['customerContact']['dateOfBirth']
            data_to_insert['cisOptIn'] =  data['serviceRequest']['customerContact']['cisOptIn']
            data_to_insert['emailAddress'] = data['serviceRequest']['customerContact']['contactDetails']['emailAddress']
            data_to_insert['Contact_Ph'] = data['serviceRequest']['customerContact']['contactDetails']['primaryPhoneNumber']
            data_to_insert['homePhoneNumber'] = data['serviceRequest']['customerContact']['contactDetails']['homePhoneNumber']
            data_to_insert['mobilePhoneNumber'] = data['serviceRequest']['customerContact']['contactDetails']['mobilePhoneNumber']
            data_to_insert['workPhoneNumber'] = data['serviceRequest']['customerContact']['contactDetails']['workPhoneNumber']
            data_to_insert['faxNumber'] = data['serviceRequest']['customerContact']['contactDetails']['faxNumber']
            data_to_insert['alternatePhoneNumber'] = data['serviceRequest']['customerContact']['contactDetails']['alternatePhoneNumber']
            data_to_insert['PreferredContactMethod'] = data['serviceRequest']['customerContact']['contactDetails']['PreferredContactMethod']
            data_to_insert['addressId'] = data['serviceRequest']['addressDetails']['addresses'][0]['addressId']
            data_to_insert['addr_line1'] = data['serviceRequest']['addressDetails']['addresses'][0]['addressLine1']
            data_to_insert['addr_line2'] = data['serviceRequest']['addressDetails']['addresses'][0]['addressLine2']
            data_to_insert['addr_line3'] = data['serviceRequest']['addressDetails']['addresses'][0]['addressLine3']
            data_to_insert['addr_suburb'] = data['serviceRequest']['addressDetails']['addresses'][0]['suburb']
            data_to_insert['addr_state'] = data['serviceRequest']['addressDetails']['addresses'][0]['state']
            data_to_insert['postCode'] = data['serviceRequest']['addressDetails']['addresses'][0]['postCode']
            data_to_insert['country'] = data['serviceRequest']['addressDetails']['addresses'][0]['country']
            data_to_insert['primaryIDType'] = data['serviceRequest']['identification']['primaryIDType']
            data_to_insert['primaryIDNumber'] =  data['serviceRequest']['identification']['primaryIDType']
            data_to_insert['primaryIDExpiryDate'] = data['serviceRequest']['identification']['primaryIDExpiryDate']
            data_to_insert['siebel_order_id'] = data['serviceRequest']['saleLine'][0]['orderReferenceNumber']
            data_to_insert['deliveryAddressId'] = data['serviceRequest']['saleLine'][0]['deliveryAddressId']
            data_to_insert['deliveryDate'] = data['serviceRequest']['saleLine'][0]['deliveryDate']
            data_to_insert['billingAccount'] = data['serviceRequest']['saleLine'][0]['billingAccount']
            data_to_insert['referenceNumber'] = data['serviceRequest']['saleLine'][0]['referenceNumber']
            data_to_insert['newServiceNumberIndicator'] = data['serviceRequest']['saleLine'][0]['newServiceNumberIndicator']
            data_to_insert['load_datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
            data_to_insert['load_filename'] = file_set

            for prod_ind in (self.get_index(data['serviceRequest']['saleLine'], 'saleLineProductPayTV')):
                self.parse_salelineproduct(data['serviceRequest']['saleLine'][prod_ind[0]]['saleLineProductPayTV'], data_to_insert,"paytv")
            for prod_ind in (self.get_index(data['serviceRequest']['saleLine'], 'saleLineProductBroadband')):
                self.parse_salelineproduct(data['serviceRequest']['saleLine'][prod_ind[0]]['saleLineProductBroadband'], data_to_insert,"broadband")
            for prod_ind in (self.get_index(data['serviceRequest']['saleLine'], 'saleLineProductVoice')):
                self.parse_salelineproduct(data['serviceRequest']['saleLine'][prod_ind[0]]['saleLineProductVoice'],data_to_insert,"voice")
            self.insert_data_to_bundles_master_table(data_to_insert)
        except Exception as e:
            self.handle_exception(input_file, str(e), '13')
            raise

    def handle_exception(self,input_file, exception, exceptionId):
        input_file = os.path.join(self.config_path_details['sftp_archive']['arc_path_1_New_Archive'],input_file)
        error = dict()
        error['load_filename'] = input_file
        error['processed_datetime'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        error['exception'] = exception
        error['source'] = 'TSA_Bundles'
        table_name = 'loadException'
        # insert_to_db(database, table_name, cursor, error, input_file)
        data = json.load(open(input_file), object_pairs_hook=OrderedDict)
        data['serviceRequest']['automation']['status'] = 'Exception'
        data['serviceRequest']['automation']['dateStamp'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        data['serviceRequest']['automation']['exceptionId'] = exceptionId
        data['serviceRequest']['automation']['exceptionReason'] = exception
        with open(input_file, 'w') as updated_file:
            updated_file.seek(0)
            json.dump(data, updated_file, indent=4)
            updated_file.truncate()

        # copying the file to the archive directory, and then moving it to the Exceptions directory
        # TODO: handle the file name duplications
        # try:
        #copy2(input_file, '/mnt/bpdata/tsa_data/archive/4_Exception_Archive')
        copy2(input_file, self.config_path_details['sftp_archive']['arc_path_4_Exception_Archive'])
        # move(input_file, '/home/dme3/Documents/json/1_New_Archive/processed')
        file_name = os.path.basename(input_file)
        # input_file = os.path.join('/home/wall-e/Desktop/source/1_New', file_name)
        #copy2(input_file, '/mnt/bpdata/tsa_data/source/4_Exception')
        copy2(input_file, self.config_path_details['sftp_source']['src_path_4_Exception'])
        #move(input_file, '/mnt/bpdata/tsa_data/archive/1_New_Archive/processed')
        ###move_file1 = os.path.join('mv /mnt/bpdata/tsa_data/archive/1_New_Archive/', file_name)+' /mnt/bpdata/tsa_data/archive/1_New_Archive/processed/.'
        move_file = os.path.join('mv '+ self.config_path_details['sftp_archive']['arc_path_1_New_Archive'],
                                 file_name) + ' '+self.config_path_details['sftp_archive']['arc_path_1_archive_processed']
        os.system(move_file)
        print("about to remove file")
        ###input_file1 = os.path.join('rm /mnt/bpdata/tsa_data/source/1_New', file_name)
        input_file = os.path.join('rm '+self.config_path_details['sftp_source']['src_path_1_New'], file_name)
        os.system(input_file)
        # except e:
        #     new_file = input_file + '_' + str(datetime.datetime.now())
        #     move(input_file, new_file)


if __name__ == '__main__':
    base_path = "/srv/robot_py/"
    data_base_details = yaml.load(open(base_path + "config/database.yml"))
    bundles_config_details = yaml.load(open(base_path + "config/json_config.yml"))
    config_path_details = yaml.load(open(base_path + "config/config_path.yml"))
    file_path = config_path_details['sftp_source']['src_path_1_New']
    file_set = sys.argv[1]

    file_set = os.path.basename(file_set)
    input_file = os.path.join(file_path, file_set)
    print(file_set)
    if "_Bundles" in file_set:
        input_file = os.path.join(config_path_details['sftp_archive']['arc_path_1_New_Archive'], file_set)
        data = json.load(open(input_file), object_pairs_hook=OrderedDict)
        #validating data
        json_parser_object = Json_Loader(input_file,data_base_details,bundles_config_details,config_path_details)
        data_to_insert = {}
        json_parser_object.recursive_ordered_dict_to_dict(data,data_to_insert)
        #json_parser_object.get_data_ready_load(data,file_set)
