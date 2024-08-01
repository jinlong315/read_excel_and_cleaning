import pandas as pd
import shutil
from Module_Common_Function import epaper_sqlserver_database, epaper_sqlserver_user, epaper_sqlserver_password, \
    epaper_sqlserver_host, MSSQL
from pathlib import Path
import datetime

# define variables to save connection to MS SQL Server
epaper_con = MSSQL(
    server=epaper_sqlserver_host,
    user=epaper_sqlserver_user,
    password=epaper_sqlserver_password,
    database=epaper_sqlserver_database
)

# define variables to save basic information
virtual_printer_dir_parent = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\08_Private Database\99_python_application\Virtual_Printer"
virtual_printer_dict_dir = {
    "LX02_A_Stock": Path.joinpath(Path(virtual_printer_dir_parent), "01_LX02_A_Stock"),
    "VEVW": Path.joinpath(Path(virtual_printer_dir_parent), "02_VEVW"),
    "MB51": Path.joinpath(Path(virtual_printer_dir_parent), "03_MB51"),
    "MB52": Path.joinpath(Path(virtual_printer_dir_parent), "04_MB52"),
    "LX02": Path.joinpath(Path(virtual_printer_dir_parent), "05_LX02"),
    "ZPRL_Value": Path.joinpath(Path(virtual_printer_dir_parent), "06_ZPRL_Value"),
    "ZPRL_Qty": Path.joinpath(Path(virtual_printer_dir_parent), "07_ZPRL_Qty"),
    "ZPCP13": Path.joinpath(Path(virtual_printer_dir_parent), "08_ZPCP13"),
    "PP_FAUF": Path.joinpath(Path(virtual_printer_dir_parent), "09_PP_FAUF"),
    "ZCO_MAT_BEWERT": Path.joinpath(Path(virtual_printer_dir_parent), "10_ZCO_MAT_BEWERT"),
    "MM03": Path.joinpath(Path(virtual_printer_dir_parent), "11_MM03"),
    "EORD_source_list": Path.joinpath(Path(virtual_printer_dir_parent), "12_EORD_Sourcelist"),
    "MB52_spare_parts": Path.joinpath(Path(virtual_printer_dir_parent), "13_MB52_Spareparts"),
    "IW47_order_staff_time": Path.joinpath(Path(virtual_printer_dir_parent), "14_IW47_OrderStaffTime"),
    "MB51_specified_MVT": Path.joinpath(Path(virtual_printer_dir_parent), "15_MB51_specified_MVT"),
    "MM_PUR_PR": Path.joinpath(Path(virtual_printer_dir_parent), "16_MM_PUR_PR"),
    "COOIS_output": Path.joinpath(Path(virtual_printer_dir_parent), "17_COOIS_Output"),
    "COOIS_order_routing": Path.joinpath(Path(virtual_printer_dir_parent), "18_COOIS_order_routing"),
    "SE16_PP_QMELD": Path.joinpath(Path(virtual_printer_dir_parent), "19_SE16_PP_QMELD"),
    "SE16_ZPSOLLMIN": Path.joinpath(Path(virtual_printer_dir_parent), "20_SE16_ZPSOLLMIN"),
    "SE16_EKKN": Path.joinpath(Path(virtual_printer_dir_parent), "21_SE16_EKKN"),
    "SE16_EKBE": Path.joinpath(Path(virtual_printer_dir_parent), "22_SE16_EKBE"),
    "Y_ED1_27000648": Path.joinpath(Path(virtual_printer_dir_parent), "23_Y_ED1_27000648"),
    "COOIS_header": Path.joinpath(Path(virtual_printer_dir_parent), "24_COOIS_Header"),
    "ME5A": Path.joinpath(Path(virtual_printer_dir_parent), "25_ME5A"),
    "EBKN": Path.joinpath(Path(virtual_printer_dir_parent), "26_EBKN"),
    "IW39": Path.joinpath(Path(virtual_printer_dir_parent), "27_IW39"),
    "MM_PUR_PO": Path.joinpath(Path(virtual_printer_dir_parent), "28_MM_PUR_PO"),
    "CSKS": Path.joinpath(Path(virtual_printer_dir_parent), "29_CSKS"),
    "Work_Center": Path.joinpath(Path(virtual_printer_dir_parent), "30_Work_Center"),
    "IH08": Path.joinpath(Path(virtual_printer_dir_parent), "31_IH08"),
    "EKKO": Path.joinpath(Path(virtual_printer_dir_parent), "32_EKKO"),
}


def profit_center():
    """
    Get profit center in Plant 3
    :return: DataFrame
    """

    # define variable to save SQL query
    sql_profit_center = f"""
    SELECT DISTINCT[profit_center],
               [segment_number]
    FROM [dbo].[basicdatamodule$department_extend]
    """

    # read data from SQL Server
    df_profit_center = pd.read_sql(sql=sql_profit_center, con=epaper_con.sqlalchemy_connection())

    return df_profit_center


class VirtualPrinter:

    def __init__(self, dir_pcl):
        """
        Initialization for variable
        :param dir_pcl: absolute directory of raw data
        """

        self.dir_pcl = dir_pcl

    def zpcp13(self):
        """
        Data cleaning for raw data.
        :return: DataFrame
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"MRP_controller": [],
                                 "MRP_group": [],
                                 "material_desc": [],
                                 "material_number": [],
                                 "days_supply": [],
                                 "category": [],
                                 "stock": [],
                                 "stock_p_plant": [],
                                 "backlog": [],
                                 "0": [],
                                 "1": [],
                                 "2": [],
                                 "3": [],
                                 "4": [],
                                 "5": [],
                                 "6": [],
                                 "7": [],
                                 "8": [],
                                 "9": [],
                                 "10": [],
                                 "11": [],
                                 "12": [],
                                 "13": [],
                                 "total": [],
                                 "total_without_stock": [],
                                 "unit": [],
                                 "segment": [],
                                 "production_type": [],
                                 "production_type_desc": [],
                                 "MRP_lot_size": [],
                                 "min_lot_size": [],
                                 "plan_delivery_time": [],
                                 "safety_stock": [],
                                 "target_stock": [],
                                 "safety_time": [],
                                 "inhouse_production_time": [],
                                 "planning_time_fence": [],
                                 "purchase_group": [],
                                 "supplier": [],
                                 "plant_status": [],
                                 "stochastic_type": [],
                                 "ABC": [],
                                 "XYZ": [],
                                 "production_hierachy": [],
                                 "quality_inspection": [],
                                 "blocked": [],
                                 "configuable_material": [],
                                 "MRP_type": [],
                                 "LS": [],
                                 "target_stock_over": [],
                                 "stock_type": [],
                                 "round_value": [],
                                 "material_type": [],
                                 "material_group": [],
                                 "GR_processing_time": [],
                                 "annual_demand": [],
                                 "budget_year_requirement": [],
                                 "purchase_group_desc": [],
                                 "consumption_current_year_2": [],
                                 "consumption_current_year_1": [],
                                 "consumption_current_year": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:62]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(inplace=True)

            # rename column name
            first_date_string = data[2].split(sep="|")[10]
            first_date_string = first_date_string[-10:]
            date_0 = datetime.datetime.strptime(first_date_string, "%d.%m.%Y")
            list_date = [datetime.datetime.strftime(date_0 + datetime.timedelta(days=i), "%Y-%m-%d") for i in
                         range(0, 14, 1)]
            dict_column_mapping = {str(k): v for k, v in enumerate(list_date)}
            df_merged.rename(columns=dict_column_mapping, inplace=True)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # unpivot DataFrame
            list_fixed_column = []
            for i in df_merged.columns.to_list():
                if i not in dict_column_mapping.values():
                    list_fixed_column.append(i)
            df_merged = df_merged.melt(id_vars=list_fixed_column,
                                       value_vars=list_date,
                                       var_name="date",
                                       value_name="quantity")

            # reset index
            df_merged.reset_index(drop=True, inplace=True)

            return df_merged

    def pp_fauf(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """
        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"oder_number": [],
                                 "order_type": [],
                                 "segment": [],
                                 "MRP_controller": [],
                                 "material_number": [],
                                 "material_desc": [],
                                 "basic_start_date": [],
                                 "basic_finish_date": [],
                                 "actual_finish_date": [],
                                 "order_quantity": [],
                                 "confirmed_quantity": [],
                                 "unit": [],
                                 "printed": [],
                                 "system_status_head": [],
                                 "confirm_enter_by": [],
                                 "confirm_date": [],
                                 "work_center": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:18]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            return df_merged

    def eord_source_list(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"client": [],
                                 "material_number": [],
                                 "plant": [],
                                 "number": [],
                                 "created_date": [],
                                 "created_by": [],
                                 "valid_from": [],
                                 "valid_to": [],
                                 "supplier": [],
                                 "fixed_vendor": [],
                                 "schedule_agreement": [],
                                 "item": [],
                                 "fixed_agreement_item": [],
                                 "procurement_plant": [],
                                 "fixed_issue_plant": [],
                                 "MPN_material": [],
                                 "blocked": [],
                                 "purchase_organization": [],
                                 "document_category": [],
                                 "source_category": [],
                                 "MRP": [],
                                 "order_unit": [],
                                 "logical_system": [],
                                 "special_stock": [],
                                 "central_contract": [],
                                 "central_contract_item": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[2:28]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            return df_merged

    def mb52_spare_parts(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"material_number": [],
                                 "plant": [],
                                 "stock_location": [],
                                 "special_stock_category": [],
                                 "valuation": [],
                                 "special_stock_number": [],
                                 "stock_location_deletion_flag": [],
                                 "batch": [],
                                 "unit": [],
                                 "stock_unrestricted": [],
                                 "stock_segment": [],
                                 "currency": [],
                                 "value_unrestricted": [],
                                 "stock_in_transit": [],
                                 "value_in_transit": [],
                                 "stock_in_quality_inspection": [],
                                 "value_in_quality_inspection": [],
                                 "stock_restricted": [],
                                 "value_restricted": [],
                                 "stock_blocked": [],
                                 "value_blocked": [],
                                 "stock_returned": [],
                                 "value_returned": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:24]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            return df_merged

    def coois_output(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"plant": [],
                                 "order_number": [],
                                 "activity": [],
                                 "operation_quantity": [],
                                 "confirmation_number": [],
                                 "confirmation_counter": [],
                                 "posting_date": [],
                                 "shift": [],
                                 "work_center": [],
                                 "unit": [],
                                 "production_minute": [],
                                 "OK_quantity": [],
                                 "confirmation_minute": [],
                                 "rework_quantity": [],
                                 "scrap_quantity": [],
                                 "confirmation_type": [],
                                 "reversed_flag": [],
                                 "cancelled_confirmation_flag": [],
                                 "created_date": [],
                                 "Germany_time": [],
                                 "entered_by": [],
                                 "material_number": [],
                                 "material_desc": [],
                                 "machine_work_center": [],
                                 "Chinese_time": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:26]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["order_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # convert data type
            df_merged["operation_quantity"] = df_merged["operation_quantity"].str.replace(",", "")
            df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d")
            df_merged["production_minute"] = df_merged["production_minute"].str.replace(",", "")
            df_merged["OK_quantity"] = df_merged["OK_quantity"].str.replace(",", "")
            df_merged["confirmation_minute"] = df_merged["confirmation_minute"].str.replace(",", "")
            df_merged["rework_quantity"] = df_merged["rework_quantity"].str.replace(",", "")
            df_merged["scrap_quantity"] = df_merged["scrap_quantity"].str.replace(",", "")
            df_merged["created_date"] = pd.to_datetime(df_merged["created_date"], format="%Y.%m.%d")

            return df_merged

    def coois_order_routing(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"order_number": [],
                                 "control_key": [],
                                 "activity": [],
                                 "setup_time_machine": [],
                                 "production_time_machine": [],
                                 "additional_time_machine": [],
                                 "number_staff_setup": [],
                                 "number_staff_production": [],
                                 "base_quantity": [],
                                 "work_center": [],
                                 "work_center_desc": [],
                                 "production_quantity": [],
                                 "yield_quantity": [],
                                 "scrap_quantity": [],
                                 "rework_quantity": []
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:-1]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["order_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # convert data type
            df_merged["setup_time_machine"] = df_merged["setup_time_machine"].str.replace(",", "")
            df_merged["production_time_machine"] = df_merged["production_time_machine"].str.replace(",", "")
            df_merged["additional_time_machine"] = df_merged["additional_time_machine"].str.replace(",", "")
            df_merged["number_staff_setup"] = df_merged["number_staff_setup"].str.replace(",", "")
            df_merged["number_staff_production"] = df_merged["number_staff_production"].str.replace(",", "")
            df_merged["base_quantity"] = df_merged["base_quantity"].str.replace(",", "")

            return df_merged

    def se16_pp_qmeld(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"client": [],
                                 "order_number": [],
                                 "sequence": [],
                                 "operation": [],
                                 "confirmation_counter": [],
                                 "posting_date": [],
                                 "work_center": [],
                                 "shift": [],
                                 "scrap_indicator": [],
                                 "quantity": [],
                                 "error_key": [],
                                 "burdening_plant": [],
                                 "burdening_cost_center": [],
                                 "burdening_work_center": [],
                                 "production_quantity": [],
                                 "operation_unit": [],
                                 "material_number": [],
                                 "plant": [],
                                 "cost_center": [],
                                 "work_center_category": [],
                                 "created_date": [],
                                 "created_time": [],
                                 "employee_ID": [],
                                 "down_time": [],
                                 "notification": [],
                                 "confirmation": [],
                                 "confirmation_counter_2": [],
                                 "type_of_message": [],
                                 "check_box": [],
                                 "deletion_flag": [],
                                 "change_document": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[2:33]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["employee_ID"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # convert data type
            df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d", errors="coerce")
            df_merged["quantity"] = df_merged["quantity"].str.replace(",", "")
            df_merged["production_quantity"] = df_merged["production_quantity"].str.replace(",", "")
            df_merged["created_date"] = pd.to_datetime(df_merged["created_date"], format="%Y.%m.%d", errors="coerce")
            df_merged["down_time"] = df_merged["down_time"].str.replace(",", "")

            return df_merged

    def se16_zpsollmin(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"client": [],
                                 "plant": [],
                                 "cost_center": [],
                                 "work_center": [],
                                 "date": [],
                                 "shift": [],
                                 "target_minutes_shift_model": [],
                                 "target_minutes": [],
                                 "posting_minutes": [],
                                 "posting_date": [],
                                 "posting_time": [],
                                 "varient_shift": [],
                                 "KZ_Bedarfsma": [],
                                 "gepl_Einsatzmin": [],
                                 "KZ_U_Schicht": [],
                                 "pause_Enisatzzeit": [],
                                 "BUMIN_masch_stillst": [],
                                 "ist_einsatzmin": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[2:20]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["cost_center"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # convert data type
            df_merged["date"] = pd.to_datetime(df_merged["date"], format="%Y.%m.%d", errors="coerce")
            df_merged["target_minutes_shift_model"] = df_merged["target_minutes_shift_model"].str.replace(",", "")
            df_merged["target_minutes"] = df_merged["target_minutes"].str.replace(",", "")
            df_merged["posting_minutes"] = df_merged["posting_minutes"].str.replace(",", "")
            df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d", errors="coerce")

            return df_merged

    def se16_ekkn(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"clint": [],
                                 "PO": [],
                                 "PO_item": [],
                                 "account_assignment_squence": [],
                                 "deletion_flag": [],
                                 "created_on": [],
                                 "change_flag": [],
                                 "quantity": [],
                                 "percent": [],
                                 "net_value": [],
                                 "GL_account": [],
                                 "business_area": [],
                                 "cost_center": [],
                                 "not_in_use": [],
                                 "SD_document": [],
                                 "item": [],
                                 "schedule_line_number": [],
                                 "gross_requirement_flag": [],
                                 "asset": [],
                                 "sub_number": [],
                                 "order": [],
                                 "recipient": [],
                                 "unloading_point": [],
                                 "controlling_area": [],
                                 "post_to_cost_center": [],
                                 "post_to_order": [],
                                 "post_to_project": [],
                                 "final_invoice": [],
                                 "cost_object": [],
                                 "segment_number": [],
                                 "profit_center": [],
                                 "WBS_element": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[2:34]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["PO"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning for column
            df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d", errors="coerce")
            df_merged["net_value"] = df_merged["net_value"].apply(lambda x: str(x).replace(",", ""))

            return df_merged

    def y_ed1_27000648(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        # define variables to save key string
        str_title = "Plant Cost Report (actual)"
        list_profit_center = ["8101-S20", "8101-S22", "8101-S36"]
        dir_garbage = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\08_Private Database\99_python_application\Virtual_Printer\23_Y_ED1_27000648\Garbage"

        # get basic information of file
        file_path = Path(self.dir_pcl).stat()
        file_name = Path(self.dir_pcl).stem
        creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
        last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # read data with Python
            data = f.readlines()

            # if content in the file can meet following conditions
            df_merged = pd.DataFrame()
            if (str_title in data[1]) and (any(char in data[4] for char in list_profit_center)):

                # get date
                year_month_day = None
                year_month = None
                for i in data[1].split(sep=" "):
                    try:

                        # get original date
                        original_date = datetime.datetime.strptime(i, "%d.%m.%Y")

                        # get previous date of original date
                        year_month_day = original_date - datetime.timedelta(days=1)
                        year_month = datetime.datetime.strftime(year_month_day, "%Y%m")
                    except:
                        pass

                # get profit center
                profit_center = None
                for j in data[4].split(sep=" "):
                    if j in list_profit_center:
                        profit_center = j

                # define dictionary to save raw data
                dict_cleaned_data = {"cost_category": [],
                                     "plan_cost_in_CNY": [],
                                     "actual_cost_in_CNY": [],
                                     "deviation_in_CNY": [],
                                     "deviation_percentage": [],
                                     "plan_YTD_cost_in_CNY": [],
                                     "actual_YTD_cost_in_CNY": [],
                                     "YTD_deviation_in_CNY": [],
                                     "YTD_deviation_percentage": [],
                                     }

                for row in data:

                    if row.count("|") >= 10:

                        # split string
                        value = row.split(sep="|")[1:10]

                        # remove white space in string
                        value = [i.strip() for i in value]

                        # append data into dictionary
                        for t in enumerate(dict_cleaned_data.keys()):
                            column_position = t[0]
                            column_name = t[1]

                            dict_cleaned_data[column_name].append(value[column_position])

                # create DataFrame
                row_count = len(dict_cleaned_data["cost_category"])

                df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
                df_file = pd.DataFrame(
                    [[file_name, creation_time, last_modified_time, year_month, year_month_day, profit_center]],
                    index=[i for i in range(0, row_count, 1)],
                    columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day",
                             "profit_center"])

                # concatenate DataFrame
                df_merged = pd.concat([df_file, df_data], axis=1)

                # reset index
                df_merged.drop_duplicates(inplace=True)
                df_merged.dropna(subset=["cost_category"], how="any", axis=0, inplace=True)
                df_merged.reset_index(drop=True, inplace=True)
                df_merged.drop(index=[0], inplace=True)
                df_merged.reset_index(drop=True, inplace=True)

                # data cleaning
                df_merged["cost_category"] = df_merged["cost_category"].str.replace("*", "").str.strip()
                df_merged["plan_cost_in_CNY"] = df_merged["plan_cost_in_CNY"].str.replace(",", "").str.strip()
                df_merged["actual_cost_in_CNY"] = df_merged["actual_cost_in_CNY"].str.replace(",", "").str.strip()
                df_merged["deviation_in_CNY"] = df_merged["deviation_in_CNY"].str.replace(",", "").str.strip()
                df_merged["plan_YTD_cost_in_CNY"] = df_merged["plan_YTD_cost_in_CNY"].str.replace(",", "").str.strip()
                df_merged["actual_YTD_cost_in_CNY"] = df_merged["actual_YTD_cost_in_CNY"].str.replace(",",
                                                                                                      "").str.strip()
                df_merged["YTD_deviation_in_CNY"] = df_merged["YTD_deviation_in_CNY"].str.replace(",", "").str.strip()

                # drop blank rows
                df_merged = df_merged[df_merged["cost_category"] != ""]

                # move file to "Done"
                f.close()
                dir_done = Path(virtual_printer_dict_dir.get("Y_ED1_27000648")).joinpath("Done")
                shutil.move(src=self.dir_pcl, dst=Path(dir_done).joinpath(Path(self.dir_pcl).name))

            # move file to "Garbage"
            else:
                # close file
                f.close()

                # move file
                dir_dst = Path.joinpath(Path(dir_garbage), Path(self.dir_pcl).name)
                shutil.move(src=self.dir_pcl, dst=dir_dst)

            return df_merged

    def zco_mat_bewert(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"plant": [],
                                 "segment": [],
                                 "material_number": [],
                                 "material_desc": [],
                                 "price_control": [],
                                 "valuation_class": [],
                                 "special_procurement": [],
                                 "price_unit": [],
                                 "GPC": [],
                                 "base_unit_of_measure": [],
                                 "moving_price": [],
                                 "standard_price": [],
                                 "GPC_date": [],
                                 "planned_price": [],
                                 "planned_price_date": []
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:16]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["GPC_date"] = pd.to_datetime(df_merged["GPC_date"], format="%d.%m.%Y", errors="coerce")
            df_merged["price_unit"] = df_merged["price_unit"].str.replace(",", "")
            df_merged["GPC"] = df_merged["GPC"].str.replace(",", "")
            df_merged["moving_price"] = df_merged["moving_price"].str.replace(",", "")
            df_merged["standard_price"] = df_merged["standard_price"].str.replace(",", "")
            df_merged["planned_price"] = df_merged["planned_price"].str.replace(",", "")
            df_merged["planned_price_date"] = pd.to_datetime(df_merged["planned_price_date"], format="%d.%m.%Y",
                                                             errors="coerce")

            return df_merged

    def mb52(self):
        """
        :return: DataFrame with cleaned data
        """

        # open file
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"material_number": [],
                                 "material_desc": [],
                                 "material_type": [],
                                 "batch": [],
                                 "S_loc": [],
                                 "unrestricted": [],
                                 "value_unrestricted": [],
                                 "blocked_quantity": [],
                                 "blocked_value": [],
                                 "S": [],
                                 "special_block_number": []
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:12]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged = df_merged.drop(df_merged[df_merged["material_number"] == "*"].index)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            return df_merged

    def mb51(self):
        """
        :return: DataFrame with cleaned data
        """

        # open file
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"material_number": [],
                                 "material_desc": [],
                                 "plant": [],
                                 "name": [],
                                 "stock_location": [],
                                 "movement_type_desc": [],
                                 "movement_type": [],
                                 "supplier": [],
                                 "PO": [],
                                 "material_document": [],
                                 "batch": [],
                                 "posting_date": [],
                                 "quantity": [],
                                 "amount_LC": [],
                                 "user": [],
                                 "document_header_desc": [],
                                 "reference": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:18]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged = df_merged.drop(df_merged[df_merged["material_number"] == "*"].index)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            return df_merged

    def lx02(self):
        """
        :return: DataFrame with cleaned data
        """

        # open file
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # warehouse number
            warehouse_number = data[2].strip()[-3:]

            # define dictionary to save raw data
            dict_cleaned_data = {"material_number": [],
                                 "plant": [],
                                 "S_loc": [],
                                 "S": [],
                                 "batch": [],
                                 "S_1": [],
                                 "special_block_number": [],
                                 "material_desc": [],
                                 "storage_unit": [],
                                 "type": [],
                                 "storage_bin": [],
                                 "available_stock": [],
                                 "pick_quantity": [],
                                 "stock_for_put_away": [],
                                 "unit": [],
                                 "GR_date": [],
                                 "GR_number": [],
                                 "to_it": [],
                                 "to_quant": [],
                                 "to_number": []
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:21]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day, warehouse_number]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day",
                         "warehouse_number"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            return df_merged

    def vevw(self):
        """
        :return: DataFrame with cleaned data
        """

        # open file
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"client": [],
                                 "handle": [],
                                 "object_type": [],
                                 "object_key": [],
                                 "time_stamp": [],
                                 "direction": []
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[2:8]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["object_key"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # reset index
            df_merged.drop_duplicates(inplace=True)
            df_merged = df_merged.drop(df_merged[df_merged["object_key"] == "*"].index)
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            return df_merged

    def zprl_value(self):
        """
        Data cleaning for pcl file from SAP virtual printer
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = (last_modified_time + datetime.timedelta(days=(-1))).date()

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"plant": [],
                                 "material_number": [],
                                 "warehouse_stock": [],
                                 "transfer_plant": [],
                                 "in_delivery": [],
                                 "segment": [],
                                 "sold_to_party": [],
                                 "name": [],
                                 "material_desc": [],
                                 "cont_grp_customer": [],
                                 "customer_material": [],
                                 "backlog_value": [],
                                 "material_type": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:14]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    dict_cleaned_data["plant"].append(value[0])
                    dict_cleaned_data["material_number"].append(value[1])
                    dict_cleaned_data["warehouse_stock"].append(value[11])
                    dict_cleaned_data["transfer_plant"].append(value[3])
                    dict_cleaned_data["in_delivery"].append(value[12])
                    dict_cleaned_data["segment"].append(value[4])
                    dict_cleaned_data["sold_to_party"].append(value[5])
                    dict_cleaned_data["name"].append(value[6])
                    dict_cleaned_data["material_desc"].append(value[7])
                    dict_cleaned_data["cont_grp_customer"].append(value[8])
                    dict_cleaned_data["customer_material"].append(value[9])
                    dict_cleaned_data["backlog_value"].append(value[10])
                    dict_cleaned_data["material_type"].append(value[2])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(inplace=True)

            # drop rows which material number length is less than 9
            length = df_merged["material_number"].astype(str).str.len()
            mask = length >= 9
            df_merged = df_merged[mask]

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # remove "," in numbers
            df_merged["backlog_value"] = df_merged["backlog_value"].str.replace(",", "")

            return df_merged

    def zprl_qty(self):
        """
        Data cleaning for pcl file from SAP virtual printer
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = (last_modified_time + datetime.timedelta(days=(-1))).date()

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"plant": [],
                                 "material_number": [],
                                 "warehouse_stock": [],
                                 "transfer_plant": [],
                                 "in_delivery": [],
                                 "segment": [],
                                 "sold_to_party": [],
                                 "name": [],
                                 "material_desc": [],
                                 "cont_grp_customer": [],
                                 "customer_material": [],
                                 "backlog_quantity": [],
                                 "material_type": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:14]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    dict_cleaned_data["plant"].append(value[0])
                    dict_cleaned_data["material_number"].append(value[1])
                    dict_cleaned_data["warehouse_stock"].append(value[11])
                    dict_cleaned_data["transfer_plant"].append(value[3])
                    dict_cleaned_data["in_delivery"].append(value[12])
                    dict_cleaned_data["segment"].append(value[4])
                    dict_cleaned_data["sold_to_party"].append(value[5])
                    dict_cleaned_data["name"].append(value[6])
                    dict_cleaned_data["material_desc"].append(value[7])
                    dict_cleaned_data["cont_grp_customer"].append(value[8])
                    dict_cleaned_data["customer_material"].append(value[9])
                    dict_cleaned_data["backlog_quantity"].append(value[10])
                    dict_cleaned_data["material_type"].append(value[2])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(inplace=True)

            # drop rows which material number length is less than 9
            length = df_merged["material_number"].astype(str).str.len()
            mask = length >= 9
            df_merged = df_merged[mask]

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # remove "," in numbers
            df_merged["backlog_quantity"] = df_merged["backlog_quantity"].str.replace(",", "")

            return df_merged

    def mm03(self):
        """
        :return: DataFrame with cleaned data for "MM03"
        """

        # read data into pandas
        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
            # get creation time of file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read data with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"plant": [],
                                 "segment": [],
                                 "material_number": [],
                                 "material_desc": [],
                                 "material_type": [],
                                 "assembly_scrap_ratio": [],
                                 "component_scrap_ratio": [],
                                 "prod_stor_location": [],
                                 "storage_loc_for_EP": [],
                                 "procurement_type": [],
                                 "special_procurement": [],
                                 "plant_sp_material_status": [],
                                 "price_control": [],
                                 "valuation_class": [],
                                 "MRP_controller": [],
                                 "prodn_supervisor": [],
                                 "backflush": [],
                                 "availability_check": [],
                                 "tot_repl_lead_time": [],
                                 "consumption_mode": [],
                                 "planning_strategy_group": [],
                                 "fwd_consumption_per": [],
                                 "bwd_consumption_per": [],
                                 "individual_coll": [],
                                 "selection_method": [],
                                 "GR_processing_time": [],
                                 "planned_delivery_time": [],
                                 "planning_time_fence": [],
                                 "in_house_production": [],
                                 "lot_size": [],
                                 "min_lot_size": [],
                                 "max_lot_size": [],
                                 "MRP_group": [],
                                 "MRP_type": [],
                                 "period_indicator": [],
                                 "safety_stock": [],
                                 "safety_time_ind": [],
                                 "safety_time_act_cov": [],
                                 "discontinuation_ind": [],
                                 "effective_out_date": [],
                                 "follow_up_material": [],
                                 "costing_lot_size": [],
                                 "budget_lot_size": [],
                                 "lab_office": [],
                                 "material_group": [],
                                 "planning_calendar": [],
                                 "product_hierarchy": [],
                                 "product_manager": [],
                                 "profit_center": [],
                                 "quota_arr_usage": [],
                                 "reorder_point": [],
                                 "spec_procurem_costing": [],
                                 "budget_flag_MRP": [],
                                 "budget_flag_calculation": [],
                                 "net_weight": [],
                                 "net_weight_unit": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:57]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["material_number"])

            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame(
                [[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                index=[i for i in range(0, row_count, 1)],
                columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(inplace=True)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            return df_merged

    def iw47_order_staff_time(self):
        """
        Data cleaning for pcl files with
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"order_type": [],
                                 "plant": [],
                                 "work_center": [],
                                 "activity_type": [],
                                 "order": [],
                                 "confirmation": [],
                                 "posting_date": [],
                                 "actual_start_date": [],
                                 "actual_start_time": [],
                                 "actual_finish_time": [],
                                 "employee_ID": [],
                                 "employee_name": [],
                                 "actual_work_minutes": [],
                                 "unit": [],
                                 "created_date": [],
                                 "created_by": [],
                                 "reverse": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:18]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["order"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged = df_merged.drop(df_merged[df_merged["order"] == "*"].index)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d")
            df_merged["actual_start_date"] = pd.to_datetime(df_merged["actual_start_date"], format="%Y.%m.%d")
            df_merged["created_date"] = pd.to_datetime(df_merged["created_date"], format="%Y.%m.%d")
            df_merged["actual_work_minutes"] = df_merged["actual_work_minutes"].apply(lambda x: str(x).replace(",", ""))

            return df_merged

    def mb51_specified_mvt(self):
        """
        Data cleaning for pcl files
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"material_number": [],
                                 "material_desc": [],
                                 "plant": [],
                                 "name": [],
                                 "stock_location": [],
                                 "movement_type_desc": [],
                                 "movement_type": [],
                                 "supplier": [],
                                 "PO": [],
                                 "material_document": [],
                                 "batch": [],
                                 "posting_date": [],
                                 "quantity": [],
                                 "unit": [],
                                 "amount_LC": [],
                                 "user": [],
                                 "document_header_desc": [],
                                 "reference": [],
                                 "cost_center": [],
                                 "order": []
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:21]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame\
            row_count = len(dict_cleaned_data["material_number"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged = df_merged.drop(df_merged[df_merged["material_number"] == "*"].index)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d")
            df_merged["quantity"] = df_merged["quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["amount_LC"] = df_merged["amount_LC"].apply(lambda x: str(x).replace(",", ""))

            return df_merged

    def mm_pur_po(self):
        """
        Data cleaning for pcl files
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"plant": [],
                                 "country": [],
                                 "material_group": [],
                                 "material_number": [],
                                 "material_desc": [],
                                 "supplier": [],
                                 "vendor_name": [],
                                 "valid_start": [],
                                 "valid_end": [],
                                 "next_FRC": [],
                                 "PO": [],
                                 "PO_item": [],
                                 "PDT_EKPO": [],
                                 "profile": [],
                                 "firm_zone": [],
                                 "trade_off_zone": [],
                                 "confirmation_control_key": [],
                                 "order": [],
                                 "PO_quantity": [],
                                 "order_price_unit": [],
                                 "order_unit": [],
                                 "price_unit": [],
                                 "net_price": [],
                                 "cost_center": [],
                                 "account_type": [],
                                 "posting_date": [],
                                 "GL_account": [],
                                 "GR_IR_flag": [],
                                 "PO_date": [],
                                 "movement_type": [],
                                 "currency": [],
                                 "item_category": [],
                                 "material_document": [],
                                 "PR": [],
                                 "PR_item": [],
                                 "received_cost_center": [],
                                 "requested_cost_center": [],
                                 "requested_by": [],
                                 "requisitioner": [],
                                 "WBS": [],
                                 "delivery_date": [],
                                 "PO_type": [],
                                 "GR_quantity": [],
                                 "delivery_complete_flag": [],
                                 "order_confirmation": [],
                                 "PO_item_desc": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|", maxsplit=46)[1:47]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["PO"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged = df_merged.drop(df_merged[df_merged["plant"] == "*"].index)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["PO_quantity"] = df_merged["PO_quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["net_price"] = df_merged["net_price"].apply(lambda x: str(x).replace(",", ""))
            df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d", errors="coerce")
            df_merged["PO_date"] = pd.to_datetime(df_merged["PO_date"], format="%Y.%m.%d", errors="coerce")
            df_merged["valid_start"] = pd.to_datetime(df_merged["valid_start"], format="%Y.%m.%d", errors="coerce")
            df_merged["valid_end"] = pd.to_datetime(df_merged["valid_end"], format="%Y.%m.%d", errors="coerce")
            df_merged["delivery_date"] = pd.to_datetime(df_merged["delivery_date"], format="%Y.%m.%d", errors="coerce")
            df_merged["GR_quantity"] = df_merged["GR_quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["price_unit"] = df_merged["price_unit"].apply(lambda x: str(x).replace(",", ""))
            df_merged["PO_item_desc"] = df_merged["PO_item_desc"].apply(lambda x: str(x)[:-1].strip())
            df_merged["PO_item"] = df_merged["PO_item"].apply(lambda x: str(x).rjust(5, "0"))
            df_merged["PR_item"] = df_merged["PR_item"].apply(lambda x: str(x).rjust(5, "0"))

            return df_merged

    def mm_pur_pr(self):
        """
        Data cleaning for pcl files
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"plant": [],
                                 "material_group": [],
                                 "material_number": [],
                                 "material_desc": [],
                                 "vendor_name": [],
                                 "PO": [],
                                 "PO_item": [],
                                 "order": [],
                                 "PR_item_quantity": [],
                                 "unit_1": [],
                                 "unit_2": [],
                                 "price_unit": [],
                                 "cost_center": [],
                                 "account_assignment_category": [],
                                 "GL_account": [],
                                 "PO_date": [],
                                 "currency": [],
                                 "PR": [],
                                 "PR_item": [],
                                 "receive_cost_center": [],
                                 "request_cost_center": [],
                                 "requested_by": [],
                                 "WBS_element": [],
                                 "purchase_document_type": [],
                                 "segment_number": [],
                                 "short_text": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|", maxsplit=26)[1:27]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["plant"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged = df_merged.drop(df_merged[df_merged["plant"] == "*"].index)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["PR_item_quantity"] = df_merged["PR_item_quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["price_unit"] = df_merged["price_unit"].apply(lambda x: str(x).replace(",", ""))
            df_merged["PO_date"] = pd.to_datetime(df_merged["PO_date"], format="%Y.%m.%d", errors="coerce")
            df_merged["short_text"] = df_merged["short_text"].apply(lambda x: str(x)[:-1].strip())
            df_merged["PR_item"] = df_merged["PR_item"].apply(lambda x: str(x).rjust(5, "0"))
            df_merged["PO_item"] = df_merged["PO_item"].apply(lambda x: str(x).rjust(5, "0"))
            df_merged["segment_number"] = df_merged["segment_number"].apply(lambda x: str(x).rjust(6, "0"))

            return df_merged

    def coois_header(self):
        """
        Data cleaning for pcl files
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"order": [],
                                 "order_type": [],
                                 "material_number": [],
                                 "material_desc": [],
                                 "alternative_BOM": [],
                                 "explosion_date": [],
                                 "MRP_controller": [],
                                 "production_supervisor": [],
                                 "target_quantity": [],
                                 "delivery_quantity": [],
                                 "confirmed_quantity": [],
                                 "scrap_quantity": [],
                                 "rework_quantity": [],
                                 "unit": [],
                                 "basic_start_date": [],
                                 "basic_finish_date": [],
                                 "actual_finish_date": [],
                                 "system_status": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:19]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["order"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged.dropna(subset=["order"], axis=0, inplace=True)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["explosion_date"] = pd.to_datetime(df_merged["explosion_date"], format="%Y.%m.%d",
                                                         errors="coerce")
            df_merged["target_quantity"] = df_merged["target_quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["delivery_quantity"] = df_merged["delivery_quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["confirmed_quantity"] = df_merged["confirmed_quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["scrap_quantity"] = df_merged["scrap_quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["rework_quantity"] = df_merged["rework_quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["basic_start_date"] = pd.to_datetime(df_merged["basic_start_date"], format="%Y.%m.%d",
                                                           errors="coerce")
            df_merged["basic_finish_date"] = pd.to_datetime(df_merged["basic_finish_date"], format="%Y.%m.%d",
                                                            errors="coerce")

            return df_merged

    def ebkn(self):
        """
        Data cleaning for pcl files
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"client": [],
                                 "PR": [],
                                 "PR_item": [],
                                 "account_assignment": [],
                                 "deletion_flag": [],
                                 "created_date": [],
                                 "created_by": [],
                                 "requested_quantity": [],
                                 "percentage": [],
                                 "GL_account": [],
                                 "business_area": [],
                                 "cost_center": [],
                                 "not_in_use": [],
                                 "SD_document": [],
                                 "SD_document_item": [],
                                 "schedule_line_number": [],
                                 "asset": [],
                                 "sub_number": [],
                                 "order": [],
                                 "recipient": [],
                                 "unloading_point": [],
                                 "controlling_area": [],
                                 "posting_to_cost_center": [],
                                 "posting_to_order": [],
                                 "posting_to_project": [],
                                 "cost_object": [],
                                 "profit_segment": [],
                                 "profit_center": [],
                                 "WBS_element": [],
                                 "network": [],
                                 "routing_number_for_operations_1": [],
                                 "real_estate_key": [],
                                 "counter_1": [],
                                 "partner": [],
                                 "commitment_item": [],
                                 "recovery_flag": [],
                                 "request_cost_center": [],
                                 "transport_cost_center": [],
                                 "receiving_cost_center": [],
                                 "follow_up_cost_center": [],
                                 "requested_by": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[2:43]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["PR"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged.dropna(subset=["PR"], axis=0, inplace=True)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["requested_quantity"] = df_merged["requested_quantity"].apply(lambda x: str(x).replace(",", ""))

            return df_merged

    def iw39(self):
        """
        Data cleaning for pcl files
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"selected_line": [],
                                 "order_type": [],
                                 "maintenance_type": [],
                                 "long_text_exist": [],
                                 "priority_text": [],
                                 "order": [],
                                 "description": [],
                                 "user_status": [],
                                 "equipment": [],
                                 "object_description": [],
                                 "plant_section": [],
                                 "basic_start_date": [],
                                 "basic_finish_date": [],
                                 "plant": [],
                                 "main_work_center": [],
                                 "currency": [],
                                 "actual_total_cost": [],
                                 "plan_total_cost": [],
                                 "estimated_cost": [],
                                 "created_on": [],
                                 "entered_by  ": [],
                                 "changed_on": [],
                                 "changed_by": [],
                                 "responsible_cost_center": [],
                                 "system_status": [],
                                 "message": [],
                                 "profit_center": [],
                                 "cost_center": []
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:29]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["order"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged.dropna(subset=["order"], axis=0, inplace=True)
            df_merged = df_merged[df_merged["order"].apply(lambda x: len(x) >= 9)]

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["basic_start_date"] = pd.to_datetime(df_merged["basic_start_date"], format="%Y.%m.%d",
                                                           errors="coerce")
            df_merged["basic_finish_date"] = pd.to_datetime(df_merged["basic_finish_date"], format="%Y.%m.%d",
                                                            errors="coerce")
            df_merged["actual_total_cost"] = df_merged["actual_total_cost"].apply(lambda x: str(x).replace(",", ""))
            df_merged["plan_total_cost"] = df_merged["plan_total_cost"].apply(lambda x: str(x).replace(",", ""))
            df_merged["estimated_cost"] = df_merged["estimated_cost"].apply(lambda x: str(x).replace(",", ""))
            df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d", errors="coerce")
            df_merged["changed_on"] = pd.to_datetime(df_merged["changed_on"], format="%Y.%m.%d", errors="coerce")

            return df_merged

    def csks(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"client": [],
                                 "controlling_area": [],
                                 "CC_TCC": [],
                                 "valid_to": [],
                                 "valid_from": [],
                                 "actual_primary": [],
                                 "plan_primary": [],
                                 "company_code": [],
                                 "business_area": [],
                                 "CC_TCC_type": [],
                                 "responsible": [],
                                 "responsible_user": [],
                                 "currency": [],
                                 "costing_sheet": [],
                                 "tax": [],
                                 "profit_center": [],
                                 "plant": [],
                                 "logical_system": [],
                                 "created_on": [],
                                 "created_by": [],
                                 "actual_secondary": [],
                                 "actual_revenue": [],
                                 "commitment": [],
                                 "plan_secondary": [],
                                 "plan_revenue": [],
                                 "allocation_method": [],
                                 "record_quantity": [],
                                 "department_department": [],
                                 "sub_sequence_CC": [],
                                 "usage": [],
                                 "application": [],
                                 "overhead_key": [],
                                 "country": [],
                                 "title": [],
                                 "name": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[2:37]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["CC_TCC"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged.dropna(subset=["CC_TCC"], axis=0, inplace=True)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["valid_to"] = pd.to_datetime(df_merged["valid_to"], format="%Y.%m.%d",
                                                   errors="coerce")
            df_merged["valid_from"] = pd.to_datetime(df_merged["valid_from"], format="%Y.%m.%d",
                                                     errors="coerce")
            df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d",
                                                     errors="coerce")

            df_merged["CC_TCC_category"] = df_merged["CC_TCC"].apply(lambda x: "CC" if len(str(x)) == 9 else "TCC")

            # filter data in Plant 3
            df_profit_center = profit_center()
            list_profit_center = df_profit_center["profit_center"].unique().tolist()
            df_merged = df_merged[df_merged["profit_center"].isin(list_profit_center)]

            return df_merged

    def work_center(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"plant": [],
                                 "segment": [],
                                 "work_center": [],
                                 "short_description": [],
                                 "TCC": [],
                                 "level": [],
                                 "lower_level": [],
                                 "category": [],
                                 "capa_quantity": [],
                                 "is_deleted": [],
                                 "is_locked": [],
                                 "control_key": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:13]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["work_center"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged.dropna(subset=["work_center"], axis=0, inplace=True)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["EWC_MWC"] = df_merged["work_center"].apply(
                lambda x: "EWC" if str(x).startswith("SCT") else "MWC")

            # filter data in Plant 3
            df_profit_center = profit_center()
            list_segment_number = df_profit_center["segment_number"].unique().tolist()
            df_merged = df_merged[df_merged["segment"].isin(list_segment_number)]

            return df_merged

    def ih08(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"S": [],
                                 "equipment": [],
                                 "serial_number": [],
                                 "equipment_category": [],
                                 "planning_plant": [],
                                 "maintenance_plant": [],
                                 "technical_object_desc": [],
                                 "plant_section": [],
                                 "planner_group": [],
                                 "main_work_center": [],
                                 "cost_center": [],
                                 "super_equipment": [],
                                 "function_location": [],
                                 "ABC_flag": [],
                                 "inventory_number": [],
                                 "manufacturer_asset": [],
                                 "construction_year": [],
                                 "manufacturer_serial_number": [],
                                 "room": [],
                                 "technical_identification_number": [],
                                 "sort_field": [],
                                 "asset_number": [],
                                 "sub_asset_number": [],
                                 "object_type": [],
                                 "system_status": [],
                                 "user_status": [],
                                 "created_on": [],
                                 "created_by": [],
                                 "changed_on": [],
                                 "changed_by": [],
                                 "location": [],
                                 "construction_type": [],
                                 "material_number": [],
                                 "material_desc": [],
                                 "plant": [],
                                 "stock_location": [],
                                 "external_calibration": [],
                                 "model_number": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:39]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["equipment"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged.dropna(subset=["equipment"], axis=0, inplace=True)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d", errors="coerce")
            df_merged["changed_on"] = pd.to_datetime(df_merged["changed_on"], format="%Y.%m.%d", errors="coerce")

            # filter data in Plant 3
            df_profit_center = profit_center()
            list_segment_number = df_profit_center["segment_number"].unique().tolist()
            df_merged = df_merged[df_merged["plant_section"].isin([i[0:3] for i in list_segment_number])]

            return df_merged

    def ekko(self):
        """
        Data cleaning for raw data
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"client": [],
                                 "PO": [],
                                 "company_code": [],
                                 "document_category": [],
                                 "document_type": [],
                                 "control": [],
                                 "delete_flas": [],
                                 "status": [],
                                 "created_on": [],
                                 "created_by": [],
                                 "item_interval": [],
                                 "last_item": [],
                                 "supplier_code": [],
                                 "language": [],
                                 "payment_terms": [],
                                 "payment_in_1": [],
                                 "payment_in_2": [],
                                 "payment_in_3": [],
                                 "disc_percent_1": [],
                                 "disc_percent_2": [],
                                 "purchase_organization": [],
                                 "purchase_group": [],
                                 "currency": [],
                                 "exchange_rate": [],
                                 "exchange_rate_fixed": [],
                                 "document_date": [],
                                 "valid_start": [],
                                 "valid_end": [],
                                 "application_by": [],
                                 "quotation_deadline": [],
                                 "binding_period": [],
                                 "warranty": [],
                                 "bid_invitation": [],
                                 "quotation": [],
                                 "quotation_date": [],
                                 "your_reference": [],
                                 "sales_person": [],
                                 "telephone": [],
                                 "vendor": [],
                                 "customer": [],
                                 "agreement": [],
                                 "field_not_used": [],
                                 "complete_delivery": [],
                                 "GR_message": [],
                                 "supply_plant": [],
                                 "receive_vendor": [],
                                 "incoterms": [],
                                 "incoterms_2": [],
                                 "target_value": [],
                                 "collective_number": [],
                                 "document_condition": [],
                                 "procedure": [],
                                 "update_group": [],
                                 "invoice_party": [],
                                 "foreign_trade_data_number": [],
                                 "our_reference": [],
                                 "logical_system": [],
                                 "subitem_interval": [],
                                 "time_dep_condition": [],
                                 "release_group": [],
                                 "release_strategy": [],
                                 "release_flag": [],
                                 "release_status": [],
                                 "subject_to_release": [],
                                 "report_country": [],
                                 "release_document": [],
                                 "address_number": [],
                                 "country_tax_number": [],
                                 "vat_reg_number": [],
                                 "reason_for_cancellation": [],
                                 "document_number": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[2:73]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["PO"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged.dropna(subset=["PO"], axis=0, inplace=True)
            df_merged = df_merged[df_merged["PO"].apply(lambda x: len(x) >= 7)]

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d",
                                                     errors="coerce")
            df_merged["document_date"] = pd.to_datetime(df_merged["document_date"], format="%Y.%m.%d",
                                                        errors="coerce")
            df_merged["valid_start"] = pd.to_datetime(df_merged["valid_start"], format="%Y.%m.%d", errors="coerce")
            df_merged["valid_end"] = pd.to_datetime(df_merged["valid_end"], format="%Y.%m.%d", errors="coerce")

            return df_merged

    def me5a(self):
        """
        Data cleaning for pcl files
        :return: DataFrame
        """

        with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

            # get basic information for file
            file_path = Path(self.dir_pcl).stat()
            file_name = Path(self.dir_pcl).stem
            creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
            last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
            year_month_day = last_modified_time.strftime("%Y-%m-%d")
            year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

            # read file with Python
            data = f.readlines()

            # define dictionary to save raw data
            dict_cleaned_data = {"purchased_organization": [],
                                 "plant": [],
                                 "purchase_group": [],
                                 "S": [],
                                 "document_type": [],
                                 "PR": [],
                                 "PR_item": [],
                                 "PO": [],
                                 "material_number": [],
                                 "MRP_controller": [],
                                 "stock_location": [],
                                 "delivery_date_category": [],
                                 "delivery_date": [],
                                 "fixed_vendor": [],
                                 "vendor_name": [],
                                 "supplier_plant": [],
                                 "requested_by": [],
                                 "quantity": [],
                                 "unit": [],
                                 "creation_flag": [],
                                 "overall_release": [],
                                 "release_date": [],
                                 "unit_price": [],
                                 "price_unit": [],
                                 "total_value": [],
                                 "currency": [],
                                 "created_by": [],
                                 "requisition_date": [],
                                 "changed_date": [],
                                 "vendor_material_number": [],
                                 "consumption": [],
                                 "PO_date": [],
                                 "customer": [],
                                 "vendor": [],
                                 "manufacturer": [],
                                 "external_manufacturer": [],
                                 "deletion_flag": [],
                                 "release_flag": [],
                                 "PR_item_desc": [],
                                 }

            for row in data:

                try:

                    # split string
                    value = row.split(sep="|")[1:40]

                    # remove white space in string
                    value = [i.strip() for i in value]

                    # append data into dictionary
                    for t in enumerate(dict_cleaned_data.keys()):
                        column_position = t[0]
                        column_name = t[1]

                        dict_cleaned_data[column_name].append(value[column_position])

                except Exception as e:
                    print(f"{data.index(row)} : {row} \n {e}")

            # create DataFrame
            row_count = len(dict_cleaned_data["PR"])
            df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
            df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
                                   index=[i for i in range(0, row_count, 1)],
                                   columns=["file_name", "creation_time", "last_modified_time", "year_month",
                                            "year_month_day"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_data], axis=1)

            # drop duplicated rows
            df_merged.drop_duplicates(keep="first", inplace=True)

            # remove invalid rows
            df_merged.dropna(subset=["PR"], axis=0, inplace=True)

            # reset index
            df_merged.reset_index(drop=True, inplace=True)
            df_merged.drop(index=[0], inplace=True)
            df_merged.reset_index(drop=True, inplace=True)

            # data cleaning
            df_merged["PR_item_desc"] = df_merged["PR_item_desc"].apply(lambda x: str(x)[:-1].strip())
            df_merged["PR_item"] = df_merged["PR_item"].apply(lambda x: str(x).rjust(5, "0"))
            df_merged["delivery_date"] = pd.to_datetime(df_merged["delivery_date"], format="%Y%m%d", errors="coerce")
            df_merged["quantity"] = df_merged["quantity"].apply(lambda x: str(x).replace(",", ""))
            df_merged["release_date"] = pd.to_datetime(df_merged["release_date"], format="%Y.%m.%d", errors="coerce")
            df_merged["unit_price"] = df_merged["unit_price"].apply(lambda x: str(x).replace(",", ""))
            df_merged["total_value"] = df_merged["total_value"].apply(lambda x: str(x).replace(",", ""))
            df_merged["requisition_date"] = pd.to_datetime(df_merged["requisition_date"], format="%Y.%m.%d",
                                                           errors="coerce")
            df_merged["changed_date"] = pd.to_datetime(df_merged["changed_date"], format="%Y.%m.%d",
                                                       errors="coerce")
            df_merged["PO_date"] = pd.to_datetime(df_merged["PO_date"], format="%Y.%m.%d",
                                                  errors="coerce")

            return df_merged

    def se16_ekbe(self):
        pass
