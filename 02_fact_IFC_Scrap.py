import pandas as pd
from pathlib import Path
import datetime
import shutil
from Module_Common_Function import sca_digital_sqlserver_host, sca_digital_sqlserver_database, \
    sca_digital_sqlserver_user, sca_digital_sqlserver_password
from Module_Common_Function import MSSQL, dict_email, SendEmail, excel_suffix, PDFData
from Module_Virtual_Printer import VirtualPrinter
from apscheduler.schedulers.blocking import BlockingScheduler

# create connection with SQL Server
sca_con = MSSQL(server=sca_digital_sqlserver_host,
                database=sca_digital_sqlserver_database,
                user=sca_digital_sqlserver_user,
                password=sca_digital_sqlserver_password)

sca_dev = MSSQL(server=sca_digital_sqlserver_host,
                database="SCA_Digital_Dev",
                user=sca_digital_sqlserver_user,
                password=sca_digital_sqlserver_password)

# define variable to switch on / off debug mode
debug_mode = 0


class IFC:

    def __init__(self, dir_file):
        """
        :param dir_file: absolute directory of raw data
        """

        self.dir_file = dir_file

    def read_pp3401(self):
        """
        :return: DataFrame with cleaned data
        """

        # read data into pandas
        sheet_name = "Analyse"
        column_name = ["plant",
                       "segment",
                       "sub_segment",
                       "year_month",
                       "year_month_day",
                       "material_number",
                       "material_desc",
                       "notification_number",
                       "production_order",
                       "operation",
                       "production_shift",
                       "machine_work_center",
                       "machine_work_center_desc",
                       "CC",
                       "CC_desc",
                       "defect_code",
                       "defect_code_desc",
                       "defect_code_reporting_group",
                       "defect_code_reporting_group_desc",
                       "production_CC",
                       "production_CC_group",
                       "internal_failure_cost_CNY",
                       "reject_production_value_CNY",
                       "scrap_production_value_CNY",
                       "detected_rework_value_CNY",
                       "executed_rework_value_CNY",
                       "external_rework_value_CNY",
                       "cost_non_useable_parts_CNY",
                       "waste_from_reject_value_CNY",
                       "scrap_driven_by_PL_value_CNY",
                       "reject_external_company_value_CNY",
                       "scrap_external_company_value_CNY",
                       "reject_production_quantity_PC",
                       "scrap_production_quantity_PC",
                       "executed_rework_quantity_PC",
                       "detected_rework_quantity_PC",
                       "external_rework_quantity_PC",
                       "non_usable_parts_quantity_PC",
                       "waste_from_reject_quantity_PC",
                       "scrap_driven_by_PL_quantity_PC",
                       "reject_external_company_quantity_PC",
                       "scrap_external_company_quantity_PC"
                       ]
        used_column = [i for i in range(0, 42, 1)]

        df_raw_data = pd.read_excel(io=self.dir_file,
                                    sheet_name=sheet_name,
                                    usecols=used_column,
                                    names=column_name,
                                    header=None,
                                    skiprows=[0, 1],
                                    dtype="str")

        # drop blank rows
        df_raw_data.dropna(subset=["material_number", "year_month_day"], how="any", axis=0, inplace=True)

        # drop duplicated rows
        df_raw_data.drop_duplicates(inplace=True)
        df_raw_data.reset_index(drop=True, inplace=True)

        # get file name
        file_name = Path(self.dir_file).stem
        creation_time = datetime.datetime.fromtimestamp(Path(self.dir_file).stat().st_ctime)
        last_modified_time = datetime.datetime.fromtimestamp(Path(self.dir_file).stat().st_mtime)

        row_count = len(df_raw_data)

        # create DataFrame
        df_file = pd.DataFrame([[file_name, creation_time, last_modified_time]],
                               index=[i for i in range(0, row_count, 1)],
                               columns=["file_name", "creation_time", "last_modified_time"])

        # concatenate DataFrame
        df_merged = pd.concat([df_file, df_raw_data], axis=1)

        # data cleaning for raw data
        for i in range(0, row_count, 1):
            material_number = df_merged.iloc[i]["material_number"]
            notification = df_merged.iloc[i]["notification_number"]
            order = df_merged.iloc[i]["production_order"]
            mwc = df_merged.iloc[i]["machine_work_center"]
            cc = df_merged.iloc[i]["CC"]
            prod_cc = df_merged.iloc[i]["production_CC"]
            prod_cc_group = df_merged.iloc[i]["production_CC_group"]
            year_month_day = df_merged.iloc[i]["year_month_day"]
            year_month = df_merged.iloc[i]["year_month"]

            df_merged.loc[i, "material_number"] = material_number.split("/")[-1]
            df_merged.loc[i, "notification_number"] = notification.split("/")[-1]
            df_merged.loc[i, "production_order"] = order.split("/")[-1]
            df_merged.loc[i, "machine_work_center"] = mwc.split("/")[-1]
            df_merged.loc[i, "CC"] = cc.split("/")[-1]
            df_merged.loc[i, "production_CC"] = prod_cc.split("/")[-1]
            df_merged.loc[i, "production_CC_group"] = prod_cc_group.split("/")[-1]
            df_merged.loc[i, "year_month_day"] = datetime.datetime.strptime(year_month_day.replace(".", "-"),
                                                                            "%Y-%m-%d")
            df_merged.loc[i, "year_month"] = year_month.replace(".", "")

        # replace value in defect code description
        tag_str = "Not assigned"
        for i in range(0, row_count, 1):
            defect_code_desc = df_merged.iloc[i]["defect_code_desc"]

            if tag_str.lower() in defect_code_desc.lower():
                df_merged.loc[i, "defect_code_desc"] = "Not assigned_routing rework"

        return df_merged

    def read_pp1000(self):
        """
        :return: DataFrame with cleaned data
        """

        # read data into pandas
        sheet_name = "Analyse"
        used_columns = [i for i in range(0, 18, 1)]
        column_name = ["segment",
                       "material_number",
                       "material_desc",
                       "production_order",
                       "operation",
                       "element_work_center",
                       "element_work_center_desc",
                       "machine_work_center",
                       "machine_work_center_desc",
                       "production_shift",
                       "year_month_day",
                       "order_entry_date",
                       "control_key",
                       "control_key_desc",
                       "production_quantity",
                       "yield_quantity",
                       "scrap_quantity",
                       "rework_quantity_without_component"
                       ]

        df_raw_data = pd.read_excel(io=self.dir_file,
                                    sheet_name=sheet_name,
                                    usecols=used_columns,
                                    names=column_name,
                                    header=None,
                                    skiprows=[0, 1],
                                    dtype="str")

        # drop blank rows
        df_raw_data.dropna(subset=["material_number", "year_month_day"], how="any", axis=0, inplace=True)

        # drop duplicated rows
        df_raw_data.drop_duplicates(inplace=True)
        df_raw_data.reset_index(drop=True, inplace=True)

        # get file name
        file_name = Path(self.dir_file).stem
        creation_time = datetime.datetime.fromtimestamp(Path(self.dir_file).stat().st_ctime)
        last_modified_time = datetime.datetime.fromtimestamp(Path(self.dir_file).stat().st_mtime)

        row_count = len(df_raw_data)

        # create DataFrame
        df_file = pd.DataFrame([[file_name, creation_time, last_modified_time]],
                               index=[i for i in range(0, row_count, 1)],
                               columns=["file_name", "creation_time", "last_modified_time"])

        # concatenate DataFrame
        df_merged = pd.concat([df_file, df_raw_data], axis=1)

        # data cleaning for raw data
        for i in range(0, row_count, 1):
            material_number = df_merged.iloc[i]["material_number"]
            order = df_merged.iloc[i]["production_order"]
            ewc = df_merged.iloc[i]["element_work_center"]
            mwc = df_merged.iloc[i]["machine_work_center"]
            year_month_day = df_merged.iloc[i]["year_month_day"]
            order_entry_date = df_merged.iloc[i]["order_entry_date"]
            control_key = df_merged.iloc[i]["control_key"]
            control_key_desc = df_merged.iloc[i]["control_key_desc"]

            df_merged.loc[i, "material_number"] = material_number.split("/")[-1]
            df_merged.loc[i, "production_order"] = order.split("/")[-1]
            df_merged.loc[i, "element_work_center"] = ewc.split("/")[-1]
            df_merged.loc[i, "machine_work_center"] = mwc.split("/")[-1]
            df_merged.loc[i, "year_month_day"] = datetime.datetime.strptime(year_month_day.replace(".", "-"),
                                                                            "%Y-%m-%d")
            df_merged.loc[i, "order_entry_date"] = datetime.datetime.strptime(order_entry_date.replace(".", "-"),
                                                                              "%Y-%m-%d")
            df_merged.loc[i, "year_month"] = year_month_day.replace(".", "")[0:6]
            df_merged.loc[i, "control_key"] = control_key.split("/")[-1]
            df_merged.loc[i, "control_key_desc"] = control_key_desc.split("/")[-1]

        # data cleaning for "control key"
        abnormal_control_key = []
        for i in range(0, row_count, 1):
            control_key = df_merged.iloc[i]["control_key"]

            if control_key in ["#"]:
                abnormal_control_key.append(i)

        for j in abnormal_control_key:
            scrap_quantity = df_merged.iloc[j]["scrap_quantity"]
            rework_quantity = df_merged.iloc[j]["rework_quantity_without_component"]

            df_merged.loc[j + 1, "scrap_quantity"] = scrap_quantity
            df_merged.loc[j + 1, "rework_quantity_without_component"] = rework_quantity

        # drop rows with abnormal control key
        df_merged.drop(index=abnormal_control_key, inplace=True, axis=0)
        df_merged.reset_index(drop=True, inplace=True)

        return df_merged


def pp1000():
    """
    Read data from EXCEL file and saved into SQL Server after cleaning.
    :return: None
    """

    # define variables to save table name
    table_ifc = "fact_BW_PP1000"

    # absolute directory of raw data
    dir_ifc = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\PP1000"

    # define variables to save sql query
    sql_delete = f"""
    DECLARE @current_date DATE = GETDATE();

    DECLARE @current_year INT = DATEPART(YEAR, @current_date);
    DECLARE @current_month INT = DATEPART(MONTH, @current_date);

    DECLARE @last_month_year INT = CASE
                                       WHEN @current_month = 1 THEN
                                           @current_year - 1
                                       ELSE
                                           @current_year
                                   END;
    DECLARE @last_month_month INT = CASE
                                        WHEN @current_month = 1 THEN
                                            12
                                        ELSE
                                            @current_month - 1
                                    END;

    DECLARE @last_month_month_string VARCHAR(2);
    SET @last_month_month_string = CAST(@last_month_month AS VARCHAR(2));

    IF LEN(@last_month_month_string) = 1
    BEGIN
        SET @last_month_month_string = '0' + @last_month_month_string
    END

    DECLARE @last_month_year_string VARCHAR(4);
    SET @last_month_year_string = CAST(@last_month_year AS VARCHAR(4));

    DECLARE @last_month_year_month_string VARCHAR(6);
    SET @last_month_year_month_string = @last_month_year_string + @last_month_month_string


    DELETE FROM {table_ifc}
    WHERE [year_month] IN ( @last_month_year_month_string, FORMAT(GETDATE(), 'yyyyMM'))
    """



    # debug mode: replace data
    if debug_mode == 1:

        list_ifc = []
        for file in Path(dir_ifc).iterdir():
            if (Path(file).is_file()) and (Path(file).suffix.lower() in excel_suffix) and ("$" not in Path(file).stem):
                list_ifc.append(Path(file))

        # IFC
        df_ifc = pd.DataFrame()
        for i in list_ifc:
            data = IFC(dir_file=i).read_pp1000()
            df_ifc = pd.concat([df_ifc, data], axis=0)

        # remove duplicated rows
        df_ifc.drop_duplicates(inplace=True)
        print(f"Read {table_ifc} into pandas !")

        # save data into SQL Server
        df_ifc.to_sql(con=sca_con.sqlalchemy_connection(),
                      name=table_ifc,
                      chunksize=1000,
                      if_exists="replace",
                      index=False,
                      index_label=None)

        sca_con.add_table_property(table_name=table_ifc,
                                   table_desc="Huang, Jinlong_fact_Daily production quantity for each operation.")

        print(f"Saved {table_ifc} into SQL Server !")


    # real trial run mode: append data
    elif debug_mode == 0:

        # get current month and last month
        today = datetime.datetime.today()
        current_month = today.month
        last_month = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
        last_month_year, last_month_month = last_month

        last_month_str = "".join([str(last_month_year), str(last_month_month).rjust(2, "0")])
        current_month_str = "".join([str(today.year), str(current_month).rjust(2, "0")])

        # filter file in current month and last month
        list_ifc = []
        for file in Path(dir_ifc).iterdir():
            if (Path(file).is_file()) and (Path(file).suffix.lower() in excel_suffix) and (
                    "$" not in Path(file).stem) and (
                    (last_month_str in Path(file).stem) or (current_month_str in Path(file).stem)):
                list_ifc.append(Path(file))

        # read data into pandas
        df_ifc = pd.DataFrame()
        try:
            for i in list_ifc:
                data = IFC(dir_file=i).read_pp1000()
                df_ifc = pd.concat([df_ifc, data], axis=0)

            # remove duplicated rows
            df_ifc.drop_duplicates(inplace=True)
            print(f"Read {table_ifc} into pandas!")

            try:

                # delete data in current month and last month from database
                sca_con.execute_sql_query(sql=sql_delete)

                # save data into SQL Server
                df_ifc.to_sql(con=sca_con.sqlalchemy_connection(),
                              name=table_ifc,
                              chunksize=1000,
                              if_exists="append",
                              index=False,
                              index_label=None)

                print("Saved data into SQL Server successfully !")

            except Exception as e:
                SendEmail(sender_name="Python Error",
                          sender_address=dict_email["HuangJinlong"],
                          receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"], dict_email["ZhouPengrui"]],
                          cc=[],
                          subject=f"Error: {table_ifc}",
                          content=f"Failed to save {table_ifc} into SQL Server. \n {e}!").send_email_with_text()

        except Exception as e:
            SendEmail(sender_name="Python Error",
                      sender_address=dict_email["HuangJinlong"],
                      receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"], dict_email["ZhouPengrui"]],
                      cc=[],
                      subject=f"Error: {table_ifc}",
                      content=f"Failed to save {table_ifc} into SQL Server. \n {e}!").send_email_with_text()


def pp3401():
    """
    Read data from EXCEL and saved into SQL Server after cleaning
    :return: None
    """

    # define variables to save table name
    table_ifc = "fact_BW_PP3401"

    # absolute directory of raw data
    dir_ifc = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\PP3401"

    # create variable to save SQL query
    sql_delete = f"""
DECLARE @current_date DATE = GETDATE();

DECLARE @current_year INT = DATEPART(YEAR, @current_date);
DECLARE @current_month INT = DATEPART(MONTH, @current_date);

DECLARE @last_month_year INT = CASE
                                   WHEN @current_month = 1 THEN
                                       @current_year - 1
                                   ELSE
                                       @current_year
                               END;
DECLARE @last_month_month INT = CASE
                                    WHEN @current_month = 1 THEN
                                        12
                                    ELSE
                                        @current_month - 1
                                END;

DECLARE @last_month_month_string VARCHAR(2);
SET @last_month_month_string = CAST(@last_month_month AS VARCHAR(2));

IF LEN(@last_month_month_string) = 1
BEGIN
    SET @last_month_month_string = '0' + @last_month_month_string
END

DECLARE @last_month_year_string VARCHAR(4);
SET @last_month_year_string = CAST(@last_month_year AS VARCHAR(4));

DECLARE @last_month_year_month_string VARCHAR(6);
SET @last_month_year_month_string = @last_month_year_string + @last_month_month_string


DELETE FROM {table_ifc}
WHERE [year_month] IN ( @last_month_year_month_string, FORMAT(GETDATE(), 'yyyyMM'))
"""


    # debug mode
    if debug_mode == 1:

        # get absolute directory of raw data
        list_ifc = []
        for file in Path(dir_ifc).iterdir():
            if (Path(file).is_file()) and (Path(file).suffix.lower() in excel_suffix) and (
                    "$" not in Path(file).stem):
                list_ifc.append(Path(file))

        # IFC
        df_ifc = pd.DataFrame()
        for i in list_ifc:
            data = IFC(dir_file=i).read_pp3401()
            df_ifc = pd.concat([df_ifc, data], axis=0)

        # remove duplicated rows
        df_ifc.drop_duplicates(inplace=True)
        print(f"Read {table_ifc} into pandas !")

        # save data into SQL Server
        df_ifc.to_sql(con=sca_con.sqlalchemy_connection(),
                      name=table_ifc,
                      chunksize=1000,
                      if_exists="replace",
                      index=False,
                      index_label=None)

        sca_con.add_table_property(table_name=table_ifc,
                                   table_desc="Huang, Jinlong_fact_IFC list which including scrap and rework.")
        print(f"Saved {table_ifc} into SQL Server !")

    elif debug_mode == 0:

        # get current month and last month
        today = datetime.datetime.today()
        current_month = today.month
        last_month = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
        last_month_year, last_month_month = last_month

        last_month_str = "".join([str(last_month_year), str(last_month_month).rjust(2, "0")])
        current_month_str = "".join([str(today.year), str(current_month).rjust(2, "0")])

        # filter file in current month and last month
        list_ifc = []
        for file in Path(dir_ifc).iterdir():
            if (Path(file).is_file()) and (Path(file).suffix.lower() in excel_suffix) and (
                    "$" not in Path(file).stem) and (
                    (last_month_str in Path(file).stem) or (current_month_str in Path(file).stem)):
                list_ifc.append(Path(file))

        # read data into pandas
        df_ifc = pd.DataFrame()
        try:
            for i in list_ifc:
                data = IFC(dir_file=i).read_pp3401()
                df_ifc = pd.concat([df_ifc, data], axis=0)

            # remove duplicated rows
            df_ifc.drop_duplicates(inplace=True)
            print(f"Read {table_ifc} into pandas !")

            try:

                # delete current month and last month data in database
                sca_con.execute_sql_query(sql=sql_delete)

                # save data into SQL Server
                df_ifc.to_sql(con=sca_con.sqlalchemy_connection(),
                              name=table_ifc,
                              chunksize=1000,
                              if_exists="append",
                              index=False,
                              index_label=None)

                print(f"Saved {table_ifc} into SQL Server !")

            except Exception as e:
                SendEmail(sender_name="Python Error",
                          sender_address=dict_email["HuangJinlong"],
                          receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"], dict_email["ZhouPengrui"]],
                          cc=[],
                          subject=f"Error: {table_ifc}",
                          content=f"Failed to save {table_ifc} into SQL Server. \n {e}!").send_email_with_text()

        except Exception as e:
            SendEmail(sender_name="Python Error",
                      sender_address=dict_email["HuangJinlong"],
                      receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"]],
                      cc=[],
                      subject=f"Error: {table_ifc}",
                      content=f"Failed to read {table_ifc} into pandas. \n {e}!").send_email_with_text()


def mm03_pcl():
    """
    Read data from PCL file and saved data into SQL Server after cleaning
    :return: None
    """

    # define variable to save basic information
    table_mm03 = "fact_SAP_MM03"
    dir_tbd = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\08_Private Database\99_python_application\Virtual_Printer\11_MM03"
    dir_done = Path.joinpath(Path(dir_tbd), "Done")

    # Get absolute directory of raw data
    list_pcl = []
    for file in Path(dir_tbd).iterdir():
        if (Path(file).is_file()) and (Path(file).suffix.lower() == ".pcl"):
            list_pcl.append(Path(file))

    print(f"{table_mm03}: got absolute directory !")


    # data saved into development environment
    if debug_mode == 1:

        for file in list_pcl:
            # call function to read data into pandas
            df_mm03 = VirtualPrinter(dir_pcl=file).mm03()
            print(f"{table_mm03}: read data into pandas")

            # move file to destination folder
            shutil.move(
                src=file,
                dst=Path(dir_done).joinpath(Path(file).name)
            )
            print(f"{table_mm03}: move file to destination folder")

            # save data into SQL Server
            df_mm03.to_sql(con=sca_dev.sqlalchemy_connection(),
                           name=table_mm03,
                           if_exists="replace",
                           chunksize=800,
                           index=False,
                           index_label=None)

            # add table property
            sca_dev.add_table_property(table_name=table_mm03,
                                       table_desc="Huang, Jinlong_fact_Material basic information which including material number / scrap ratio ......")

    # data saved into production environment
    elif debug_mode == 0:

        for file in list_pcl:

            try:

                # call function to read data into pandas
                df_mm03 = VirtualPrinter(dir_pcl=file).mm03()
                print(f"{table_mm03}: read data into pandas")

                # move file to destination folder
                shutil.move(
                    src=file,
                    dst=Path(dir_done).joinpath(Path(file).name)
                )
                print(f"{table_mm03}: move file to destination folder")

                try:
                    # save data into SQL Server
                    df_mm03.to_sql(con=sca_con.sqlalchemy_connection(),
                                   name=table_mm03,
                                   if_exists="append",
                                   chunksize=800,
                                   index=False,
                                   index_label=None)

                except Exception as e:
                    SendEmail(sender_name="Python Error",
                              sender_address=dict_email["HuangJinlong"],
                              receiver=[dict_email["QuXiaoli"], dict_email["HuangJinlong"]],
                              cc=[],
                              subject=f"Error: {table_mm03}",
                              content=f"Failed to save {table_mm03} into SQL Server.\n{e}").send_email_with_text()
            except Exception as e:
                SendEmail(sender_name="Python Error",
                          sender_address=dict_email["HuangJinlong"],
                          receiver=[dict_email["QuXiaoli"], dict_email["HuangJinlong"]],
                          cc=[],
                          subject=f"Error: {table_mm03}",
                          content=f"Failed to read {table_mm03} into pandas.\n{e}").send_email_with_text()


def ifc_target():
    """
    Read data from EXCEL and saved into SQL Server after cleaning
    :return: None
    """

    table_ifc = "fact_IFC_target"

    dir_ifc = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\IFC_Target.xlsx"

    # define variables
    used_columns = [i for i in range(0, 5, 1)]
    column_name = ["year", "segment", "IFC_target", "Non_Usable_target", "total_target"]

    # define one variable to switch on / off debug mode
    debug_mode = 0

    # save data into development environment
    if debug_mode == 1:

        # IFC
        df_ifc = pd.read_excel(io=dir_ifc,
                               sheet_name=0,
                               usecols=used_columns,
                               names=column_name,
                               header=None,
                               skiprows=[0],
                               dtype="str"
                               )

        # remove duplicated rows
        df_ifc.drop_duplicates(inplace=True)
        print(f"{table_ifc} : read data into pandas !")

        # save data into SQL Server
        df_ifc.to_sql(con=sca_dev.sqlalchemy_connection(),
                      name=table_ifc,
                      chunksize=1000,
                      if_exists="replace",
                      index=False,
                      index_label=None)

        sca_dev.add_table_property(table_name=table_ifc,
                                   table_desc="Huang, Jinlong_fact_Yearly IFC target for each segment.")
        print(f"{table_ifc}: saved data into SQL Server!")

    elif debug_mode == 0:
        try:
            # IFC
            df_ifc = pd.read_excel(io=dir_ifc,
                                   sheet_name=0,
                                   usecols=used_columns,
                                   names=column_name,
                                   header=None,
                                   skiprows=[0],
                                   dtype="str"
                                   )

            # remove duplicated rows
            df_ifc.drop_duplicates(inplace=True)
            print(f"{table_ifc}: read data into pandas !")

            try:
                # save data into SQL Server
                df_ifc.to_sql(con=sca_con.sqlalchemy_connection(),
                              name=table_ifc,
                              chunksize=1000,
                              if_exists="replace",
                              index=False,
                              index_label=None)

                sca_con.add_table_property(table_name=table_ifc,
                                           table_desc="Huang, Jinlong_fact_Yearly IFC target for each segment.")
                print("Saved data into SQL Server successfully !")

            except Exception as e:
                SendEmail(sender_name="Python Error",
                          sender_address=dict_email["HuangJinlong"],
                          receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"]],
                          cc=[],
                          subject=f"Error: {table_ifc}",
                          content=f"Failed to save {table_ifc} into SQL Server. \n {e}!").send_email_with_text()

        except Exception as e:
            SendEmail(sender_name="Python Error",
                      sender_address=dict_email["HuangJinlong"],
                      receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"]],
                      cc=[],
                      subject=f"Error: {table_ifc}",
                      content=f"Failed to read {table_ifc} into pandas. \n {e}!").send_email_with_text()


def pdf_ifc_category_definition():
    """
    Transform PDF file to base64
    :return: None
    """

    # define variable to save absolute directory of raw data
    dir_pdf = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\IFC_Non_Usable_category.pdf"

    # define table name
    table_name = "fact_IFC_category_definition"

    # define one switch to turn on / off debug mode
    debug_mode = 0

    # debug mode
    if debug_mode == 1:
        # read data into pandas
        df_pdf = PDFData(dir_pdf=dir_pdf).convert_to_base64()
        print("Converted to base64 successfully !")

        # save data into SQL Server
        df_pdf.to_sql(
            name=table_name,
            chunksize=1000,
            con=sca_con.sqlalchemy_connection(),
            if_exists="replace",
            index=False,
            index_label=None
        )

        # add table property
        sca_con.add_table_property(table_name=table_name,
                                   table_desc="Huang, Jinlong_fact_PDF transfered to base64, which saved IFC category definition.")

        print("Saved data into SQL Server successfully !")

    elif debug_mode == 0:

        # read data into pandas
        try:
            df_pdf = PDFData(dir_pdf=dir_pdf).convert_to_base64()

            print("Converted to base64 successfully !")

            # save data into SQL Server
            try:
                df_pdf.to_sql(
                    name=table_name,
                    chunksize=1000,
                    con=sca_con.sqlalchemy_connection(),
                    if_exists="replace",
                    index=False,
                    index_label=None
                )

                # add table property
                sca_con.add_table_property(table_name=table_name,
                                           table_desc="Huang, Jinlong_fact_PDF transfered to base64, which saved IFC category definition.")

                print("Saved data into SQL Server successfully !")

            except Exception as e:

                SendEmail(sender_name="Python Error",
                          sender_address=dict_email["HuangJinlong"],
                          receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"]],
                          cc=[],
                          subject="Error: {}".format(table_name),
                          content="Failed to save data into SQL Server. {}".format(e)).send_email_with_text()

        except Exception as e:

            SendEmail(sender_name="Python Error",
                      sender_address=dict_email["HuangJinlong"],
                      receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"]],
                      cc=[],
                      subject="Error: {}".format(table_name),
                      content="Failed to read data into pandas. {}".format(e)).send_email_with_text()


def pdf_ifc_p_170001():
    """
    Transform PDF file to base64
    :return: None
    """

    # define variable to save absolute directory of raw data
    dir_pdf = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\IFC_P_170001.pdf"

    # define table name
    table_name = "fact_IFC_P_170001"

    # define one switch to turn on / off debug mode
    debug_mode = 0

    # debug mode
    if debug_mode == 1:
        # read data into pandas
        df_pdf = PDFData(dir_pdf=dir_pdf).convert_to_base64()
        print("Converted to base64 successfully !")

        # save data into SQL Server
        df_pdf.to_sql(
            name=table_name,
            chunksize=1000,
            con=sca_con.sqlalchemy_connection(),
            if_exists="replace",
            index=False,
            index_label=None
        )

        # add table property
        sca_con.add_table_property(table_name=table_name,
                                   table_desc="Huang, Jinlong_fact_PDF file saved as base64, IFC_P_170001")

        print("Saved data into SQL Server successfully !")

    elif debug_mode == 0:

        # read data into pandas
        try:
            df_pdf = PDFData(dir_pdf=dir_pdf).convert_to_base64()

            print("Converted to base64 successfully !")

            # save data into SQL Server
            try:
                df_pdf.to_sql(
                    name=table_name,
                    chunksize=1000,
                    con=sca_con.sqlalchemy_connection(),
                    if_exists="replace",
                    index=False,
                    index_label=None
                )

                # add table property
                sca_con.add_table_property(table_name=table_name,
                                           table_desc="Huang, Jinlong_fact_PDF file saved as base64, IFC_P_170001")

                print("Saved data into SQL Server successfully !")

            except Exception as e:

                SendEmail(sender_name="Python Error",
                          sender_address=dict_email["HuangJinlong"],
                          receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"]],
                          cc=[],
                          subject="Error: {}".format(table_name),
                          content="Failed to save data into SQL Server. {}".format(e)).send_email_with_text()

        except Exception as e:

            SendEmail(sender_name="Python Error",
                      sender_address=dict_email["HuangJinlong"],
                      receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"]],
                      cc=[],
                      subject="Error: {}".format(table_name),
                      content="Failed to read data into pandas. {}".format(e)).send_email_with_text()


def data_source():

    # table name
    db_table_name = "fact_IFC_Scrap_data_source"

    ifc_scrap = [
        {
            "fact_IFC_PP1000": r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\PP1000"},
        {
            "fact_IFC_PP3401": r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\PP3401"},
        {
            "fact_IFC_target": r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\IFC_Target.xlsx"},
        {
            "fact_IFC_category_definition": r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\IFC_Non_Usable_category.pdf"},
        {
            "fact_IFC_P_170001": r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\05_Project Database\IFC_quxol\IFC_P_170001.pdf"},
        {
            "dim_pv_for_exclude_tl": r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\88_ Information Sharing\01_Report and Documents\06_Data Management\01_Data Quality\14_OrderPrechecking before end of month\Order Segment Category Info.xlsx"},
        {
            "dim_blocked_work_center": r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\03_Digitalization\06_Data Flow Architechture\01_维度表\Dim_Plant3_CC_TCC_WC_Structure.xlsm"},
        {
            "dim_segment_department": r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\88_ Information Sharing\01_Report and Documents\06_Data Management\01_Data Quality\14_OrderPrechecking before end of month\Order Segment Category Info.xlsx"},
    ]

    # initialization for variables
    df_merged = pd.DataFrame(dtype="str")

    # get basic data for file
    for dictionary in ifc_scrap:

        # initialization for variables
        table_name = None
        directory = None
        creation_time = None
        last_modified_time = None

        for table_name, directory in dictionary.items():

            if Path(directory).is_file() == True:
                creation_time = datetime.datetime.fromtimestamp(Path(directory).stat().st_ctime)
                last_modified_time = datetime.datetime.fromtimestamp(Path(directory).stat().st_mtime)

            elif Path(directory).is_dir() == True:

                list_creation_time = []
                list_last_modified_time = []

                for file in Path(directory).iterdir():
                    if (Path(file).is_file()) and (Path(file).suffix.lower() in excel_suffix) and (
                            "$" not in Path(file).stem):
                        time_1 = datetime.datetime.fromtimestamp(Path(file).stat().st_ctime)
                        time_2 = datetime.datetime.fromtimestamp(Path(file).stat().st_mtime)

                        list_creation_time.append(time_1)
                        list_last_modified_time.append(time_2)

                # get creation time and last modified time for one file
                creation_time = sorted(list_creation_time)[-1]
                last_modified_time = sorted(list_last_modified_time)[-1]

        # create DataFrame
        df_file = pd.DataFrame([["IFC_Scrap", table_name, directory, creation_time, last_modified_time]],
                               index=[0], columns=["project", "table_name", "directory", "creation_time",
                                                   "last_modified_time"])

        # concatenate DataFrame
        df_merged = pd.concat([df_merged, df_file], axis=0)
        print("Read data into pandas successfully !")

    # save data into MS SQL Server
    try:
        df_merged.to_sql(con=sca_con.sqlalchemy_connection(),
                         name=db_table_name,
                         if_exists="replace",
                         chunksize=800,
                         index=False,
                         index_label=None
                         )

        sca_con.add_table_property(table_name=db_table_name,
                                   table_desc="Huang, Jinlong_fact_Data source list for IFC_Scrap project.")

        print("Saved data into SQL Server successfully !")

    except Exception as e:
        SendEmail(sender_name="Python Error",
                  sender_address=dict_email["HuangJinlong"],
                  receiver=[dict_email["HuangJinlong"], dict_email["QuXiaoli"]],
                  cc=[],
                  subject=f"Error: {db_table_name}",
                  content=f"Failed to read {db_table_name} into pandas. \n {e}!").send_email_with_text()


def main():
    pp1000()
    pp3401()
    mm03_pcl()
    ifc_target()
    data_source()
    pdf_ifc_category_definition()
    pdf_ifc_p_170001()


# set scheduled running
s_run = 0
if __name__ == "__main__":
    if s_run == 1:
        s = BlockingScheduler()
        s.add_job(main, "cron", day_of_week="0,1,2,3,4,5,6", hour=9, minute=25)
        s.start()

    elif s_run == 0:
        start_time = datetime.datetime.now()
        main()
        end_time = datetime.datetime.now()
        total_seconds = (end_time - start_time).total_seconds()
        print(total_seconds)
