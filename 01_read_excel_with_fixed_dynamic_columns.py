from pathlib import Path
import datetime
import pandas
import pandas as pd
from numpy import nan as null


# create one object to clean data for raw data
class ProductionCapacity:
    """
    data cleaning for production capacity data and save cleaned data into SQL Server
    """

    def __init__(self, dir_xlsx):
        """
        :param dir_xlsx: initialization for directory of EXCEL file which saved raw data
        """
        self.dir_xlsx = dir_xlsx

    def dcc_demand(self):
        """
        :return: DataFrame with cleaned "DCC demand" data
        """

        # get file name without suffix
        file_name = Path(self.dir_xlsx).stem

        # get export date of raw data
        dcc_export_date = file_name.split("_")[-1]

        # read data in EXCEL
        df_raw_data = pd.read_excel(io=self.dir_xlsx, header=0, sheet_name=0)

        # filter column named as "Reqmts/Coverage"
        df_raw_data = df_raw_data[df_raw_data["Reqmts/Coverage"] == "Receipts"]

        # reset index
        df_raw_data.reset_index(drop=True, inplace=True)

        # remove specified rows
        removed_rows_index = []
        for i in range(0, len(df_raw_data), 1):
            if df_raw_data.iloc[i]["Material Description"][0:4] == "SPAN":
                removed_rows_index.append(i)

        df_raw_data.drop(labels=removed_rows_index, axis=0, inplace=True)

        # remove duplicated rows
        df_raw_data.drop_duplicates(inplace=True)

        # reset index
        df_raw_data.reset_index(drop=True, inplace=True)

        # get column names of raw data
        list_all_columns = df_raw_data.columns.to_list()
        list_fixed_columns = ["MRP Controller",
                              "MRP group",
                              "Material Description",
                              "Material",
                              "Days' supply",
                              "Reqmts/Coverage",
                              "Warehouse st",
                              "Stock P-Plant",
                              "Backlog",
                              "Total",
                              "Total without stock",
                              "Base Unit of Measure",
                              "Segment Number",
                              "Production type (local)",
                              "Production Type Description",
                              "MRP Lot Size",
                              "Minimum Lot Size",
                              "Planned Deliv. Time",
                              "Safety Stock",
                              "Target stock",
                              "Safety time/act.cov.",
                              "In-house production",
                              "Planning time fence",
                              "Purchasing Group",
                              "Vendor from Source List",
                              "Plant-sp.matl status",
                              "Stochastic Type",
                              "ABC Indicator",
                              "XYZ-Indicator",
                              "Product Hierarchy",
                              "In Quality Insp.",
                              "Blocked",
                              "Configurable material",
                              "MRP Type",
                              "Lot size",
                              "Target stock overexceeded [%]",
                              "Stock Type",
                              "Rounding value",
                              "Material Type",
                              "Material Group",
                              "GR processing time",
                              "Annual demand count",
                              "Budget-year-requirement",
                              "Description p. group",
                              "Consumption CurYr-2",
                              "Consumption CurYr-1",
                              "Cons. Current Year", ]

        # get name of dynamic column
        list_dynamic_columns = []
        for i in list_all_columns:
            if i not in list_fixed_columns:
                list_dynamic_columns.append(i)

        # unpivot raw data
        df_dcc_demand = pd.melt(df_raw_data, id_vars=list_fixed_columns, value_vars=list_dynamic_columns,
                                var_name="month_year", value_name="DCC_Demand")

        # reset index for cleaned data
        df_dcc_demand.reset_index(drop=True, inplace=True)

        # set name of "df_dcc_demand"
        df_dcc_demand.index.name = "internal_index"
        df_dcc_demand.reset_index(drop=False, inplace=True)

        # create DataFrame to save "file_name" and "dcc_export_date"
        df_file = pd.DataFrame(data=[[file_name, dcc_export_date]], columns=["file_name", "dcc_export_date"],
                               index=[i for i in range(0, len(df_dcc_demand), 1)], dtype="str")

        # concatenate DataFrame
        df_merged = pd.concat([df_file, df_dcc_demand], axis=1)

        # add "plant" to DataFrame
        row_count = len(df_merged)
        plant = []
        for i in range(0, row_count, 1):
            if str(df_merged.iloc[i]["Segment Number"]) == "341000":
                plant.append("8101")
            elif str(df_merged.iloc[i]["Segment Number"]) == "510000":
                plant.append("8101")
            elif str(df_merged.iloc[i]["Segment Number"]) == "50000":
                plant.append("1001")
            else:
                plant.append("8101")

        df_merged["plant"] = plant

        # add "year_month_day" to DataFrame
        year_month_day = []
        for i in range(0, row_count, 1):
            month_year = df_merged.iloc[i]["month_year"]
            year = month_year[-4:]
            month = month_year[-7:-5]
            day = "01"
            joined_str = "-".join([year, month, day])
            year_month_day.append(datetime.datetime.strptime(joined_str, "%Y-%m-%d"))

        df_merged["year_month_day"] = year_month_day

        # add "dcc_reversion" to DataFrame
        dcc_reversion = []
        for i in range(0, row_count, 1):
            export_date = df_merged.iloc[i]["dcc_export_date"]
            year = export_date[:4]
            month = export_date[4:6]
            day = "01"
            joined_date = "-".join([year, month, day])
            dcc_reversion.append(datetime.datetime.strptime(joined_date, "%Y-%m-%d"))

        df_merged["dcc_reversion"] = dcc_reversion

        # remove duplicated rows
        df_merged.drop_duplicates(inplace=True)

        # add new column "plant_material"
        for i in range(0, len(df_merged), 1):
            plant = df_merged.iloc[i]["plant"]
            material = df_merged.iloc[i]["Material"]
            df_merged.loc[i, "plant_material"] = "_".join([plant, material])

        # return cleaned data
        return df_merged
