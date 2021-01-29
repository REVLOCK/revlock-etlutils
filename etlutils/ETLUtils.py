import os
import json
import etlutils
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
from contextlib import suppress

from pandas.api.types import is_datetime64_any_dtype as is_datetime
from dateutil.relativedelta import relativedelta

import ast
from functools import reduce
from pandas.io.json._normalize import nested_to_record

print(pd.__version__)


class ETLUtils:

    @staticmethod
    def createDefaultDataFrames():
        to_return = {}
        default_mapping_path = os.path.join(
            etlutils.__path__[0], 'DefaultMappings.json')
        with open(default_mapping_path) as default_mapping:
            mappings = json.loads(default_mapping.read())
            for key in mappings:
                to_return[key] = pd.DataFrame(columns=mappings[key])
        return to_return

    @staticmethod
    def addMonths(date, numbOfMonths):
        if (type(date) == str):
            date = ETLUtils.convertDateStrToDateObject(date)
        toReturn = date + relativedelta(months=numbOfMonths)
        return toReturn

    @staticmethod
    def addYears(date, numOfYears):
        if (type(date) == str):
            date = ETLUtils.convertDateStrToDateObject(date)
        toReturn = date + relativedelta(years=numOfYears)
        return toReturn

    @staticmethod
    def addDays(date, numbOfDays):
        if (type(date) == str):
            date = ETLUtils.convertDateStrToDateObject(date)
        toReturn = date + relativedelta(days=numbOfDays)
        return toReturn

    @staticmethod
    def convertDateStrToDateObject(dateStr, format='%Y-%m-%d'):
        if type(dateStr) != str:
            raise ValueError('Date is not in the String format')
        toReturn = datetime.datetime.strptime(dateStr, format)
        return toReturn

    @staticmethod
    def convertStringToArray(strToConvert):
        toReturn = []
        toConvert = strToConvert
        if (type(strToConvert) == str):
            toConvert = [x.strip() for x in strToConvert.split(',')]
        toReturn = toReturn + toConvert
        return toReturn

    @staticmethod
    def readExcelFile(path):
        isDirectory = os.path.isdir(path)
        files = []
        excelData = []
        if isDirectory:
            for entry in os.listdir(path):
                if (os.path.isfile(os.path.join(path, entry)) and not entry.startswith('~$') and (
                    entry.endswith('.xlsx') or entry.endswith('.xls'))):
                    files.append(os.path.join(path, entry))
        else:
            files.append(path)

        for file in files:
            excel = pd.ExcelFile(file)
            sheetNames = excel.sheet_names
            for name in sheetNames:
                sheetData = ETLUtils.readExcelSheetFromExcelFile(file, name)
                excelData.append({name: sheetData})

        processedInputData = {}
        for data in excelData:
            for key in data:
                if processedInputData.get(key, None) is None:
                    processedInputData[key] = []

                processedInputData[key].append(data[key])

        dataToReturn = {}
        for key in processedInputData:
            dataToReturn[key] = pd.concat(processedInputData[key], ignore_index=True)

        return dataToReturn

    @staticmethod
    def readExcelSheetFromExcelFile(file, sheetname):
        if (os.path.exists(file)):
            toReturn = pd.read_excel(file, sheetname)
            toReturn = toReturn.dropna(
                axis=0, how='all', thresh=None, subset=None, inplace=False)
            return toReturn
        else:
            raise Exception('File: {} does not exists'.format(file))

    @staticmethod
    def filterRows(sheet, filterKey, filterValue):
        return sheet[sheet[filterKey] == filterValue]

    @staticmethod
    def convertToExpression(values, expression, conditionPipe):
        expressionToReturn = ''
        for i in values:
            if (expressionToReturn == ''):
                expressionToReturn += '(' + expression.format(i) + ')'
            else:
                expressionToReturn += ' ' + conditionPipe + \
                                      ' (' + expression.format(i) + ')'
        return expressionToReturn

    @staticmethod
    def getSourceColumns(sheet):
        toReturn = []
        for col in sheet.columns.values:
            if (col.__contains__('(source)')):
                toReturn.append(col)
        return toReturn

    @staticmethod
    def getTargetColumns(sheet):
        toReturn = []
        for col in sheet.columns.values:
            if col.__contains__('(source)'):
                col = col.replace('(source)', '').rstrip()
            toReturn.append(col)
        return toReturn

    @staticmethod
    def getStrictTargetColumns(sheet):
        toReturn = []
        for col in sheet.columns.values:
            if (col.__contains__('(source)') == False):
                toReturn.append(col)
        return toReturn

    @staticmethod
    def getAllColumnNames(sheet):
        toReturn = []
        for col in sheet.columns.values:
            toReturn.append(col)
        return toReturn

    @staticmethod
    def getColumnNameByValue(df, valueToSearch, sourceColumns):
        toReturn = ''
        for col in sourceColumns:
            if (col in df and df[col].isin([valueToSearch]).any()):
                toReturn = col
                break
        return toReturn

    @staticmethod
    def createFilterExpressionFromRow(rows, sourceColumns, dataFrameName, dropDuplicates, duplicateColumnName):
        expression = ''
        finalExpression = dataFrameName + '[{}]'
        if (dropDuplicates == True):
            finalExpression += ".drop_duplicates(['" + \
                               duplicateColumnName + "'])"
        count = 0
        for rIndex, row in rows.iterrows():
            expr = ''
            if (rIndex > 0 and count > 0):
                expression += ' & '
            for index in range(len(sourceColumns)):
                col = sourceColumns[index]
                if (col.__contains__('(source)')):
                    colVal = ETLUtils.convertStringToArray(row.loc[col])
                    col = col.replace('(source)', '').rstrip()
                else:
                    colVal = [row.loc[col]]

                if (index > 0):
                    expr += " & (" + ETLUtils.convertToExpression(colVal, dataFrameName + "['" + col + "'] == '{}'",
                                                                  '|') + ")"
                else:
                    expr += ETLUtils.convertToExpression(
                        colVal, dataFrameName + "['" + col + "'] == '{}'", '|')
            expression += expr
            count += 1
        finalExpression = finalExpression.format(expression)
        return finalExpression

    @staticmethod
    def getSubstring(str, startKey, endKey):
        if (str.__contains__(startKey) and str.__contains__(endKey)):
            beginIndex = str.index(startKey) + 1
            endIndex = str.index(endKey)
            str = str[beginIndex:endIndex]
        return str

    @staticmethod
    def getColumnIndexByColumnName(df, colName):
        return df.columns.get_loc(colName)

    @staticmethod
    def doUpdates(updatesSheet, dataFrame):
        dataFrame = ETLUtils.convert_datetime(dataFrame)
        dataFrame = ETLUtils.format_dates(dataFrame)
        sourceColumns = ETLUtils.getSourceColumns(updatesSheet)
        targetColumns = ETLUtils.getStrictTargetColumns(updatesSheet)
        columnNames = ETLUtils.getAllColumnNames(updatesSheet)
        for cIndex, _row in updatesSheet.iterrows():
            if (_row.empty == False):
                rulesDf = pd.DataFrame(columns=columnNames, index=[0])
                rulesDf.loc[0] = pd.Series(_row)
                expression = ETLUtils.createFilterExpressionFromRow(
                    rulesDf, sourceColumns, "dataFrame", False, '')
                print(expression)
                dfToUpdate = dataFrame
                if (expression != 'dataFrame[]'):
                    dfToUpdate = eval(expression)
                ETLUtils.updateData(rulesDf, dfToUpdate, dataFrame)

    @staticmethod
    def executeCustomMethod(colVal, val, refCol):
        if (colVal.__contains__('addMonths') and str(val) != ''):
            addMonthsFunction = colVal.replace(
                '[' + refCol + ']', "'" + str(val) + "'")
            val = eval('ETLUtils.' + addMonthsFunction)
        return val

    @staticmethod
    def sortDataFrame(dataFrame, sortKeys, asc=True):
        return dataFrame.sort_values(sortKeys, ascending=asc)

    @staticmethod
    def resolveSubstrings(string, startKey, endKey, df, originalDf):
        index = 0
        while True:
            if (type(string) == str and (string.__contains__(startKey) and string.__contains__(endKey))):
                if (string.startswith("VLOOKUP") == False):
                    toReplace = ETLUtils.getSubstring(string, startKey, endKey)
                    columnIndex = originalDf.columns.get_loc(toReplace) + 1
                    string = string.replace(startKey + toReplace + endKey,
                                            'INDIRECT(ADDRESS(ROW(),' + str(columnIndex) + ', 2))')
                elif (string.startswith("VLOOKUP") == True):
                    columnRange = 'MID(ADDRESS(ROW(),COLUMN()),2,SEARCH("$",ADDRESS(ROW(),COLUMN()),2)-2)'
                    toReplace = ETLUtils.getSubstring(string, startKey, endKey)
                    if (index == 0):
                        columnIndex = originalDf.columns.get_loc(toReplace) + 1
                        string = string.replace(startKey + toReplace + endKey,
                                                'INDIRECT(ADDRESS(ROW(),' + str(columnIndex) + ', 2))')
                    else:
                        string = string.replace(
                            startKey + toReplace + endKey, columnRange)

                    index += 1
            else:
                break
        return string

    @staticmethod
    def updateData(rules, dfToUpdate, originalDf):
        for index, rule in rules.iterrows():
            if (dfToUpdate.empty == False):
                for rIndex, _row in dfToUpdate.iterrows():
                    _dfToUpdatecolumnNames = ETLUtils.getAllColumnNames(
                        dfToUpdate)
                    _dfToUpdate = pd.DataFrame(
                        columns=_dfToUpdatecolumnNames, index=[0])
                    _dfToUpdate.loc[0] = pd.Series(_row)
                    for col in rules.columns.values:
                        __col = col
                        if (__col.__contains__('(source)')):
                            __col = __col.replace('(source)', '').rstrip()
                        if (__col in _dfToUpdate):
                            rVal = rule[col]
                            val = _dfToUpdate[__col].values[0]
                            if (type(rule[col]) == str and (rVal.startswith('EXRULE:'))):
                                colVal = rule[col]
                                colVal = colVal.replace('EXRULE:', '').rstrip()
                                colVal = '=' + \
                                         ETLUtils.resolveSubstrings(
                                             colVal, '[', ']', _dfToUpdate, originalDf)
                                print(colVal)
                                originalDf.loc[rIndex, __col] = colVal
                            elif (type(rule[col]) == str and (rVal.__contains__('[') and rVal.__contains__(']'))):
                                colVal = rule[col]
                                refCol = ETLUtils.getSubstring(
                                    colVal, '[', ']')
                                if (refCol in _dfToUpdate):
                                    val = _dfToUpdate[refCol].values[0]
                                    val = ETLUtils.executeCustomMethod(
                                        colVal, val, refCol)
                                    originalDf.loc[rIndex, __col] = val

    @staticmethod
    def inferData(rules, uniqueDf, outputDataFrame):
        for index, rule in rules.iterrows():
            if (uniqueDf.empty == False):
                data = uniqueDf.copy(deep=True)
                for col in rules.columns.values:
                    __col = col
                    if (__col.__contains__('(source)')):
                        __col = __col.replace('(source)', '').rstrip()
                    if (__col in data):
                        val = rule[col]
                        if (type(rule[col]) == str and (val.__contains__('[') and val.__contains__(']'))):
                            refCol = rule[col].replace(
                                '[', '').replace(']', '')
                            if (refCol in uniqueDf):
                                val = uniqueDf[refCol]
                        data[__col] = val
                outputDataFrame.append(data)
        return outputDataFrame

    @staticmethod
    def doInference(rulesFile, dataFrame, columnsToDrop):
        rulesSheet = ETLUtils.readExcelSheetFromExcelFile(
            rulesFile, 'Inference')
        sourceColumns = ETLUtils.getSourceColumns(rulesSheet)
        sourceRuleDataFrame = ETLUtils.filterRows(
            rulesSheet, 'Inference Type', 'Source')
        targetRuleDataFrame = ETLUtils.filterRows(
            rulesSheet, 'Inference Type', 'Target')
        outputDf = [dataFrame]
        groups = targetRuleDataFrame.groupby('Rule').groups
        for group in groups:
            sourceRules = ETLUtils.filterRows(
                sourceRuleDataFrame, 'Rule', group)
            expression = ETLUtils.createFilterExpressionFromRow(sourceRules, sourceColumns, "dataFrame", True,
                                                                columnsToDrop)
            uniqueDf = eval(expression)
            if (uniqueDf.empty == False and uniqueDf.shape[0] > 0):
                columnName = ETLUtils.getColumnNameByValue(
                    targetRuleDataFrame, group, sourceColumns)
                rules = targetRuleDataFrame[targetRuleDataFrame[columnName] == group]
                ETLUtils.inferData(rules, uniqueDf, outputDf)
        return pd.concat(outputDf, axis=0, ignore_index=True, sort=False)

    @staticmethod
    def convert_datetime(df):
        object_cols = [
            col for col, col_type in df.dtypes.iteritems() if col_type == 'object']
        df.loc[:, object_cols] = df[object_cols].apply(
            pd.to_datetime, errors='ignore')
        return df

    @staticmethod
    def format_dates(df, format='%Y-%m-%d'):
        date_cols = [
            column for column in df.columns if is_datetime(df[column])]
        df.loc[:, date_cols] = df[date_cols].apply(
            lambda x: x.dt.strftime(format).replace('NaT', ''))
        return df

    @staticmethod
    def readRevlockDefaultRules(ruleFile):
        toReturn = {}
        rules = ETLUtils.readExcelFile(ruleFile)
        for key in rules:
            toReturn[key] = {}
            for rIndex, _row in rules[key].iterrows():
                toReturn[key][_row['SourceCol']] = _row['TargetCol']
        return toReturn

    @staticmethod
    def populate_customer_address(df, col_street="BillingStreet", col_city="BillingCity", col_state="BillingState",
                                  col_postalCode="BillingPostalCode", col_country="BillingCountry"):
        return ETLUtils.getCustomerAddress(df[col_street], df[col_city], df[col_state], df[col_postalCode],
                                           df[col_country])

    @staticmethod
    def getCustomerAddress(street, city, state, postalCode, country):

        address = street.map(str) + ', ' \
                  + city.map(str) + ', ' \
                  + state.map(str) + ', ' \
                  + postalCode.map(lambda x: x if type(x) is str else "{:.0f}".format(x)) + ', ' \
                  + country.map(str)
        address = address.map(lambda x: x.replace(
            "nan, ", "").replace(", nan", ""))
        return address

    @staticmethod
    def convert_string_to_json(data):
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, dict) and len(v) > 0:
                    for _k, _v in v.items():
                        if isinstance(_v, str):
                            data[k][_k] = json.loads(_v)

    @staticmethod
    def JsonParser(data):
        to_return = ast.literal_eval(data)
        if isinstance(to_return, list):
            for index in range(len(to_return)):
                ETLUtils.convert_string_to_json(to_return[index])
        return to_return

    @staticmethod
    def readCSVFile(file, **kwargs):
        return pd.read_csv(file, error_bad_lines=False, **kwargs)

    @staticmethod
    def readCSVFiles(path, **kwargs):
        isDirectory = os.path.isdir(path)
        files = []
        toReturn = {}
        if (isDirectory):
            for entry in os.listdir(path):
                if (os.path.isfile(os.path.join(path, entry)) and os.path.join(path, entry).endswith('.csv')):
                    files.append(os.path.join(path, entry))
        else:
            files.append(path)
        for file in files:
            splitedPath = file.split('/')
            filename = splitedPath[len(splitedPath) - 1].split('.csv')[0]
            if '_' in filename:
                filename = filename.split('_')[0]

            if '-' in filename:
                filename = filename.split('-')[0]

            if (filename not in toReturn):
                print('Reading file: {} '.format(filename))
                toReturn[filename] = ETLUtils.readCSVFile(file, **kwargs)
        return toReturn

    @staticmethod
    def create_target_df(df, target_columns):
        if target_columns is not None:
            if isinstance(target_columns, list):
                return df[target_columns]
            elif isinstance(target_columns, dict):
                idx1 = pd.Index(target_columns.keys())
                idx2 = pd.Index(df.columns)
                target_column_names = idx1.intersection(idx2).array
                return df[target_column_names].rename(columns=target_columns)
        return df

    @staticmethod
    def normalize_object(df, column_name, source_columns, **kwargs):
        child_df = df[source_columns]

        def flatten(y): return pd.Series(nested_to_record(y, sep='.'))

        child_df = pd.concat([child_df, child_df[column_name].apply(flatten).add_prefix(f"{column_name}.")], axis=1)

        return child_df

    @staticmethod
    def normalize_array_prop(df, column_name, source_columns, **kwargs):
        child_df = df[source_columns]
        child_df = child_df.explode(column_name)

        def flatten(y): return pd.Series(nested_to_record(y, sep='.'))

        child_df = pd.concat([child_df, child_df[column_name].apply(
            flatten).add_prefix(f"{column_name}.")], axis=1)

        return child_df

    @staticmethod
    def array_prop_to_columns(df, column_name, source_columns, **kwargs):
        reducer = kwargs.get(
            'reducer', ETLUtils.array_to_dict_reducer('Name', 'Value'))
        child_df = df[source_columns]
        child_df = child_df.pipe(
            lambda x: x.drop(column_name, 1).join(
                x[column_name].apply(
                    lambda y: pd.Series(reduce(reducer, y, {})))
            )
        )
        return child_df

    @staticmethod
    def array_to_dict_reducer(key_prop, value_prop):
        def reducer(result, value):
            result[value[key_prop]] = value[value_prop]
            return result

        return reducer

    @staticmethod
    def expand_json_column(df, json_column):
        expander = json_column.pop('expander')
        column_name = json_column.pop('column_name')
        return expander(df, column_name, df.columns, **json_column)

    @staticmethod
    def writeFile(dfMapToWrite, outputFile, **kwargs):
        parse_dates = kwargs.get('parse_dates', True)
        writer = pd.ExcelWriter(outputFile, engine='xlsxwriter', datetime_format='yyyy-mm-dd hh:mm:ss',
                                date_format='yyyy-mm-dd')
        for key in dfMapToWrite:
            print('Writing {} Sheet'.format(key))
            dataFrame = dfMapToWrite[key]
            if parse_dates:
                dataFrame = ETLUtils.convert_datetime(dataFrame)
                dataFrame = ETLUtils.format_dates(dataFrame)
            dataFrame.to_excel(writer, index=None, header=True, sheet_name=key)
        writer.save()

    @staticmethod
    def get_snapshot(snapshot_dir, stream, column_mapping=None):
        snap_path = f"{snapshot_dir}/{stream}.snapshot.csv"

        # Ensure file exists
        if os.path.isfile(snap_path) is False:
            return None

        # Read the old snapshot, if present
        snap_df = pd.read_csv(snap_path)

        if column_mapping is not None:
            return ETLUtils.fix_datatypes(snap_df, column_mapping)
        else:
            return snap_df

    @staticmethod
    def combine_with_snapshot(snapshot_dir, stream, key, data_df, column_mapping=None, persist=False, override=False,
                              drop_duplicates=True):
        # get updated after merge but no persist.
        return ETLUtils.update_snapshot(snapshot_dir, stream, key, data_df, persist, override, column_mapping, None,
                                        None, drop_duplicates)

    @staticmethod
    def update_snapshot(snapshot_dir, stream, key, data_df, persist=True, override=False, column_mapping=None,
                        drop_column=None, sort_columns=None, drop_duplicates=True):
        snap_df = data_df

        if drop_duplicates:
            snap_df = snap_df.drop_duplicates(key, keep="last")

        if not override:
            # Get the old snapshot dataframe
            psnap_df = ETLUtils.get_snapshot(snapshot_dir, stream, column_mapping)

            if psnap_df is not None:
                # Combine with prior snapshot
                snap_df = snap_df.set_index(
                    key).combine_first(psnap_df.set_index(key))
                snap_df = snap_df.reset_index()
        if drop_column is not None:
            snap_df = snap_df.drop(columns=drop_column, errors='ignore')
        if persist:
            # Save this snapshot in correct spot
            snap_path = f"{snapshot_dir}/{stream}.snapshot.csv"
            snap_df.to_csv(snap_path, index=False)

        if sort_columns is not None:
            snap_df.sort_values(by=sort_columns)

        return snap_df

    @staticmethod
    def convertToString(df, column_name):
        df[column_name] = df[column_name].fillna('')
        return df[column_name].astype(str)

    @staticmethod
    def convertToDate(df, column_name):
        return pd.to_datetime(df[column_name]).dt.tz_localize(None)

    @staticmethod
    def convertToNumber(df, column_name):
        return pd.to_numeric(df[column_name])

    @staticmethod
    def convertToInteger(df, column_name):
        return df[column_name].astype(int)

    @staticmethod
    def fix_datatypes(df, column_mapping):
        target_df = df

        convertTo = {
            "string": ETLUtils.convertToString,
            "datetime": ETLUtils.convertToDate,
            "number": ETLUtils.convertToNumber,
            "integer": ETLUtils.convertToInteger,
        }

        for target_column in column_mapping.keys():

            if target_column not in target_df:
                continue

            column_metadata = column_mapping[target_column]

            if isinstance(column_metadata, str):
                column_type = "string"
            else:
                column_type = column_metadata.get("type", "string")

            target_df.loc[:, target_column] = convertTo[column_type](
                target_df, target_column)

        if 'Sync Source' not in target_df:
            return target_df[column_mapping.keys()]
        else:
            return target_df[list(column_mapping.keys()) + ['Sync Source']]

    @staticmethod
    def create_target_dataframe(df, column_mapping):
        target_df = df.copy(deep=True)

        convertTo = {
            "string": ETLUtils.convertToString,
            "datetime": ETLUtils.convertToDate,
            "number": ETLUtils.convertToNumber,
            "integer": ETLUtils.convertToInteger,
        }

        print(column_mapping.keys())
        for target_column in column_mapping.keys():

            column_metadata = column_mapping[target_column]

            if isinstance(column_metadata, str):
                column_metadata = {
                    "source_column": column_metadata, "column_type": "string", "default": ""}

            if column_metadata.get('source_column', None) is not None:
                source_column = column_metadata['source_column']
                column_type = column_metadata.get("type", "string")
                if source_column not in target_df:
                    target_df.loc[:, target_column] = column_metadata.get("default", None)
                else:
                    target_df.loc[:, target_column] = convertTo[column_type](
                        target_df, source_column)

            elif column_metadata.get('use_function', None) is not None:
                function = column_metadata['use_function']
                column_type = column_metadata.get("type", "string")

                if isinstance(function, str):
                    # Client chose to run a product standard function.
                    use_function = getattr(
                        ETLUtils, column_metadata['use_function'])
                else:
                    # Client has there on custom function.
                    use_function = function

                target_df.loc[:, target_column] = use_function(target_df)
            elif column_metadata.get('default', None) is not None:
                if target_df.empty:
                    target_df[target_column] = None
                else:
                    target_df.loc[:, target_column] = column_metadata['default']

        return target_df[column_mapping.keys()]

    @staticmethod
    def transform_dataframe(df, table_metadata, sync_source=None):

        _source_df = df
        if table_metadata.get('source_primary_key', None) is not None:
            _source_df = df.drop_duplicates(
                subset=table_metadata['source_primary_key'], keep="last")

        if table_metadata.get('json_columns', None) is not None:
            for json_column in table_metadata['json_columns']:
                _source_df = ETLUtils.expand_json_column(
                    _source_df, json_column)

        if table_metadata.get('filter', None) is not None:
            _source_df = table_metadata['filter'](_source_df)

        if table_metadata.get('column_mapping', None) is not None:
            _source_df = ETLUtils.create_target_dataframe(
                _source_df, table_metadata['column_mapping'])

        if sync_source is not None:
            _source_df['Sync Source'] = sync_source

        return _source_df

    @staticmethod
    def dateDiffIgnoreLeapDay(startDate, endDate):
        endDate = endDate + timedelta(days=1)
        diff = (endDate - startDate).days

        for year in range(startDate.year, endDate.year + 1, 1):
            date = None
            with suppress(ValueError):
                date = datetime.datetime(year, 2, 29)

            if date is not None and startDate < date < endDate:
                diff = diff - 1

        return diff;

    def establish_directories(global_vars):

        def get_var(var_name, default_value):
            return os.getenv(var_name, get_var(var_name, default_value))

        ROOT_DIR = get_var("ROOT_DIR", "/home/etl")
        base_input_dir = get_var("base_input_dir", f"{ROOT_DIR}/sync-output")
        output_dir = get_var("output_dir", f"{ROOT_DIR}/etl-output")
        snapshot_dir = get_var("snapshot_dir", f"{ROOT_DIR}/snapshots")
        today = get_var("today", None)

        if today is None:
            today = datetime.date.today()
        else:
            today = datetime.datetime.strptime(today, '%Y%m%d')

        print(f"ROOT_DIR is {ROOT_DIR}")
        print(f"base_input_dir is {base_input_dir}")
        print(f"output_dir is {output_dir}")
        print(f"snapshot_dir is {snapshot_dir}")
        print(f"today is {today}")

        with suppress(FileExistsError):
            os.makedirs(base_input_dir)
        with suppress(FileExistsError):
            os.makedirs(output_dir)
        with suppress(FileExistsError):
            os.makedirs(snapshot_dir)

        return base_input_dir, output_dir, snapshot_dir, today
