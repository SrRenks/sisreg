from datetime import datetime, timedelta
from src.utils import Sisreg
import concurrent.futures
from tqdm import tqdm
import pandas as pd
import argparse
import tempfile
import psutil
import json
import sys
import os
import re


if __name__ == '__main__':
    from_date = datetime.today().strftime("%d/%m/%Y")
    to_date = (datetime.today() + timedelta(7)).strftime("%d/%m/%Y")
    parser = argparse.ArgumentParser(description='Sisreg class arg parser')
    parser.add_argument('--username', '-u', type=str, help='Login Username', required=True)
    parser.add_argument('--password', '-p', type=str, help='Login Password', required=True)
    parser.add_argument('--unit', '-ut', type=str, help='units', nargs='+', required=False)
    parser.add_argument('--from_date', '-f', type=str, help='initial range', default=from_date, required=False)
    parser.add_argument('--to_date', '-t', type=str, help='final range', default=to_date, required=False)
    parser.add_argument('--columns', '-c', type=str, help='selected columns', nargs='+', default=None, required=False)
    parser.add_argument('--banlist', '-b', type=str, help='string values to remove from dataframe in output based in string/regex', nargs='+', default=None, required=False)
    parser.add_argument('--export_path', '-ep', type=str, default=None, help='path to export data, if None will be returned in console', required=False)
    parser.add_argument('--export_type', '-et', type=str, choices=['json', 'xlsx'], default="xlsx", help='type method to export data, default "xlsx"', required=False)

    args = vars(parser.parse_args())

    from_date = datetime.strptime(args["from_date"], "%d/%m/%Y")
    to_date = datetime.strptime(args["to_date"], "%d/%m/%Y")

    date_range = [((from_date + timedelta(days=i*6)).strftime("%d/%m/%Y"), min(from_date + timedelta(days=(i+1)* 6 - 1), to_date).strftime("%d/%m/%Y"))
                    for i in range((to_date - from_date).days // 6 + 1)]

    sisreg = Sisreg(args["username"], args["password"])
    units = sisreg.get_schedule_unit(args["unit"])

    cpu_frequency = psutil.cpu_freq().current / 1000
    max_cpu_capacity = os.cpu_count() * psutil.cpu_count(logical=True) * psutil.cpu_freq().current / 1000

    threads = int(max_cpu_capacity / cpu_frequency)

    with tqdm(total=len(units), ascii=' ━', colour='GREEN', dynamic_ncols=True, unit="unit",
              desc=f"get workers from unit(s) (threads: {threads})", postfix={"workers": "0"}, leave=False) as pbar:

        unit_futures_map = {}
        with concurrent.futures.ThreadPoolExecutor(threads) as executor:
            futures = {executor.submit(sisreg.get_workers_from_schedule_unit, unit): unit for unit in units}
            units.clear()
            unit_futures_map.update(futures)
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                unit_param = unit_futures_map[future]
                units.extend(result)
                pbar.update(1), pbar.set_postfix(workers=len(units))

    with tqdm(total=len(units), ascii=' ━', colour='GREEN', dynamic_ncols=True, unit="unit",
              desc=f"get workers methods (threads: {threads})", postfix={"unit": ""}, leave=False) as pbar:

        unit_futures_map = {}
        with concurrent.futures.ThreadPoolExecutor(threads) as executor:
            futures = {executor.submit(sisreg.get_worker_methods_from_schedule_unit, worker): worker for worker in units}
            units.clear()
            unit_futures_map.update(futures)
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                worker_param = unit_futures_map[future]
                units.extend([{**data, 'from_date': from_date_str, 'to_date': to_date_str}
                              for from_date_str, to_date_str in date_range
                              for data in result])

                pbar.update(1), pbar.set_postfix(unit=f"{worker_param['unit'][:20]}{'+' if len(worker_param['unit']) > 20 else ''}")

    with open(os.path.join("resources", "relatory_flags.json"), "r") as file:
        flags = json.loads(file.read())

    with tqdm(total=len(units), ascii=' ━', colour='GREEN', dynamic_ncols=True, unit="data",
              desc=f"get method data (threads: {threads})", postfix={"unit": ""}, leave=False) as pbar:

        unit_futures_map = {}
        with concurrent.futures.ThreadPoolExecutor(threads) as executor:
            futures = {executor.submit(sisreg.get_worker_schedule_relatory, method, **flags): method for method in units}
            units.clear()
            unit_futures_map.update(futures)
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                method_param = unit_futures_map[future]
                units.extend(result)
                pbar.update(1), pbar.set_postfix(unit=f"{method_param['unit'][:20]}{'+' if len(method_param['unit']) > 20 else ''}")

    with open(os.path.join("resources", "unit_address.json"), "r") as file:
        addresses = json.loads(file.read())

    def fix_wrong_lines(row: pd.Series, wrong_lines: pd.DataFrame, to_fix: pd.DataFrame) -> pd.Series:
        if row.name in wrong_lines.index:
            previous_index = to_fix[to_fix.index < row.name].index.max()
            method = next(value for value in row if isinstance(value, str) and re.search(r'^(\d+) - ', value))
            row[:] = df.loc[previous_index]
            row[["Procedimento", "Procedimento.1"]] = method
        return row

    df = pd.DataFrame(units, dtype=str)
    wrong_lines = df[df.apply(lambda row: any(pd.to_numeric(row.str.extract(r'^(\d+)\s+\-\s+', expand=False).squeeze()) > 1), axis=1)]
    to_fix = df[df.index.isin(wrong_lines.index - 1) & df.apply(lambda row: row.str.contains(r'^01\s+\-\s+').any(), axis=1)]
    df = df.apply(fix_wrong_lines, wrong_lines=wrong_lines, to_fix=to_fix, axis=1)
    df['Data/Hora'] = df['Data/Hora'].apply(lambda x: datetime.strptime(re.search(r"(\d{1,2}\/\d{1,2}\/\d{4}\s+\d{2}\:\d{2})", x).group(1), "%d/%m/%Y %H:%M") if pd.notnull(x) else None)
    df[['Data', 'Hora']] = df['Data/Hora'].apply(lambda x: pd.Series([x.strftime("%d/%m/%Y"), x.strftime("%H:%M")]) if pd.notnull(x) else pd.Series([None, None]))
    df['Endereco'] = df['Unidade'].apply(lambda x: addresses.get(x, ""))
    df = df.map(lambda x: re.sub(r"^\d+\s+\-\s+", '', x) if isinstance(x, str) else x)
    df.replace("---", None, inplace=True)
    df.sort_values(by=['Data/Hora'], ascending=True, inplace=True)

    if args["columns"]:
        valid_columns = df.columns.to_list()
        invalid_columns = [column for column in args["columns"] if column not in valid_columns]
        if invalid_columns:
            raise ValueError(f"Invalid columns: {', '.join(invalid for invalid in invalid_columns)}.\n See valid columns: {', '.join(valid for valid in valid_columns)}.")
        df = df[args["columns"]]

    if args.get("banlist"):
        regex = re.compile(r'|'.join(re.escape(value) for value in args.get("banlist")))
        df = df[~df['Procedimento'].str.contains(regex, na=False)]

    if args["export_type"] == "xlsx":
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
            df.to_excel(temp_file.name, index=False)

            with open(temp_file.name, "rb") as file:
                data = file.read()
        os.remove(temp_file.name)

    elif args["export_type"] == "json":
        data = df.to_dict(orient="records")
        data = json.dumps(data)

    if args["export_path"] is not None:
        with open(args["export_path"], "wb") as file:
            file.write(data)
    else:
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
