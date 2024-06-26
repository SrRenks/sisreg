from datetime import datetime, timedelta
from src.utils import Sisreg
import concurrent.futures
from tqdm import tqdm
import pandas as pd
import argparse
import tempfile
import json
import sys
import os
import re

if __name__ == '__main__':
    today = datetime.today().strftime("%d/%m/%Y")
    five_after = (datetime.today() + timedelta(5)).strftime("%d/%m/%Y")

    parser = argparse.ArgumentParser(description='Sisreg class arg parser')
    parser.add_argument('--username', '-u', type=str, help='Login Username', required=True)
    parser.add_argument('--password', '-p', type=str, help='Login Password', required=True)
    parser.add_argument('--unit', '-ut', type=str, help='units', nargs='+', required=False)
    parser.add_argument('--from_date', '-f', type=str, help='initial range', default=today, required=False)
    parser.add_argument('--to_date', '-t', type=str, help='final range', default=five_after, required=False)
    parser.add_argument('--columns', '-c', type=str, help='selected columns', nargs='+', default=None, required=False)
    parser.add_argument('--export_path', '-ep', type=str, default=None, help='path to export data, if None will be returned in console', required=False)
    parser.add_argument('--export_type', '-et', type=str, choices=['json', 'xlsx'], default="xlsx", help='type method to export data, default "xlsx"', required=False)

    args = vars(parser.parse_args())
    sisreg = Sisreg(args["username"], args["password"])
    units = sisreg.get_schedule_unit(args["unit"])

    with tqdm(total=len(units), ascii=' ━', colour='GREEN', dynamic_ncols=True, unit="unit",
              desc="get workers from unit(s)", postfix={"workers": "0"}) as pbar:

        unit_futures_map = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(sisreg.get_workers_from_schedule_unit, unit["id"]): unit for unit in units}
            unit_futures_map.update(futures)

            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                unit_param = unit_futures_map[future]
                [unit.update({"workers": result}) for unit in units if unit == unit_param]
                pbar.update(1), pbar.set_postfix(workers=sum(len(unit.get("workers", [])) for unit in units))

    with tqdm(total=sum(len(unit["workers"]) for unit in units), ascii=' ━', colour='GREEN', dynamic_ncols=True, unit="unit",
              desc="get workers methods", postfix={"unit": ""}) as pbar:
        unit_futures_map = {}
        for unit in units:
            workers = unit["workers"]
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(sisreg.get_worker_methods_from_schedule_unit, unit["id"], worker["id"]): worker for worker in workers}
                unit_futures_map.update(futures)

                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    worker_param = unit_futures_map[future]
                    unit["workers"] = workers
                    [worker.update({"methods": result}) for worker in workers if worker == worker_param]
                    pbar.update(1), pbar.set_postfix(unit=unit["name"])

    with open(os.path.join("resources", "relatory_flags.json"), "r") as file:
        flags = json.loads(file.read())

    with tqdm(total=sum(len(worker["methods"]) for unit in units for worker in unit["workers"]), ascii=' ━', colour='GREEN',
              dynamic_ncols=True, unit="unit", desc="get workers method relatorys", postfix={"unit": ""}) as pbar:

        unit_futures_map = {}
        for unit in units:
            workers = unit["workers"]
            for worker in workers:
                methods = worker["methods"]
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = {executor.submit(sisreg.get_worker_schedule_relatory, args["from_date"], args["to_date"],
                                               unit["id"], worker["id"], method["id"], **flags): method for method in methods}
                    unit_futures_map.update(futures)

                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        method_param = unit_futures_map[future]
                        [method.update({"relatory": result}) for method in methods if method == method_param]
                        pbar.update(1), pbar.set_postfix(unit=unit["name"])

    with open(os.path.join("resources", "unit_address.json"), "r") as file:
        addresses = json.loads(file.read())

    dfs = []
    for unit in units:
        for worker in unit["workers"]:
            for method in worker["methods"]:
                df = pd.DataFrame(method["relatory"], dtype=str)
                if not df.empty:
                    df[["Unidade Executante", "Endereco Unidade", "Profissional"]] = unit["name"], addresses.get(unit["name"], ""), worker["name"]
                    df[['Data', 'Hora']] = df['Data/Hora'].apply(lambda x: pd.Series(re.search(r'(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})', x).groups()) if pd.notnull(x) else pd.Series(["", ""]))
                    df.drop("Data/Hora", axis=1, inplace=True)
                    df.replace("---", None, inplace=True)
                    dfs.append(df)

    df = pd.concat(dfs)
    if args["columns"]:
        valid_columns = df.columns.to_list()
        invalid_columns = [column for column in args["columns"] if column not in valid_columns]
        if invalid_columns:
            raise ValueError(f"Invalid columns: {', '.join(invalid for invalid in invalid_columns)}.\n See valid columns: {', '.join(valid for valid in valid_columns)}.")
        df = df[args["columns"]]

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
        sys.exit(data)
