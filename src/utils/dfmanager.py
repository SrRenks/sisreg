from typing import Dict, List, Union
from datetime import datetime
import pandas as pd
import json
import re
import os

def get_unit_addresses(path: str):
    with open(path, "r") as file:
        return json.loads(file.read())

def get_method_dataframe(method: Dict[str, str]) -> pd.DataFrame:
    df = pd.DataFrame(method["relatory"], dtype=str)
    if not df.empty:
        filter = df.apply(lambda row: any(pd.to_numeric(row.str.extract(r'^(\d+)\s+\-\s+', expand=False)) > 1), axis=1)
        filtered_df = df[filter]

        for index in filtered_df.index:
            if index - 1 >= 0:
                if df.loc[index - 1].str.contains(r'^01\s+\-\s+', regex=True).any():
                    value = next(value for value in filtered_df.loc[index].values if isinstance(value, str) and re.search(r'^(\d+) - ', value))
                    df.loc[index] = df.loc[index - 1]
                    df.loc[index - 1, ["Procedimento", "Procedimento.1"]] = value, value

        df["method_id"] = method["id"]
        df['Data/Hora'] = df['Data/Hora'].apply(lambda x: datetime.strptime(re.search(r"(\d{1,2}\/\d{1,2}\/\d{4}\s+\d{2}\:\d{2})", x).group(1), "%d/%m/%Y %H:%M") if pd.notnull(x) else None)
        df[['Data', 'Hora']] = df['Data/Hora'].apply(lambda x: pd.Series([x.strftime("%d/%m/%Y"), x.strftime("%H:%M")]) if pd.notnull(x) else pd.Series([None, None]))
        df.replace("---", None, inplace=True)
        return df
    return pd.DataFrame()

def get_worker_dataframe(worker: Dict[str, Union[str, str]]):
    df = pd.concat([get_method_dataframe(method) for method in worker["methods"]])
    df[["Profissional", "worker_id"]] = worker["name"], worker["id"]
    return df

def get_unit_dataframe(unit: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
    df = pd.concat([get_worker_dataframe(worker) for worker in unit["workers"]])
    unit_addresses = get_unit_addresses(os.path.join("resources", "unit_address.json"))
    df[["Unidade Executante", "Endereco Unidade", "unit_id"]] = unit["name"], unit_addresses.get(unit["name"], ""), unit["id"]
    return df
