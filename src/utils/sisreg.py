from collections import defaultdict
from .exceptions import LoginError
from typing import List, Dict
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
import requests
import hashlib
import re

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:127.0) Gecko/20100101 Firefox/127.0"}

class Sisreg:
    def __init__(self, user: str, password: str) -> None:
        self.__user = self.__str_validator(user)
        self.__password = self.__str_validator(password)
        self.__cookies = self.__login()


    def __str_validator(self, string: str) -> str:
        if not isinstance(string, str):
            raise TypeError("string expected")

        return string

    def __login(self) -> None:
        session = requests.Session()

        payload = {"usuario": self.__user.upper(),
                   "senha": "",
                   "senha_256": hashlib.sha256(self.__password.encode('utf-8')).hexdigest(),
                   "etapa": "ACESSO",
                   "logout": ""}

        login = session.post("https://sisregiii.saude.gov.br", headers=headers, data=payload)
        soup = BeautifulSoup(login.content, "html.parser")
        has_exception = soup.find("div", {"id": "mensagem"})
        if has_exception:
            raise LoginError(has_exception.text.strip())

        return login.history[0].cookies.get_dict()

    def get_schedule_unit(self, unit_name: List[str] = None, unit_id: List[str] = None) -> List[Dict[str, str]]:
        session = requests.Session()
        sched = session.get("https://sisregiii.saude.gov.br/cgi-bin/cons_agendas", headers=headers,
                    cookies=self.__cookies)

        sched = BeautifulSoup(sched.content, "html.parser")
        table = sched.find("table", {"class": "table_listagem"})
        executor_tr = next(td for td in table.find_all("tr") if re.search(r"Executante", td.text))
        unit_options = executor_tr.find_all("option")
        units = [{"name": unit.text, "id": unit["value"]} for unit in unit_options if (unit.has_attr("value") and unit["value"])]
        if unit_name or unit_id:
            filtered_units = []
            for unit in units:
                name_matches = any(re.match(name, unit["name"], flags=re.I) for name in unit_name) if unit_name else True
                id_matches = any(re.match(id_, unit["id"]) for id_ in unit_id) if unit_id else True
                if name_matches and id_matches:
                    filtered_units.append(unit)
            return filtered_units

        return units

    def get_workers_from_schedule_unit(self, unity_id) -> Dict[str, str]:
        session = requests.Session()
        params = {"BUSCA": "PROFISSIONAIS_POR_UPS", "AJAX_UPS": unity_id}
        workers = session.get("https://sisregiii.saude.gov.br/cgi-bin/sisreg_ajax", params=params,
                                    cookies=self.__cookies)

        workers = BeautifulSoup(workers.content, "xml")
        workers = [{"name": worker.text, "id": worker["codigo"]} for worker in workers.find_all("ROW") if (worker.has_attr("codigo") and worker["codigo"])]
        return workers

    def get_worker_methods_from_schedule_unit(self, unit_id: str, worker_id: str):
        session = requests.Session()
        params = {"BUSCA": "PROCEDIMENTOS_POR_PROFISSIONAIS_E_UPS", "AJAX_UPS": unit_id,
                  "AJAX_CPF": worker_id}

        methods = session.get("https://sisregiii.saude.gov.br/cgi-bin/sisreg_ajax", params=params,
                                    cookies=self.__cookies)

        methods = BeautifulSoup(methods.content, "xml")
        methods = [{"name": method.text, "id": method["codigo"]} for method in methods.find_all("ROW") if (method.has_attr("codigo") and method["codigo"])]
        return methods

    def get_worker_schedule_relatory(self, from_date: str, to_date: str, unit_id: str, worker_id: str, method_id: str, **flags) -> Dict[str, str]:

        valid_params = {"chkboxExibirProcedimentos": r'^(on|off)$',
                        "chkboxExibirTelefones": r'^(on|off)$',
                        "chkboxListaImpressao": r'^(on|off)$'}

        invalid_keys = flags.keys() - valid_params.keys()
        if invalid_keys:
            raise ValueError(f"invalid flags key names: {', '.join(invalid_keys)}")

        invalid_params = {key: type(value) for key, value in flags.items() if not isinstance(value, str)}
        if invalid_params:
            raise ValueError(f"invalid flags value types: {', '.join(f'{key}: {value}' for key, value in invalid_params.items())}")

        invalid_formats = {key: value for key, value in flags.items() if not re.match(valid_params[key], value)}
        if invalid_formats:
            raise ValueError(f"invalid flags value formats: {', '.join(f'{key}: {value}' for key, value in invalid_formats.items())}")


        payload = {"co_solicitacao": "",
                  "cns_paciente": "",
                  "dataInicial": from_date,
                  "dataFinal": to_date,
                  "ups": unit_id,
                  "cpf": worker_id,
                  "pa": method_id,
                  "cmbTipoOperacao": "Consulta",
                  "etapa": "ListaImpressao"}

        payload.update(flags)

        session = requests.Session()
        relatory = session.post("https://sisregiii.saude.gov.br/cgi-bin/cons_agendas", headers=headers, data=payload,
                                cookies=self.__cookies)

        def split_and_expand_phone_numbers(row):
            phone_numbers = re.findall(r"\(\d{2}\)\s+\d{4,5}\-\d{4}", row['Telefone(s)'] if isinstance(row['Telefone(s)'], str) else '')
            if len(phone_numbers) > 1:
                new_lines = []
                for phone_number in phone_numbers:
                    new_line = row.copy()
                    new_line['Telefone(s)'] = phone_number
                    new_lines.append(new_line)
                return pd.DataFrame(new_lines)

            return row.to_frame().T

        soup = BeautifulSoup(relatory.content, "html.parser")
        data_div = soup.find("div", {"id": "divImpressaoAgenda"})
        table = data_div.find("table", {"id": "tblImpressao"})
        if not table:
            return {}

        df = pd.read_html(StringIO(table.prettify()), header=0, converters=defaultdict(lambda: str))[0]
        df = df.apply(split_and_expand_phone_numbers, axis=1).reset_index(drop=True)
        df = pd.concat(df.tolist(), ignore_index=True)
        return df.to_dict(orient="records")
