from typing import List, Dict, Callable, Optional
from .exceptions import LoginError
from itertools import product
from bs4 import BeautifulSoup
from time import sleep
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

    def __get_session(self) -> requests.Session:
        session = requests.Session()
        session.get("https://sisregiii.saude.gov.br/cgi-bin/recaptcha?cod=0", headers=headers, cookies=self.__cookies)
        return session

    def __manage_request(self, request: Callable, *payload: Optional[Dict[str, str]], retry: int = 5, wait: int = 5) -> requests.Response:

        count = 0
        while count < retry:
            try:
                response = request(*payload)
                return response

            except requests.exceptions.RequestException as error:
                count += 1
                sleep(wait)
                if count == retry:
                    raise error

    def __get_schedule_unit(self) -> requests.Response:
        return self.__manage_request(self.__get_session).get("https://sisregiii.saude.gov.br/cgi-bin/cons_agendas", headers=headers,
                    cookies=self.__cookies)

    def get_schedule_unit(self, unit_name: List[str] = None, unit_id: List[str] = None) -> List[Dict[str, str]]:
        sched = self.__manage_request(self.__get_schedule_unit)
        sched = BeautifulSoup(sched.content, "html.parser")
        table = sched.find("table", {"class": "table_listagem"})
        executor_tr = next(td for td in table.find_all("tr") if re.search(r"Executante", td.text))
        unit_options = executor_tr.find_all("option")
        units = [{"unit": unit.text, "unit_id": unit["value"]} for unit in unit_options if (unit.has_attr("value") and unit["value"])]
        if unit_name or unit_id:
            filtered_units = []
            for unit in units:
                name_matches = any(re.match(name, unit["unit"], flags=re.I) for name in unit_name) if unit_name else True
                id_matches = any(re.match(id_, unit["unit_id"]) for id_ in unit_id) if unit_id else True
                if name_matches and id_matches:
                    filtered_units.append(unit)
            return filtered_units

        return units

    def __get_worker_from_schedule_unit(self, params: Dict[str, str]) -> requests.Response:
        return self.__manage_request(self.__get_session).get("https://sisregiii.saude.gov.br/cgi-bin/sisreg_ajax", params=params,
                            cookies=self.__cookies)

    def get_workers_from_schedule_unit(self, unit_data: Dict[str, str]) -> Dict[str, str]:
        params = {"BUSCA": "PROFISSIONAIS_POR_UPS", "AJAX_UPS": unit_data["unit_id"]}
        workers = self.__manage_request(self.__get_worker_from_schedule_unit, params)
        workers = BeautifulSoup(workers.content, "xml")
        workers = [{**{"worker": worker.text, "worker_id": worker["codigo"]}, **unit_data}
                   for worker in workers.find_all("ROW") if (worker.has_attr("codigo") and worker["codigo"])]

        return workers

    def __get_worker_methods_from_schedule_unit(self, params: Dict[str, str]) -> requests.Response:
        return self.__manage_request(self.__get_session).get("https://sisregiii.saude.gov.br/cgi-bin/sisreg_ajax", params=params,
                           cookies=self.__cookies)

    def get_worker_methods_from_schedule_unit(self, worker_data: Dict[str, str]) -> Dict[str, str]:
        params = {"BUSCA": "PROCEDIMENTOS_POR_PROFISSIONAIS_E_UPS", "AJAX_UPS": worker_data["unit_id"],
                  "AJAX_CPF": worker_data["worker_id"]}

        methods = self.__manage_request(self.__get_worker_methods_from_schedule_unit, params)

        methods = BeautifulSoup(methods.content, "xml")
        methods = [{**{"method": method.text, "method_id": method["codigo"]}, **worker_data}
                   for method in methods.find_all("ROW") if (method.has_attr("codigo") and method["codigo"])]

        return methods

    def __get_worker_schedule_relatory(self, payload: Dict[str, str]) -> requests.Response:
        return self.__manage_request(self.__get_session).post("https://sisregiii.saude.gov.br/cgi-bin/cons_agendas", headers=headers, data=payload,
                                cookies=self.__cookies)

    def get_worker_schedule_relatory(self, worker_data: Dict[str, str]) -> Dict[str, str]:

        def parse_strings_to_dict_list(strings: List[str]) -> pd.DataFrame:
            data = {}
            current_key = None

            for item in strings:
                clean_item = item.strip()
                if clean_item.endswith(':'):
                    current_key = clean_item[:-1]
                    data[current_key] = []
                elif current_key and clean_item:
                    data[current_key].append(re.sub(r"\s+", " ", clean_item))

            data["Procedimento(s)"] = list(map(lambda item: item.lstrip('- ').strip(),
                                                filter(lambda item: not item.isdigit(), data.get('Procedimento(s)', []))))

            data = {key: ', '.join(value) if isinstance(value, list) else value for key, value in data.items()}
            data = [{**data, 'Procedimento(s)': method} for method in data['Procedimento(s)'].split(', ')]

            return data

        payload = {"co_solicitacao": "",
                   "cns_paciente": "",
                   "chkboxExibirProcedimentos": "on",
                   "chkboxExibirTelefones": "on",
                   "dataInicial": worker_data["from_date"],
                   "dataFinal": worker_data["to_date"],
                   "ups": worker_data["unit_id"],
                   "cpf": worker_data["worker_id"],
                   "pa": worker_data["method_id"],
                   "cmbTipoOperacao": "Consulta",
                   "cmbOrdenacao": "1",
                   "cmbMaxResults": "500",
                   "etapa": "ListaConsulta",
                   "pagina": "0",
                   "linhas": "0"}

        registry = []
        relatory = self.__manage_request(self.__get_worker_schedule_relatory, payload)
        soup = BeautifulSoup(relatory.content.decode(relatory.encoding), "html.parser")
        registry.append(soup)
        counter = soup.find(string=re.compile("Mostrando Página"))

        if counter:
            counter = counter.find_next_sibling(string=re.compile(r"de\s+\d+"))
            counter = int(re.search(r"(\d+)", counter.text.strip()).group(0))
            for count in range(2, counter + 1):
                payload["pagina"] = count
                relatory = self.__manage_request(self.__get_worker_schedule_relatory, payload)
                registry.append(BeautifulSoup(relatory.content.decode(relatory.encoding), "html.parser"))

        data = []
        for soup in registry:
            tables = soup.findAll("table", {"class": "table_listagem", "id": re.compile(r"tblConsulta\d+")})
            for table in tables:
                strings = [string for string in table.strings if string and not re.match(r"^\s+$", string)]
                strings.insert(0, "Código:")
                data.extend(parse_strings_to_dict_list(strings))

        if not data:
            return {}

        data = pd.DataFrame(data)
        data[["Unidade", "unit_id", "Profissional", "worker_id", "method_id"]] = worker_data["unit"], worker_data["unit_id"], worker_data["worker"], worker_data["worker_id"], worker_data["method_id"]
        return data.to_dict(orient="records")
