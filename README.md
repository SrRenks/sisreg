# Sisreg Python Module

## Overview

The **Sisreg Python Module** facilitates data extraction and interaction with the Sistema de Regulação (Sisreg) website, used in healthcare for managing scheduling and resource allocation. This module offers functionalities to retrieve schedules, practitioner information, and related reports programmatically.

## Installation

You can install the Sisreg Python module using pip:

```bash
pip install sisreg
```
Or with git:
```
git clone "https://github.com/SrRenks/sisreg.git"
```

Make sure you have Python 3.x installed. This will also install the necessary dependencies like requests, BeautifulSoup, and Pandas, using the command below (if you used git method):
```
    pip install -r requirements.txt
```

## Initialization
```
python

from sisreg import Sisreg

# Initialize Sisreg instance with username and password
sisreg = Sisreg(user='your_username', password='your_password')
```

Example Usage
* Get Schedule for a Unit

```
unit_name = ['Hospital A']
schedule = sisreg.get_schedule_unit(unit_name=unit_name)
print(schedule)

Get Workers from a Unit

python

unit_id = '123456'
workers = sisreg.get_workers_from_schedule_unit(unit_id)
print(workers)
```
* Get Worker Methods from a Unit

```
unit_id = '123456'
worker_id = '789'
methods = sisreg.get_worker_methods_from_schedule_unit(unit_id, worker_id)
print(methods)
```
* Generate Worker Schedule Report
```
from_date = '01/01/2024'
to_date = '01/05/2024'
unit_id = '123456'
worker_id = '789'
method_id = '456'
flags = {'chkboxExibirProcedimentos': 'on'}

report = sisreg.get_worker_schedule_relatory(from_date, to_date, unit_id, worker_id, method_id, **flags)
print(report)
```
## Error Handling
```
from sisreg.exceptions import LoginError

try:
    sisreg = Sisreg(user='invalid_user', password='invalid_password')
except LoginError as e:
    print(f"Login Error: {e}")
    # Handle login error gracefully
```
Notes

    Ensure valid credentials are provided for accessing Sisreg.
    Check the Sisreg documentation for any changes in API endpoints or authentication methods.

License

This project is licensed under the MIT License - see the LICENSE file for details.