import os
from typing import Any
from datetime import datetime, timezone
import asyncio
import argparse
from enum import Enum
import json
import re

date_time_format = "%Y_%m_%d"


class Command(Enum):
    report = '--report'
    help = '-h'
    show_operation_list = '-list'


class Operation(Enum):
    payout = 'payout'
    average_rate = 'average-rate'


operation_list = [operation.value for operation in Operation]


class NameHeader(Enum):
    name = 'name'
    staff = 'staff'
    full_name = 'full_name'
    staff_name = 'staff_name'
    person = 'person'


name_list = [name.value for name in NameHeader]

name_regex = r'^[^()[\]{}*&^%$#@!\d]+$'


class EmailHeader(Enum):
    e_mail = 'e-mail'
    e_mail_two = 'e_mail'
    email = 'email'
    address = 'address'


email_list = [e_mail.value for e_mail in EmailHeader]

email_regex = r'[^@]+@[^@]+\.[^@]+'


class DepartmentHeader(Enum):
    department = 'department'
    devision = 'devision'
    direction = 'direction'


department_list = [department.value for department in DepartmentHeader]


class HoursHeader(Enum):
    hours_worked = 'hours_worked'
    hours = 'hours'
    worked = 'worked'


hours_list = [hour.value for hour in HoursHeader]


class RateHeader(Enum):
    rate = 'rate'
    hourly_rate = 'hourly_rate'
    payment = 'payment'
    pay = 'pay'
    salary = 'salary'
    cash = 'cash'


rate_list = [rate.value for rate in RateHeader]


class IdHeader(Enum):
    id = 'id'
    number = '#'
    another_number = 'N'
    small_number = 'n'
    formal_number = '№'


id_point_list = [id_point.value for id_point in IdHeader]


async def read_data_from_data_files(
        data_files: list[str] | None = None
) -> list | None:
    """
    The table should content following header:
    ID of employees.
    Email address of employees.
    Department occupation of employees.
    Name or full name of employees.
    Worked hours of each employee.
    Hourly rate of each employee.
    Please, correct your table to that headers.
    """
    if data_files is None:
        return
    raw_table = []
    header_row = ['id', 'email', 'department', 'name', 'hours', 'rate']
    raw_table.append(header_row)
    for data_file in data_files:
        try:
            with open(data_file, mode='r') as file:
                file_address = os.path.abspath(data_file)
                headers = file.readline()
                headers = headers.replace('\n', '')
                headers = headers.split(',')
                if len(headers) != len(header_row):
                    return print(read_data_from_data_files.__doc__)
                check_set = set()
                for header in headers:
                    if header in id_point_list:
                        id_index = headers.index(header)
                        id_header = 'id'
                        check_set.add(id_header)
                    elif header in email_list:
                        email_index = headers.index(header)
                        email_header = 'email'
                        check_set.add(email_header)
                    elif header in department_list:
                        department_index = headers.index(header)
                        dep_header = 'department'
                        check_set.add(dep_header)
                    elif header in name_list:
                        name_index = headers.index(header)
                        name_header = 'name'
                        check_set.add(name_header)
                    elif header in hours_list:
                        hours_index = headers.index(header)
                        hour_header = 'hour'
                        check_set.add(hour_header)
                    elif header in rate_list:
                        rate_index = headers.index(header)
                        rate_header = 'rate'
                        check_set.add(rate_header)
                    else:
                        return print(read_data_from_data_files.__doc__)
                if len(headers) != len(check_set):
                    return print(read_data_from_data_files.__doc__)
                data_rows = file.readlines()
                for data_row in data_rows:
                    data_row_number = data_row.index(data_row) + 1
                    raw_table_new_line = []
                    data_row = data_row.replace('\n', '')
                    data_row = data_row.split(',')
                    if len(data_row) != len(header_row):
                        return print(f"Data file {file_address} "
                                     f"row number {data_row_number} "
                                     f"content incorrect number of columns.")
                    try:
                        int(data_row[id_index])
                    except ValueError:
                        return print(f"{file_address} Id column row "
                                     f"{data_row_number} "
                                     f"should content integer")
                    if int(data_row[id_index]) < 0:
                        return print(f'{file_address} '
                                     f'Id column '
                                     f'row {data_row_number} '
                                     f'should content positive integer')
                    if not re.match(email_regex, data_row[email_index]):
                        return print(f"{file_address} row {data_row_number}"
                                     f" Incorrect email address in column")
                    if not re.match(name_regex, data_row[name_index]):
                        return print(f'{file_address} line {data_row_number}'
                                     'Incorrect staff name in column.'
                                     'Please, insert only letters '
                                     'in this column')
                    try:
                        int(data_row[hours_index])
                    except ValueError:
                        return print(f'{file_address} line {data_row_number}'
                                     'Hours column should content integer')
                    if int(data_row[hours_index]) < 0:
                        return print(f"{file_address} line {data_row_number}"
                                     "Hours can't be negative number")
                    try:
                        float(data_row[rate_index])
                    except ValueError:
                        return print(f'{file_address} line {data_row_number}'
                                     'Rate colum should content number')
                    if float(data_row[rate_index]) <= 0:
                        return print(f'{file_address} line {data_row_number}'
                                     'Rate should be more than null')
                    raw_table_new_line.append(int(data_row[id_index]))
                    raw_table_new_line.append(data_row[email_index])
                    raw_table_new_line.append(data_row[department_index])
                    raw_table_new_line.append(data_row[name_index])
                    raw_table_new_line.append(int(data_row[hours_index]))
                    raw_table_new_line.append(
                        round(float(data_row[rate_index]), 2)
                    )
                    raw_table.append(raw_table_new_line)
        except FileNotFoundError:
            return print(f"{os.path.abspath(data_file)} not found.")
    return raw_table


def check_unique_element_in_process_list(
        process_list: list,
        element: Any,
        index: int
) -> int:
    elements_list = []
    for raw in range(len(process_list)):
        elements_list.append(process_list[raw][index])
    element_count = elements_list.count(element)
    return element_count


async def process_raw_data_table_to_dict(
        raw_table: list,
        operation: str | None = None
) -> dict | None:
    if operation is None:
        dict_list = []
        raw_table.pop(0)
        for row in range(len(raw_table)):
            data_dict = dict(id=raw_table[row][0],
                             email=raw_table[row][1],
                             department=raw_table[row][2],
                             name=raw_table[row][3],
                             hours=raw_table[row][4],
                             rate=raw_table[row][5])
            dict_list.append(data_dict)
        process_dict = {'raw_table': dict_list}
    else:
        process_table = []
        department_dict_list = []
        process_dict = {operation: department_dict_list}
        headers_row = raw_table.pop(0)
        process_table.append(headers_row)
        for row in range(len(raw_table)):
            blank_list = []
            id_name = raw_table[row][0]
            blank_list.append(id_name)
            e_mail = raw_table[row][1]
            check_email = check_unique_element_in_process_list(process_table,
                                                               e_mail,
                                                               1)
            blank_list.append(e_mail)
            department = raw_table[row][2]
            check_department = check_unique_element_in_process_list(
                process_table,
                department,
                2
            )
            blank_list.append(department)
            name = raw_table[row][3]
            blank_list.append(name)
            hours = raw_table[row][4]
            blank_list.append(hours)
            rate = raw_table[row][5]
            blank_list.append(rate)
            staff = []
            if check_department == 0:
                department_dict_list.append(dict(department=department,
                                                 staff=staff))
                staff.append(dict(id=id_name,
                                  email=e_mail,
                                  name=name,
                                  hours=hours,
                                  rate=rate))
            else:
                if check_email == 0:
                    for i in range(len(department_dict_list)):
                        if department_dict_list[i]['department'] == department:
                            department_dict_list[i]['staff'].append(dict(
                                id=id_name,
                                email=e_mail,
                                name=name,
                                hours=hours,
                                rate=rate
                            ))
                else:
                    for i in range(len(department_dict_list)):
                        if department_dict_list[i]['department']\
                                == department:
                            for e in range(
                                    len(department_dict_list[i]['staff'])
                            ):
                                employee = department_dict_list[i]['staff']
                                if employee[e]['email']\
                                        == e_mail:
                                    if employee[e]['rate']\
                                            != rate:
                                        employee.append(
                                            dict(id=id_name,
                                                 email=e_mail,
                                                 name=name,
                                                 hours=hours,
                                                 rate=rate)
                                        )
                                    else:
                                        employee[e]['hours'] += hours
            process_table.append(blank_list)
    return process_dict


async def save_data_to_file(data: Any,
                            operation: str | None) -> None:
    today = datetime.now(timezone.utc).strftime(date_time_format)
    if operation is None:
        with open(
                f'raw_{today}.json', mode='w'
        ) as file:
            json.dump(data, file)
    else:
        with open(
                f'{operation}_{today}.json', mode='w'
        ) as file:
            json.dump(data, file)


async def process_raw_table_to_processed_table(raw_table: list,
                                               operation: str | None) -> list:
    to_dict_table = raw_table.copy()
    operation_dict = await process_raw_data_table_to_dict(
        to_dict_table, operation
    )
    if operation is None:
        pass
    else:
        oper_dep_dict_list = operation_dict[operation]
    headers = raw_table[0]
    if operation is None:
        pass
    if operation == Operation.payout.value:
        headers.append(operation)
    if operation == Operation.average_rate.value:
        headers = [*headers, 'payout', operation]
    processed_table = [headers]
    if operation is None:
        raw_table.pop(0)
        for row in raw_table:
            processed_table.append(row)
    else:
        for i in range(len(oper_dep_dict_list)):
            department = oper_dep_dict_list[i]['department']
            depart_line = 0
            while raw_table[depart_line][2] != department:
                depart_line += 1
            # Блоки кода с условиями сделаны таким образом,
            # Чтобы можно было добавить новое условие для нового типа отчета
            # Где могут быть произведены другие расчеты между столбцами
            if operation == Operation.payout.value:
                hours = 0
                payout = 0
                hours += raw_table[depart_line][4]
                payout_first = \
                    raw_table[depart_line][4] * raw_table[depart_line][5]
                payout += payout_first
                for s in range(len(oper_dep_dict_list[i]['staff'])):
                    if oper_dep_dict_list[i]['staff'][s]['email'] \
                            == raw_table[depart_line][1]:
                        oper_dep_dict_list[i]['staff'][s]. \
                            update({'payout': payout_first})
                first_depart_line = [
                    *raw_table[depart_line], payout_first
                ]
            if operation == Operation.average_rate.value:
                hours = 0
                payout = 0
                hours += raw_table[depart_line][4]
                payout_first = \
                    raw_table[depart_line][4] * raw_table[depart_line][5]
                payout += payout_first
                for s in range(len(oper_dep_dict_list[i]['staff'])):
                    if oper_dep_dict_list[i]['staff'][s]['email'] \
                            == raw_table[depart_line][1]:
                        oper_dep_dict_list[i]['staff'][s]. \
                            update({'payout': payout_first})
                first_depart_line = [*raw_table[depart_line], payout_first, '']
            processed_table.append(first_depart_line)
            raw_table.pop(depart_line)
            for row in range(len(raw_table)):
                if raw_table[row][2] == department:
                    raw_table[row][2] = '-' * len(department)
                    if operation == Operation.payout.value:
                        hours += raw_table[row][4]
                        payout_other = raw_table[row][4] * raw_table[row][5]
                        payout += payout_other
                        for s in range(
                                len(operation_dict[operation][i]['staff'])
                        ):
                            if oper_dep_dict_list[i]['staff'][s]['email']\
                                    == raw_table[row][1]:
                                oper_dep_dict_list[i]['staff'][s].\
                                    update({'payout': payout_other})
                        other_depart_line = [*raw_table[row], payout_other]
                    if operation == Operation.average_rate.value:
                        hours += raw_table[row][4]
                        payout_other = raw_table[row][4] * raw_table[row][5]
                        payout += payout_other
                        for s in range(
                                len(operation_dict[operation][i]['staff'])
                        ):
                            if oper_dep_dict_list[i]['staff'][s]['email'] \
                                    == raw_table[row][1]:
                                oper_dep_dict_list[i]['staff'][s]. \
                                    update({'payout': payout_other})
                        other_depart_line = [*raw_table[row], payout_other, '']
                    processed_table.append(other_depart_line)
            if operation == Operation.payout.value:
                total_line = ['', '', '', '', hours, '', payout]
            if operation == Operation.average_rate.value:
                average_rate = round((payout / hours), 2)
                total_line = ['', '', '', '', hours, '', payout, average_rate]
            processed_table.append(total_line)
            if operation == Operation.payout.value:
                operation_dict[operation][i].update({'hours': hours})
                operation_dict[operation][i].update({'payout': payout})
            if operation == Operation.average_rate.value:
                operation_dict[operation][i].update({'hours': hours})
                operation_dict[operation][i].update({'payout': payout})
                operation_dict[operation][i].\
                    update({'average_rate': average_rate})
    await save_data_to_file(operation_dict, operation)
    return processed_table


def print_table(table):
    for row in range(len(table)):
        for i in range(len(table[row])):
            if type(table[row][i]) is not str:
                table[row][i] = str(table[row][i])
    col_width = [max(len(x) for x in col) for col in zip(*table)]
    for line in table:
        print(
            "| " + " | ".join("{:{}}".format(x, col_width[i])
                              for i, x in enumerate(line)) + " |"
        )


async def main():
    parser = argparse.ArgumentParser(
        description="Display and process all CSV-Data files,"
                    " uploading to that script."
    )
    parser.add_argument('csv_files', type=str,
                        nargs='+',
                        help='All your csv-files, '
                             'you want to process and display. '
                             'Example: "data1.csv"')
    parser.add_argument('--report', type=str,
                        default=0,
                        choices=[report for report in operation_list],
                        help='Choose report name you want to process. '
                             'For example, "payout"')
    args = parser.parse_args()
    data_files = []
    raw_data = args.csv_files
    for data in raw_data:
        if data.endswith('.csv'):
            data_files.append(data)
        else:
            return print('Valid only csv files to process')
    operation = args.report
    if not args.report:
        operation = None
    table = await read_data_from_data_files(data_files)
    if table is None:
        return
    proc_table = await process_raw_table_to_processed_table(
        table, operation
    )
    print_table(proc_table)


if __name__ == '__main__':
    asyncio.run(main())
