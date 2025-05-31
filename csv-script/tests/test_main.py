import asyncio
import json
from datetime import datetime, timezone
import pytest

from unittest.mock import patch

from main import (
    read_data_from_data_files,
    check_unique_element_in_process_list,
    process_raw_data_table_to_dict,
    process_raw_table_to_processed_table,
    print_table
                  )


def test_wrong_data_file_name(capsys):
    with patch('sys.argv', ['main.py', 'foo']):
        print('Valid only csv files to process')
        captured = capsys.readouterr()
    assert captured.out == 'Valid only csv files to process\n'


def test_wrong_report_args_name(capsys):
    with patch('sys.argv', [
        'main.py', 'csv-script/tests/example.csv', '--report', 'foo'
    ]):
        print("usage: main.py [-h] "
              "[--report {payout,average-rate}] csv_files [csv_files ...]\n"
              "main.py: error: argument --report: "
              "invalid choice: 'foo' (choose from payout, average-rate)")
        captured = capsys.readouterr()
    assert (captured.out == "usage: main.py [-h] "
                            "[--report {payout,average-rate}] "
                            "csv_files [csv_files ...]\n"
                            "main.py: error: argument --report: "
                            "invalid choice: 'foo' "
                            "(choose from payout, average-rate)\n")


def test_read_data_from_data_files():
    raw_table = asyncio.run(
        read_data_from_data_files(['csv-script/tests/example.csv'])
    )
    assert raw_table == [
        ['id', 'email', 'department', 'name', 'hours', 'rate'],
        [101, 'grace@example.com', 'HR', 'Grace Lee', 160, 45],
        [102, 'bob@example.com', 'Marketing', 'Bob Dylan', 240, 32],
        [103, 'john@example.com', 'Marketing', 'John Dylan', 190, 45]
    ]


def test_wrong_data_file_path_read():
    raw_table = asyncio.run(
        read_data_from_data_files(['csv-script/tests/no_way.csv'])
    )
    assert raw_table is None


def test_wrong_header_from_data_file():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/wrong_header.csv'])
    )
    assert raw_table is None


def test_wrong_id_from_data_files():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/wrong_id.csv'])
    )
    assert raw_table is None


def test_wrong_email_from_data_files():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/wrong_email.csv'])
    )
    assert raw_table is None


def test_wrong_name_from_data_files():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/wrong_name.csv'])
    )
    assert raw_table is None


def test_wrong_hour_from_data_files():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/wrong_hours.csv'])
    )
    assert raw_table is None


def test_wrong_rate_from_data_files():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/wrong_rate.csv'])
    )
    assert raw_table is None


def test_no_negative_id():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/negative_id.csv'])
    )
    assert raw_table is None


def test_no_negative_hours():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/negative_hours.csv'])
    )
    assert raw_table is None


def test_no_negative_rate():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/negative_rate.csv'])
    )
    assert raw_table is None


def test_to_many_columns():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/wrong_number_column.csv'])
    )
    assert raw_table is None


def test_not_enough_columns():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/not_enough_column.csv'])
    )
    assert raw_table is None


def test_wrong_number_of_elements_in_line():
    raw_table = asyncio.run(read_data_from_data_files(
        ['csv-script/tests/wrong_line.csv'])
    )
    assert raw_table is None


def test_check_unique_elements_in_process_list():
    process_list = [['wall', 'floor', 'stone'], ['wall', 'big', 2]]
    element_count = check_unique_element_in_process_list(process_list,
                                                         'wall',
                                                         0)
    assert element_count == 2


def test_process_raw_data_table():
    raw_table = asyncio.run(
        read_data_from_data_files(['csv-script/tests/example.csv'])
    )
    data_dict_operation = asyncio.run(
        process_raw_data_table_to_dict(raw_table, 'payout')
    )
    data_dict = {'payout': [
        {'department': 'HR',
         'staff': [
             {'id': 101,
              'email': 'grace@example.com',
              'name': 'Grace Lee',
              'hours': 160, 'rate': 45.0}]},
        {'department': 'Marketing',
         'staff': [
             {'id': 102,
              'email': 'bob@example.com',
              'name': 'Bob Dylan',
              'hours': 240, 'rate': 32.0},
             {'id': 103,
              'email': 'john@example.com',
              'name': 'John Dylan',
              'hours': 190, 'rate': 45.0}]}]}
    assert data_dict_operation == data_dict


def test_no_operation_in_process_raw_data():
    raw_table = asyncio.run(
        read_data_from_data_files(['csv-script/tests/example.csv'])
    )
    data_dict_operation = asyncio.run(
        process_raw_data_table_to_dict(raw_table)
    )
    data_dict = {'raw_table': [
        {
            'id': 101,
            'email': 'grace@example.com',
            'department': 'HR',
            'name': 'Grace Lee',
            'hours': 160,
            'rate': 45
         },
        {
            'id': 102,
            'email': 'bob@example.com',
            'department': 'Marketing',
            'name': 'Bob Dylan',
            'hours': 240,
            'rate': 32
        },
        {
            'id': 103,
            'email': 'john@example.com',
            'department': 'Marketing',
            'name': 'John Dylan',
            'hours': 190,
            'rate': 45
        }
    ]
    }
    assert data_dict_operation == data_dict


def read_json_file(data: str):
    with open(f'{data}', mode='r') as file:
        json_data = json.load(file)
    return json_data


date_time_format = "%Y_%m_%d"


@pytest.mark.asyncio
async def test_read_processed_json():
    raw_table = await read_data_from_data_files(
        ['csv-script/tests/example.csv']
    )
    await process_raw_table_to_processed_table(raw_table, 'payout')
    create_date = datetime.now(timezone.utc).strftime(date_time_format)
    data_file = f'payout_{create_date}.json'
    read_data = read_json_file(data_file)
    data = {'payout': [
        {'department': 'HR',
         'staff': [
             {'id': 101,
              'email': 'grace@example.com',
              'name': 'Grace Lee',
              'hours': 160, 'rate': 45.0,
              'payout': 7200.0}],
         'hours': 160, 'payout': 7200.0},
        {'department': 'Marketing',
         'staff': [
             {'id': 102,
              'email': 'bob@example.com',
              'name': 'Bob Dylan',
              'hours': 240, 'rate': 32.0,
              'payout': 7680},
             {'id': 103,
              'email': 'john@example.com',
              'name': 'John Dylan',
              'hours': 190, 'rate': 45.0,
              'payout': 8550}],
         'hours': 430, 'payout': 16230}]}
    assert read_data == data


@pytest.mark.asyncio
async def test_get_processed_table():
    raw_table = await read_data_from_data_files(
        ['csv-script/tests/example.csv']
    )
    processed_table = await process_raw_table_to_processed_table(
        raw_table, 'payout'
    )
    data = [
        ['id', 'email', 'department', 'name', 'hours', 'rate', 'payout'],
        [101, 'grace@example.com', 'HR', 'Grace Lee', 160, 45.0,
         7200.0],
        ['', '', '', '', 160, '', 7200.0],
        [102, 'bob@example.com', 'Marketing', 'Bob Dylan', 240, 32.0,
         7680.0],
        [103, 'john@example.com', '---------', 'John Dylan', 190, 45.0,
         8550.0],
        ['', '', '', '', 430, '', 16230.0]
    ]
    assert processed_table == data


@pytest.mark.asyncio
async def test_print_table(capsys):
    raw_table = await read_data_from_data_files(
        ['csv-script/tests/example.csv']
    )
    proc_table = await process_raw_table_to_processed_table(
        raw_table, None
    )
    print_table(proc_table)
    captured = capsys.readouterr()
    assert captured.out == ("| id  | email             |"
                            " department | name       | hours | rate |\n"
                            "| 101 | grace@example.com |"
                            " HR         | Grace Lee  | 160   | 45.0 |\n"
                            "| 102 | bob@example.com   |"
                            " Marketing  | Bob Dylan  | 240   | 32.0 |\n"
                            "| 103 | john@example.com  |"
                            " Marketing  | John Dylan | 190   | 45.0 |\n")


@pytest.mark.asyncio
async def test_print_operated_table(capsys):
    raw_table = await read_data_from_data_files(
        ['csv-script/tests/example.csv']
    )
    proc_table = await process_raw_table_to_processed_table(
        raw_table, 'payout'
    )
    print_table(proc_table)
    captured = capsys.readouterr()
    assert captured.out == ("| id  | email             | department "
                            "| name       | hours | rate | payout  |\n"
                            "| 101 | grace@example.com "
                            "| HR         | Grace Lee  | 160   | 45.0 "
                            "| 7200.0  |\n"
                            "|     |                   "
                            "|            |            | 160   |      "
                            "| 7200.0  |\n"
                            "| 102 | bob@example.com   "
                            "| Marketing  | Bob Dylan  | 240   | 32.0 "
                            "| 7680.0  |\n"
                            "| 103 | john@example.com  "
                            "| ---------  | John Dylan | 190   | 45.0 "
                            "| 8550.0  |\n"
                            "|     |                   "
                            "|            |            | 430   |      "
                            "| 16230.0 |\n")
