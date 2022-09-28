# -*- coding: utf-8 -*-
"""
@author:qianhangkang
@time:2022年09月26日
"""
import configparser
import csv
import multiprocessing
import os
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, wait, ALL_COMPLETED
from decimal import *

DEFAULT = 'DEFAULT'
KEY = 'key'
COLUMN = 'column'
ENCODING = 'encoding'
UP = 'up'
DOWN = 'down'
RIGHT = 'right'
LEFT = 'left'
EXPLODE = 'explode'
OUTPUT_FOLDER_NAME = 'output_folder_name'
PERFORMANCE = 'performance'
KEEP_COMPUTE_FILE = 'keep_compute_file'

USE_FLAIR = 'diamond'
CONFIG_NAME = 'config.ini'
DEFAULT_ENCODING = 'gbk'
DEFAULT_OUTPUT_FOLDER_NAME = 'output'
DEFAULT_OUTPUT_SUMMARY_NAME = 'summary.csv'
DEFAULT_PERFORMANCE = 5
DEFAULT_KEEP_COMPUTE_FILE = 0


def get_flair(flag=USE_FLAIR) -> str:
    flair = {
        "rocket": "(🚀🚀)",
        "diamond": "(💎💎💎)",
        "stars": "(✨✨)",
        "baseball": "(⚾)",
        "boat": "(⛵)",
        "phone": "(☎)",
        "mercury": "(☿)",
        "sun": "(☼)",
        "moon": "(☾)",
        "nuke": "(☢)",
        "hazard": "(☣)",
        "tunder": "(☈)",
        "king": "(♔)",
        "queen": "(♕)",
        "knight": "(♘)",
        "recycle": "(♻)",
        "scales": "(⚖)",
        "ball": "(⚽)",
        "golf": "(⛳)",
        "piece": "(☮)",
        "yy": "(☯)",
        "up": "👆👆👆",
        "down": "👇👇👇",
        "right": "👉",
        "left": "👈",
        "explode": "💥💥💥"
    }

    return flair[flag]


def read_config() -> dict:
    if not os.path.exists(CONFIG_NAME):
        raise Exception(f'当前路径下缺少配置文件{CONFIG_NAME}')
    config = configparser.ConfigParser()
    config.read(CONFIG_NAME, encoding='utf-8')
    config_default = config[DEFAULT]
    cf = dict()

    if KEY in config_default:
        key = config_default[KEY]
        key_list = key.split(' ')
        key_list = [int(x) - 1 for x in key_list if x.isdigit()]
        cf[KEY] = key_list
    else:
        # print(f'配置文件缺少{KEY}配置')
        raise Exception(f'配置文件缺少{KEY}配置')

    if COLUMN in config_default:
        column = config_default[COLUMN]
        column_list = column.split(' ')
        column_list = [int(x) - 1 for x in column_list if x.isdigit()]
        cf[COLUMN] = column_list
    else:
        # print(f'配置文件缺少{COLUMN}配置')
        raise Exception(f'配置文件缺少{COLUMN}配置')

    if ENCODING in config_default:
        encoding = config_default[ENCODING]
        cf[ENCODING] = encoding
    else:
        cf[ENCODING] = DEFAULT_ENCODING

    if OUTPUT_FOLDER_NAME in config_default:
        output_folder_name = config_default[OUTPUT_FOLDER_NAME]
        cf[OUTPUT_FOLDER_NAME] = output_folder_name
    else:
        cf[OUTPUT_FOLDER_NAME] = DEFAULT_OUTPUT_FOLDER_NAME

    cf[PERFORMANCE] = config_default.getint(PERFORMANCE, DEFAULT_PERFORMANCE)
    cf[KEEP_COMPUTE_FILE] = config_default.getint(KEEP_COMPUTE_FILE, DEFAULT_KEEP_COMPUTE_FILE)

    return cf


def check_config(user_config: dict, csv_header: list) -> None:
    max_column = len(csv_header)
    key_list = user_config[KEY]
    column_list = user_config[COLUMN]
    performance = user_config[PERFORMANCE]
    tmp_key_set = set()
    tmp_column_set = set()
    # check key
    for i in key_list:
        if i < 0 or i >= max_column:
            raise Exception(f'维度列数{i + 1}错误，需要在1-{max_column}之间')
        if i in tmp_key_set:
            raise Exception(f'维度列数存在重复，重复的输入={i + 1}')
        else:
            tmp_key_set.add(i)

    # check column
    for i in column_list:
        if i < 0:
            raise Exception(f'计算的列数不能小于1，错误的输入={i + 1}')
        if i >= max_column:
            raise Exception(f'计算的列数不能超过header最大列数({max_column})，错误的输入={i + 1}')
        if i in tmp_column_set:
            raise Exception(f'计算的列数存在重复，重复的输入={i + 1}')
        else:
            tmp_column_set.add(i)

    # check performance
    if performance < 0 or performance > 10:
        raise Exception(f'性能强度范围需限制在0-10之前，错误的输入={performance}')
    print_config(user_config, csv_header)


def scan_csv_file() -> list:
    origin_csv_list = sorted(os.listdir())
    return [x for x in origin_csv_list if '.csv' in x]


def load_csv_header(config: dict, csv_path) -> list:
    with open(csv_path, 'rt', encoding=config[ENCODING]) as c:
        reader = csv.reader(c)
        header = next(reader)
        return header


def confirm():
    user_confirm_input = input(f'\n{get_flair()} 确认请按1，以Enter键入> ')
    user_confirm_input = user_confirm_input.strip()
    if not user_confirm_input == '1':
        raise Exception(f'请确认后重新运行该程序')


def print_header(csv_header):
    print('\n')
    print('以下是第一个csv文件的header，确保所有csv文件header一致')
    print(get_flair(DOWN))
    print(csv_header)
    print(get_flair(UP))
    # confirm()


def print_config(user_config: dict, csv_header: list):
    print('\n配置文件参数如下')
    print(get_flair(DOWN))
    origin_user_key_list = [x + 1 for x in user_config[KEY]]
    print(f'计算的维度：{origin_user_key_list}')
    match_header_key_list = [csv_header[x - 1] for x in origin_user_key_list]
    print(f'维度对应的header：{match_header_key_list}')

    origin_user_column_list = [x + 1 for x in user_config[COLUMN]]
    print(f'计算的列数：{origin_user_column_list}')
    match_header_column_list = [csv_header[x - 1] for x in origin_user_column_list]
    print(f'对应的header：{match_header_column_list}')
    print(f'读取文件的编码格式：{user_config[ENCODING]}')
    output_folder_name = user_config[OUTPUT_FOLDER_NAME]
    print(f'输出的文件夹名称：{output_folder_name}')
    keep = '保留' if user_config[KEEP_COMPUTE_FILE] else '不保留'
    print(f'是否保留对应单个计算文件：{keep}')
    print(f'性能强度：{user_config[PERFORMANCE]}')
    print(get_flair(UP))


def generate_rows(res_dict: dict):
    rows = []
    for key in list(res_dict.keys()):
        kk = [k.strip() for k in key.split('-')]
        kk.extend(res_dict[key])
        rows.append(kk)
    return rows


def check_header(filename: str, header: list, first_csv_header: list):
    if len(header) != len(first_csv_header):
        raise Exception(f'文件{filename}的header与第一个csv文件的header不一致')
    for index in range(0, len(header)):
        if first_csv_header[index] != header[index]:
            raise Exception(
                f'文件{filename}的header中，第{index + 1}列{header[index]}与第一个csv头文件的值{first_csv_header[index]}不相等')


def calculate_single_csv(config: dict, filename: str, first_csv_header: list) -> dict:
    print(f'{get_flair(RIGHT)} 开始计算文件{filename}...')
    temp_res_dict = {}
    # r:以只读方式打开文件。文件的指针将会放在文件的开头。这是默认模式。
    # t:文本模式 (默认)。
    try:
        with open(filename, 'rt', encoding=config[ENCODING]) as c:
            reader = csv.reader(c)
            header = next(reader)
            check_header(filename, header, first_csv_header)

            for index, row in enumerate(reader):
                key = "-".join([str(row[x]) for x in config[KEY]])
                row_need_cal = [Decimal(row[x]) for x in config[COLUMN]]
                if key not in temp_res_dict.keys():
                    temp_res_dict[key] = row_need_cal
                else:
                    decimal_list = temp_res_dict[key]
                    temp_res_dict[key] = [i + j for i, j in zip(decimal_list, row_need_cal)]

        # sort by key and convert decimal to str for saving to csv
        res = {}
        for key in sorted(list(temp_res_dict.keys())):
            res[key] = [str(x) for x in temp_res_dict[key]]
        return res
    except Exception:
        print(f'计算文件{filename}出现异常，异常如下请检查')
        traceback.print_exc()
        raise Exception


def write_result_to_csv_file(config: dict, result: dict, filename: str, first_csv_header: list):
    if config[KEEP_COMPUTE_FILE]:
        print(f'{get_flair(RIGHT)} 开始写入对应文件{filename}的汇总文件...')
    path = f'{config[OUTPUT_FOLDER_NAME]}/{filename}'
    target_header = [first_csv_header[x] for x in config[KEY]] + [first_csv_header[x] for x in config[COLUMN]]
    with open(path, 'w', encoding=config[ENCODING], newline='') as f:
        writer = csv.writer(f)
        writer.writerow(target_header)
        rows = generate_rows(result)
        writer.writerows(rows)
        print(f'{get_flair(RIGHT)} 生成计算后的文件到{path}')
        f.flush()


def calculate_and_write(config: dict, filename: str, first_csv_header: list):
    try:
        res = calculate_single_csv(config, filename, first_csv_header)
        write_result_to_csv_file(config, res, filename, first_csv_header)
    except Exception:
        traceback.print_exc()


def summary_output_csv_file(config: dict, first_csv_header: list):
    print(f'\n')
    print(f'{get_flair(RIGHT)} 正在生成最终的汇总文件...')
    summary_path = f'{config[OUTPUT_FOLDER_NAME]}/{DEFAULT_OUTPUT_SUMMARY_NAME}'
    # 排除已有的summary.csv
    output_csv_filename_list = [x for x in sorted(os.listdir(config[OUTPUT_FOLDER_NAME])) if
                                DEFAULT_OUTPUT_SUMMARY_NAME not in x]
    # 生成汇总文件的header
    # 文件名称-{维度}-{计算列数}
    h1 = ['文件名称']
    h2 = [first_csv_header[x] for x in config[KEY]]
    h3 = [first_csv_header[x] for x in config[COLUMN]]
    summary_header = h1 + h2 + h3
    with open(summary_path, 'w', encoding=config[ENCODING], newline='') as s:
        summary_writer = csv.writer(s)
        summary_writer.writerow(summary_header)
        # s.flush()

        for output_csv_filename in output_csv_filename_list:
            with open(f'{config[OUTPUT_FOLDER_NAME]}/{output_csv_filename}', 'rt', encoding=config[ENCODING]) as c:
                reader = csv.reader(c)
                next(reader)
                rows = [[output_csv_filename] + row for row in reader]
                summary_writer.writerows(rows)
                s.flush()
    print(f'{get_flair(RIGHT)} 最终的汇总文件生成完毕...')
    if not config[KEEP_COMPUTE_FILE]:
        for output_csv_filename in output_csv_filename_list:
            os.remove(f'{config[OUTPUT_FOLDER_NAME]}/{output_csv_filename}')

    print(f'{get_flair(EXPLODE)} ENJOY {get_flair(EXPLODE)}')


def serial_compute(config: dict, csv_filename_list: list, first_csv_header: list):
    start_time = time.time()
    print('\n')
    print(f'{get_flair(RIGHT)} 开始串行执行...')
    for filename in csv_filename_list:
        calculate_and_write(config, filename, first_csv_header)
    summary_output_csv_file(config, first_csv_header)
    end_time = time.time()
    print("耗时: {:.2f}秒".format(end_time - start_time))


def multi_compute(config: dict, csv_filename_list: list, first_csv_header: list):
    start_time = time.time()
    print('\n')
    max_workers = int(os.cpu_count() * config[PERFORMANCE] / 10) or 1
    print(f'{get_flair(RIGHT)} 启动进程数：{max_workers}...')
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        task_handle = [pool.submit(calculate_and_write, config, filename, first_csv_header) for filename in
                       csv_filename_list]
    print(f'{get_flair(RIGHT)} 请等待所有计算任务完成...')
    wait(task_handle, return_when=ALL_COMPLETED)
    summary_output_csv_file(config, first_csv_header)
    end_time = time.time()
    print("耗时: {:.2f}秒".format(end_time - start_time))


def print_csv_files(csv_filename_list: list):
    print('\n')
    print(get_flair(DOWN))
    print('1、请将该脚本和配置文件放在待计算的csv文件夹下')
    print('2、请确保csv文件名以.csv结尾')
    print('3、请确保所有csv的格式（如header，编码格式等）一致')
    print(get_flair(UP))
    print(f'\n以下是待计算的csv文件')
    print(get_flair(DOWN))
    for filename in csv_filename_list:
        print(f'{filename}')
    print(get_flair(UP))
    pass


def main():
    config = read_config()
    csv_filename_list = scan_csv_file()
    first_file_name = csv_filename_list[0]
    print_csv_files(csv_filename_list)
    csv_header = load_csv_header(config, f'{first_file_name}')
    print_header(csv_header)
    check_config(config, csv_header)
    print(f'\n{get_flair(RIGHT) * 3}请确认以上待计算文件、配置信息{get_flair(LEFT) * 3}')
    confirm()
    if not os.path.exists(config[OUTPUT_FOLDER_NAME]):
        os.mkdir(config[OUTPUT_FOLDER_NAME])

    if config[PERFORMANCE] > 0:
        multi_compute(config, csv_filename_list, csv_header)
    else:
        serial_compute(config, csv_filename_list, csv_header)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    try:
        main()
    except Exception:
        traceback.print_exc()
    os.system('pause')
