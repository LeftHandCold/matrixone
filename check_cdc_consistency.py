#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CDC数据一致性校验脚本
用于在停止插入/删除操作后，校验上游和下游数据的一致性
"""

import os
import sys
import time
import logging
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import pymysql
from pymysql import Error as PyMySQLError
from collections import defaultdict

# 配置参数
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "6001"))
DB_USER = os.getenv("DB_USER", "dump")
DB_PASS = os.getenv("DB_PASS", "111")
TENANT_PREFIX = os.getenv("TENANT_PREFIX", "cdc_test")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "111")

# 校验配置
MAX_SAMPLE_SIZE = 10000  # 最大采样数量（用于数据内容校验）
DETAILED_CHECK = True  # 是否进行详细的数据内容校验
ROW_COUNT_DIFF_THRESHOLD = 0.01  # 行数差异阈值（1%）

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class CDCConnection:
    """数据库连接管理类"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._connection = None
    
    def connect(self):
        """建立数据库连接"""
        try:
            self._connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            return True
        except PyMySQLError as e:
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def execute_sql(self, sql: str, fetch: bool = False) -> Optional[List[Dict]]:
        """执行SQL语句"""
        if not self._connection:
            if not self.connect():
                return None
        
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
                if fetch:
                    return cursor.fetchall()
                return []
        except PyMySQLError as e:
            logger.error(f"SQL执行失败: {sql}")
            logger.error(f"错误信息: {e}")
            return None
    
    def execute_sql_silent(self, sql: str) -> Optional[str]:
        """执行SQL并返回单个值（静默模式）"""
        result = self.execute_sql(sql, fetch=True)
        if result and len(result) > 0:
            first_row = result[0]
            if first_row:
                return list(first_row.values())[0]
        return None
    
    def close(self):
        """关闭数据库连接"""
        if self._connection:
            self._connection.close()
            self._connection = None


class CDCConsistencyChecker:
    """CDC数据一致性校验类"""
    
    def __init__(self, conn: CDCConnection):
        self.conn = conn
        self.tenants: Dict[str, Dict] = {}  # tenant_name -> {account_id, databases, cdc_tasks}
        self.check_results: List[Dict] = []  # 存储校验结果
    
    def get_account_id(self, tenant_name: str) -> Optional[int]:
        """获取租户的account_id"""
        sql = f"SELECT account_id FROM mo_catalog.mo_account WHERE account_name='{tenant_name}' LIMIT 1"
        account_id = self.conn.execute_sql_silent(sql)
        if account_id:
            return int(account_id)
        return None
    
    def load_tenants(self):
        """加载所有租户信息"""
        logger.info("加载租户信息...")
        
        # 查询所有匹配前缀的租户
        sql = f"SELECT account_id, account_name FROM mo_catalog.mo_account WHERE account_name LIKE '{TENANT_PREFIX}_%'"
        result = self.conn.execute_sql(sql, fetch=True)
        
        if result:
            for row in result:
                tenant_name = row['account_name']
                account_id = row['account_id']
                self.tenants[tenant_name] = {
                    'account_id': account_id,
                    'databases': [],
                    'cdc_tasks': []
                }
                logger.info(f"发现租户: {tenant_name} (account_id: {account_id})")
        
        logger.info(f"共加载 {len(self.tenants)} 个租户")
    
    def check_existing_tasks(self) -> Dict[str, Dict]:
        """检查现有的CDC任务状态"""
        logger.info("检查现有CDC任务状态...")
        existing_tasks = {}
        
        try:
            # 查询所有CDC任务，包括tables字段以解析source_db和sink_db
            sql = "SELECT account_id, task_id, task_name, state, tables FROM mo_catalog.mo_cdc_task"
            result = self.conn.execute_sql(sql, fetch=True)
            
            if result:
                for row in result:
                    account_id = row['account_id']
                    task_id = row['task_id']
                    task_name = row['task_name']
                    status = row.get('state', '')
                    tables = row.get('tables', '')
                    
                    # 找到对应的租户
                    tenant_name = None
                    for tname, tinfo in self.tenants.items():
                        if tinfo.get('account_id') == account_id:
                            tenant_name = tname
                            break
                    
                    if tenant_name:
                        # 解析source_db和sink_db
                        # 优先从任务名推断（更可靠），tables字段解析作为备用
                        source_db = None
                        sink_db = None
                        level = "database"
                        table_name = None
                        
                        # 方法1: 从任务名推断（主要方法）
                        # 任务名格式: cdc_task_{tenant_name}_{db_name} 或 cdc_task_{tenant_name}_{db_name}_{table_name}
                        # 例如: cdc_task_cdc_test_1_cdc_test_1_db1 或 cdc_task_cdc_test_1_cdc_test_1_db1_table1
                        if task_name.startswith("cdc_task_"):
                            suffix = task_name[len("cdc_task_"):]
                            
                            # 去掉租户名前缀
                            if suffix.startswith(tenant_name + "_"):
                                db_and_rest = suffix[len(tenant_name) + 1:]  # 去掉租户名和后面的下划线
                                
                                # 检查是否是表级别任务（包含表名）
                                # 表级别任务格式: {db_name}_{table_name}
                                # 数据库级别任务格式: {db_name}
                                
                                # 尝试判断是否是表级别：检查最后一部分是否是表名（通常是 table1, table2 等）
                                parts = db_and_rest.split('_')
                                if len(parts) >= 2:
                                    # 检查最后一部分是否是表名格式
                                    last_part = parts[-1]
                                    if last_part.startswith('table') and len(last_part) > 5 and last_part[5:].isdigit():
                                        # 这是表级别任务
                                        level = "table"
                                        table_name = last_part
                                        source_db = '_'.join(parts[:-1])  # 除了最后一部分，其他都是数据库名
                                        sink_db = f"{source_db}_bak_table"
                                    else:
                                        # 数据库级别任务
                                        source_db = db_and_rest
                                        sink_db = f"{source_db}_bak"
                                else:
                                    # 只有一部分，应该是数据库名
                                    source_db = db_and_rest
                                    sink_db = f"{source_db}_bak"
                                
                                logger.info(f"从任务名推断: source_db={source_db}, sink_db={sink_db}, level={level}, table_name={table_name}")
                        
                        # 方法2: 如果从任务名推断失败，尝试从tables字段解析（备用方法）
                        if not source_db and tables:
                            logger.info(f"任务名推断失败，尝试从tables字段解析: '{tables}'")
                            # tables格式可能是: "source_db:sink_db" 或 "source_db.table_name:sink_db.table_name"
                            # 也可能包含多个表，用逗号分隔
                            # 先去除可能的引号和空格
                            tables = str(tables).strip().strip('"').strip("'")
                            
                            parts = tables.split(',')
                            if parts:
                                first_part = parts[0].strip()
                                
                                if ':' in first_part:
                                    source_sink = first_part.split(':', 1)
                                    if len(source_sink) == 2:
                                        source_spec = source_sink[0].strip()
                                        sink_spec = source_sink[1].strip()
                                        
                                        # 检查是否是表级别（包含点号）
                                        if '.' in source_spec and '.' in sink_spec:
                                            # 表级别: source_db.table_name:sink_db.table_name
                                            level = "table"
                                            source_parts = source_spec.split('.', 1)
                                            sink_parts = sink_spec.split('.', 1)
                                            if len(source_parts) == 2 and len(sink_parts) == 2:
                                                source_db = source_parts[0]
                                                table_name = source_parts[1]
                                                sink_db = sink_parts[0]
                                        else:
                                            # 数据库级别: source_db:sink_db
                                            source_db = source_spec
                                            sink_db = sink_spec
                                        
                                        logger.info(f"从tables字段解析: source_db={source_db}, sink_db={sink_db}, level={level}")
                        
                        if not source_db:
                            logger.warning(f"无法从任务名或tables字段推断数据库名: task_name={task_name}, tables={tables}")
                        
                        existing_tasks[task_name] = {
                            'tenant_name': tenant_name,
                            'task_id': task_id,
                            'status': status,
                            'account_id': account_id,
                            'source_db': source_db,
                            'sink_db': sink_db,
                            'level': level,
                            'table_name': table_name
                        }
                        logger.info(f"发现任务: {task_name} (租户: {tenant_name}, source_db: {source_db}, sink_db: {sink_db}, level: {level})")
        except Exception as e:
            logger.error(f"检查现有任务失败: {e}")
        
        return existing_tasks
    
    def get_table_row_count(self, tenant_name: str, db_name: str, table_name: str) -> int:
        """获取表的行数"""
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS,
            db_name
        )
        
        if not tenant_conn.connect():
            return -1
        
        try:
            sql = f"SELECT COUNT(*) as cnt FROM {table_name}"
            result = tenant_conn.execute_sql(sql, fetch=True)
            
            if result and len(result) > 0:
                count = result[0].get('cnt', 0)
                return int(count) if count is not None else 0
            return 0
        except Exception as e:
            logger.debug(f"获取表行数失败: {tenant_name}.{db_name}.{table_name}, {e}")
            return -1
        finally:
            tenant_conn.close()
    
    def get_table_ids(self, tenant_name: str, db_name: str, table_name: str, limit: int = None) -> List[int]:
        """获取表的所有ID（用于详细校验）"""
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS,
            db_name
        )
        
        if not tenant_conn.connect():
            return []
        
        try:
            if limit:
                sql = f"SELECT id FROM {table_name} ORDER BY id LIMIT {limit}"
            else:
                sql = f"SELECT id FROM {table_name} ORDER BY id"
            result = tenant_conn.execute_sql(sql, fetch=True)
            
            if result:
                return [row['id'] for row in result if 'id' in row]
            return []
        except Exception as e:
            logger.debug(f"获取表ID列表失败: {tenant_name}.{db_name}.{table_name}, {e}")
            return []
        finally:
            tenant_conn.close()
    
    def get_table_row_by_id(self, tenant_name: str, db_name: str, table_name: str, row_id: int) -> Optional[Dict]:
        """根据ID获取表的单行数据"""
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS,
            db_name
        )
        
        if not tenant_conn.connect():
            return None
        
        try:
            sql = f"SELECT * FROM {table_name} WHERE id = {row_id} LIMIT 1"
            result = tenant_conn.execute_sql(sql, fetch=True)
            
            if result and len(result) > 0:
                return result[0]
            return None
        except Exception as e:
            logger.debug(f"获取表行数据失败: {tenant_name}.{db_name}.{table_name}, id={row_id}, {e}")
            return None
        finally:
            tenant_conn.close()
    
    def compare_table_data(self, tenant_name: str, source_db: str, sink_db: str, 
                          table_name: str) -> Dict:
        """比较上游表和下游表的数据"""
        result = {
            'tenant_name': tenant_name,
            'source_db': source_db,
            'sink_db': sink_db,
            'table_name': table_name,
            'source_count': 0,
            'sink_count': 0,
            'count_match': False,
            'count_diff': 0,
            'count_diff_percent': 0.0,
            'data_match': False,
            'missing_in_sink': [],
            'extra_in_sink': [],
            'data_mismatch': [],
            'error': None
        }
        
        try:
            # 获取行数
            source_count = self.get_table_row_count(tenant_name, source_db, table_name)
            sink_count = self.get_table_row_count(tenant_name, sink_db, table_name)
            
            result['source_count'] = source_count
            result['sink_count'] = sink_count
            result['count_diff'] = abs(source_count - sink_count)
            
            if source_count > 0:
                result['count_diff_percent'] = (result['count_diff'] / source_count) * 100
            else:
                result['count_diff_percent'] = 0.0
            
            # 检查行数是否匹配（允许一定误差）
            if result['count_diff_percent'] <= (ROW_COUNT_DIFF_THRESHOLD * 100):
                result['count_match'] = True
            else:
                result['count_match'] = False
            
            # 如果行数不匹配，不进行详细数据校验
            if not result['count_match']:
                logger.warning(f"表 {tenant_name}.{source_db}.{table_name} 行数不匹配: 上游={source_count}, 下游={sink_count}, 差异={result['count_diff']} ({result['count_diff_percent']:.2f}%)")
                return result
            
            # 进行详细的数据内容校验
            if DETAILED_CHECK and source_count > 0:
                logger.info(f"开始详细校验表 {tenant_name}.{source_db}.{table_name} (上游={source_count}行, 下游={sink_count}行)...")
                
                # 获取上游表的ID列表（采样）
                sample_size = min(MAX_SAMPLE_SIZE, source_count)
                source_ids = set(self.get_table_ids(tenant_name, source_db, table_name, sample_size))
                sink_ids = set(self.get_table_ids(tenant_name, sink_db, table_name, sample_size))
                
                # 找出缺失和多余的数据
                missing_in_sink = source_ids - sink_ids
                extra_in_sink = sink_ids - source_ids
                
                result['missing_in_sink'] = list(missing_in_sink)[:100]  # 最多记录100个
                result['extra_in_sink'] = list(extra_in_sink)[:100]  # 最多记录100个
                
                if missing_in_sink:
                    logger.warning(f"表 {tenant_name}.{source_db}.{table_name} 下游缺失 {len(missing_in_sink)} 条数据（采样检查）")
                
                if extra_in_sink:
                    logger.warning(f"表 {tenant_name}.{source_db}.{table_name} 下游多余 {len(extra_in_sink)} 条数据（采样检查）")
                
                # 检查共同ID的数据内容是否一致
                common_ids = source_ids & sink_ids
                data_mismatch = []
                
                # 随机选择一些ID进行详细比较
                check_ids = list(common_ids)[:min(1000, len(common_ids))]
                
                for row_id in check_ids:
                    source_row = self.get_table_row_by_id(tenant_name, source_db, table_name, row_id)
                    sink_row = self.get_table_row_by_id(tenant_name, sink_db, table_name, row_id)
                    
                    if source_row and sink_row:
                        # 比较除id外的所有字段
                        source_dict = {k: v for k, v in source_row.items() if k != 'id'}
                        sink_dict = {k: v for k, v in sink_row.items() if k != 'id'}
                        
                        if source_dict != sink_dict:
                            data_mismatch.append({
                                'id': row_id,
                                'source': source_dict,
                                'sink': sink_dict
                            })
                
                result['data_mismatch'] = data_mismatch[:10]  # 最多记录10个不匹配的数据
                
                if data_mismatch:
                    logger.warning(f"表 {tenant_name}.{source_db}.{table_name} 发现 {len(data_mismatch)} 条数据内容不匹配（采样检查）")
                
                # 判断数据是否完全匹配
                result['data_match'] = (
                    len(missing_in_sink) == 0 and 
                    len(extra_in_sink) == 0 and 
                    len(data_mismatch) == 0
                )
            else:
                # 如果不需要详细校验或表为空，认为数据匹配
                result['data_match'] = result['count_match']
            
            if result['count_match'] and result['data_match']:
                logger.info(f"✓ 表 {tenant_name}.{source_db}.{table_name} 校验通过: 上游={source_count}行, 下游={sink_count}行")
            else:
                logger.warning(f"✗ 表 {tenant_name}.{source_db}.{table_name} 校验失败")
                
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"校验表 {tenant_name}.{source_db}.{table_name} 时发生错误: {e}")
        
        return result
    
    def check_task_consistency(self, task_name: str, task_info: Dict):
        """检查单个CDC任务的数据一致性"""
        tenant_name = task_info['tenant_name']
        source_db = task_info.get('source_db')
        sink_db = task_info.get('sink_db')
        level = task_info.get('level', 'database')
        table_name = task_info.get('table_name')
        status = task_info.get('status', '')
        
        logger.info("=" * 80)
        logger.info(f"检查任务: {task_name}")
        logger.info(f"  租户: {tenant_name}")
        logger.info(f"  级别: {level}")
        logger.info(f"  状态: {status}")
        logger.info(f"  源数据库: {source_db}")
        logger.info(f"  目标数据库: {sink_db}")
        logger.info("=" * 80)
        
        if not source_db or not sink_db:
            logger.warning(f"任务 {task_name} 的source_db或sink_db信息不完整，跳过校验")
            return
        
        if level == "database":
            # 数据库级别：检查所有表
            # 需要先获取源数据库中的所有表
            tenant_conn = CDCConnection(
                DB_HOST, DB_PORT,
                f"{tenant_name}#{ADMIN_USER}",
                ADMIN_PASS,
                source_db
            )
            
            if not tenant_conn.connect():
                logger.error(f"无法连接到租户 {tenant_name} 的源数据库 {source_db}")
                return
            
            try:
                # 获取所有表名
                sql = "SHOW TABLES"
                result = tenant_conn.execute_sql(sql, fetch=True)
                
                if result:
                    tables = []
                    for row in result:
                        # SHOW TABLES 返回的字段名可能是 'Tables_in_xxx' 格式
                        table_name = list(row.values())[0]
                        tables.append(table_name)
                    
                    logger.info(f"发现 {len(tables)} 个表需要校验")
                    
                    for tbl_name in tables:
                        check_result = self.compare_table_data(tenant_name, source_db, sink_db, tbl_name)
                        check_result['task_name'] = task_name
                        check_result['task_level'] = level
                        self.check_results.append(check_result)
                else:
                    logger.warning(f"源数据库 {source_db} 中没有找到表")
            except Exception as e:
                logger.error(f"获取表列表失败: {e}")
            finally:
                tenant_conn.close()
        
        elif level == "table" and table_name:
            # 表级别：只检查指定的表
            check_result = self.compare_table_data(tenant_name, source_db, sink_db, table_name)
            check_result['task_name'] = task_name
            check_result['task_level'] = level
            self.check_results.append(check_result)
    
    def check_all_tasks(self):
        """检查所有CDC任务的数据一致性"""
        logger.info("开始检查所有CDC任务的数据一致性...")
        
        # 加载租户信息
        self.load_tenants()
        
        if not self.tenants:
            logger.warning("没有找到任何租户，退出")
            return
        
        # 获取所有CDC任务
        existing_tasks = self.check_existing_tasks()
        
        if not existing_tasks:
            logger.warning("没有找到任何CDC任务，退出")
            return
        
        logger.info(f"共找到 {len(existing_tasks)} 个CDC任务，开始逐一校验...")
        
        # 逐一检查每个任务
        for task_name, task_info in existing_tasks.items():
            self.check_task_consistency(task_name, task_info)
            time.sleep(0.5)  # 短暂延迟，避免对数据库造成过大压力
    
    def generate_report(self) -> str:
        """生成校验报告"""
        if not self.check_results:
            return "没有校验结果"
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CDC数据一致性校验报告")
        report_lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # 统计信息
        total_tables = len(self.check_results)
        count_match_tables = sum(1 for r in self.check_results if r.get('count_match', False))
        data_match_tables = sum(1 for r in self.check_results if r.get('data_match', False))
        
        report_lines.append("统计信息:")
        report_lines.append(f"  总表数: {total_tables}")
        report_lines.append(f"  行数匹配的表: {count_match_tables}")
        report_lines.append(f"  数据完全匹配的表: {data_match_tables}")
        report_lines.append(f"  行数不匹配的表: {total_tables - count_match_tables}")
        report_lines.append(f"  数据不匹配的表: {total_tables - data_match_tables}")
        report_lines.append("")
        
        # 详细结果
        report_lines.append("详细结果:")
        report_lines.append("=" * 80)
        
        for result in self.check_results:
            report_lines.append("")
            report_lines.append(f"任务: {result.get('task_name', 'N/A')}")
            report_lines.append(f"  表: {result.get('tenant_name', 'N/A')}.{result.get('source_db', 'N/A')}.{result.get('table_name', 'N/A')}")
            report_lines.append(f"  上游行数: {result.get('source_count', 0)}")
            report_lines.append(f"  下游行数: {result.get('sink_count', 0)}")
            report_lines.append(f"  行数差异: {result.get('count_diff', 0)} ({result.get('count_diff_percent', 0.0):.2f}%)")
            report_lines.append(f"  行数匹配: {'✓' if result.get('count_match', False) else '✗'}")
            report_lines.append(f"  数据匹配: {'✓' if result.get('data_match', False) else '✗'}")
            
            if result.get('error'):
                report_lines.append(f"  错误: {result['error']}")
            
            if result.get('missing_in_sink'):
                missing = result['missing_in_sink']
                report_lines.append(f"  下游缺失数据ID (前{len(missing)}个): {missing[:10]}")
            
            if result.get('extra_in_sink'):
                extra = result['extra_in_sink']
                report_lines.append(f"  下游多余数据ID (前{len(extra)}个): {extra[:10]}")
            
            if result.get('data_mismatch'):
                mismatch = result['data_mismatch']
                report_lines.append(f"  数据内容不匹配 (前{len(mismatch)}个):")
                for item in mismatch[:3]:  # 只显示前3个
                    report_lines.append(f"    ID {item['id']}: 上游={item['source']}, 下游={item['sink']}")
        
        report_lines.append("")
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)
    
    def print_report(self):
        """打印校验报告"""
        report = self.generate_report()
        print("\n" + report + "\n")
        
        # 同时保存到文件
        report_file = f"cdc_consistency_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"校验报告已保存到: {report_file}")
        except Exception as e:
            logger.error(f"保存报告文件失败: {e}")


def main():
    """主函数"""
    logger.info("开始CDC数据一致性校验...")
    logger.info(f"数据库连接: {DB_USER}@{DB_HOST}:{DB_PORT}")
    logger.info(f"租户前缀: {TENANT_PREFIX}")
    logger.info("")
    
    # 创建数据库连接（使用系统账户）
    conn = CDCConnection(DB_HOST, DB_PORT, DB_USER, DB_PASS)
    if not conn.connect():
        logger.error("无法连接到数据库")
        sys.exit(1)
    
    try:
        checker = CDCConsistencyChecker(conn)
        
        # 检查所有任务
        checker.check_all_tasks()
        
        # 生成并打印报告
        checker.print_report()
        
        # 返回退出码
        all_match = all(r.get('count_match', False) and r.get('data_match', False) 
                       for r in checker.check_results)
        
        if all_match:
            logger.info("✓ 所有表的数据一致性校验通过")
            sys.exit(0)
        else:
            logger.warning("✗ 部分表的数据一致性校验失败，请查看详细报告")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"校验异常: {e}", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()
        logger.info("校验结束")


if __name__ == "__main__":
    main()


