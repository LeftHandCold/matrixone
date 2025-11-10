#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CDC GC 高级测试脚本
支持多租户、多数据库、多表、CDC任务管理和长时间运行
"""

import os
import sys
import time
import logging
import random
import threading
from typing import Optional, Dict, List, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from pymysql import Error as PyMySQLError

# 配置参数
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "6001"))
DB_USER = os.getenv("DB_USER", "dump")
DB_PASS = os.getenv("DB_PASS", "111")
ACCOUNT_ID = int(os.getenv("ACCOUNT_ID", "0"))

# 测试配置
NUM_TENANTS = 6
TENANT_PREFIX = "cdc_test"
ADMIN_USER = "admin"
ADMIN_PASS = "111"
NUM_DATABASES_PER_TENANT = 3
NUM_TABLES_PER_DATABASE = 3
WATERMARK_CHECK_INTERVAL = 60  # 检查水位间隔（秒）
WATERMARK_STALL_TIMEOUT = 1200  # 水位停滞超时（20分钟）
TASK_PAUSE_INTERVAL = 300  # 任务暂停间隔（5分钟）
TASK_RESUME_INTERVAL = 300  # 任务恢复间隔（5分钟）
DATA_INSERT_INTERVAL = 1  # 数据操作间隔（秒）
DELETE_PROBABILITY = 0.7  # 删除操作概率（30%）
ROW_COUNT_CHECK_PROBABILITY = 0.3  # 行数校验概率（30%）
ROW_COUNT_DIFF_THRESHOLD = 0.1  # 行数差异阈值（10%）
INIT_DATA_COUNT = 100000  # 初始化时每个表插入的数据量（10万条）
INIT_BATCH_SIZE = 100000  # 初始化批量插入的批次大小（10万条一次）
BATCH_INSERT_SIZE = 3000  # 批量插入的批次大小（1万条一次）
BATCH_DELETE_SIZE = 3000  # 批量删除的批次大小（3000条一次）

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


class AdvancedCDCTester:
    """高级CDC测试类"""
    
    def __init__(self, conn: CDCConnection):
        self.conn = conn
        self.tenants: Dict[str, Dict] = {}  # tenant_name -> {account_id, databases, cdc_tasks}
        self.running = True
        self.watermark_history: Dict[str, Dict[str, str]] = {}  # task_name -> {table: watermark}
        self.watermark_last_update: Dict[str, Dict[str, float]] = {}  # task_name -> {table: timestamp}
    
    def create_tenant(self, tenant_name: str, admin_user: str, admin_pass: str) -> bool:
        """创建租户"""
        sql = f"CREATE ACCOUNT {tenant_name} ADMIN_NAME '{admin_user}' IDENTIFIED BY '{admin_pass}';"
        logger.info(f"创建租户: {tenant_name}")
        result = self.conn.execute_sql(sql)
        if result is not None:
            # 获取account_id
            account_id = self.get_account_id(tenant_name)
            if account_id:
                self.tenants[tenant_name] = {
                    'account_id': account_id,
                    'admin_user': admin_user,
                    'admin_pass': admin_pass,
                    'databases': [],
                    'cdc_tasks': []
                }
                logger.info(f"租户 {tenant_name} 创建成功，account_id: {account_id}")
                return True
        return False
    
    def tenant_exists(self, tenant_name: str) -> bool:
        """检查租户是否存在"""
        sql = f"SELECT account_id FROM mo_catalog.mo_account WHERE account_name='{tenant_name}' LIMIT 1"
        account_id = self.conn.execute_sql_silent(sql)
        return account_id is not None
    
    def get_account_id(self, tenant_name: str) -> Optional[int]:
        """获取租户的account_id"""
        sql = f"SELECT account_id FROM mo_catalog.mo_account WHERE account_name='{tenant_name}' LIMIT 1"
        account_id = self.conn.execute_sql_silent(sql)
        if account_id:
            return int(account_id)
        return None
    
    def setup_tenants(self) -> bool:
        """设置租户（如果不存在则创建）"""
        logger.info(f"设置 {NUM_TENANTS} 个租户（前缀: {TENANT_PREFIX}）...")
        
        for i in range(1, NUM_TENANTS + 1):
            tenant_name = f"{TENANT_PREFIX}_{i}"
            
            if self.tenant_exists(tenant_name):
                logger.info(f"租户 {tenant_name} 已存在，跳过创建")
                account_id = self.get_account_id(tenant_name)
                if account_id:
                    self.tenants[tenant_name] = {
                        'account_id': account_id,
                        'admin_user': ADMIN_USER,
                        'admin_pass': ADMIN_PASS,
                        'databases': [],
                        'cdc_tasks': []
                    }
            else:
                if not self.create_tenant(tenant_name, ADMIN_USER, ADMIN_PASS):
                    logger.error(f"创建租户 {tenant_name} 失败")
                    return False
        
        logger.info(f"租户设置完成，共 {len(self.tenants)} 个租户")
        return True
    
    def create_database(self, tenant_name: str, db_name: str) -> bool:
        """在租户下创建数据库"""
        tenant = self.tenants.get(tenant_name)
        if not tenant:
            return False
        
        # 使用租户的admin用户连接
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS
        )
        
        if not tenant_conn.connect():
            logger.error(f"无法连接到租户 {tenant_name}")
            return False
        
        sql = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        result = tenant_conn.execute_sql(sql)
        tenant_conn.close()
        
        if result is not None:
            if db_name not in tenant['databases']:
                tenant['databases'].append(db_name)
            logger.info(f"租户 {tenant_name} 下创建数据库 {db_name} 成功")
            return True
        return False
    
    def create_table(self, tenant_name: str, db_name: str, table_name: str) -> bool:
        """在数据库中创建表"""
        tenant = self.tenants.get(tenant_name)
        if not tenant:
            return False
        
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS,
            db_name
        )
        
        if not tenant_conn.connect():
            return False
        
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100),
            data VARCHAR(200),
            value INT,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_ts (ts)
        )"""
        result = tenant_conn.execute_sql(sql)
        
        if result is not None:
            logger.info(f"租户 {tenant_name} 数据库 {db_name} 下创建表 {table_name} 成功")
            # 创建表后立即批量插入初始化数据
            self.batch_insert_initial_data(tenant_conn, table_name, INIT_DATA_COUNT, INIT_BATCH_SIZE)
            tenant_conn.close()
            return True
        
        tenant_conn.close()
        return False
    
    def batch_insert_initial_data(self, conn: CDCConnection, table_name: str, total_count: int, batch_size: int):
        """批量插入初始化数据（一次插入10万条）"""
        logger.info(f"开始为表 {table_name} 批量插入 {total_count} 条初始化数据（批次大小: {batch_size}）...")
        
        try:
            # 计算需要插入的批次数
            num_batches = (total_count + batch_size - 1) // batch_size
            
            for batch_idx in range(num_batches):
                # 计算当前批次要插入的数量
                current_batch_size = min(batch_size, total_count - batch_idx * batch_size)
                
                # 构建批量插入SQL（一次插入10万条）
                values = []
                for i in range(current_batch_size):
                    value = random.randint(1, 1000)
                    values.append(f"('name_{value}', 'data_{value}', {value})")
                
                sql = f"INSERT INTO {table_name} (name, data, value) VALUES {','.join(values)}"
                
                result = conn.execute_sql(sql)
                if result is None:
                    logger.error(f"批量插入失败: 表 {table_name}, 批次 {batch_idx + 1}/{num_batches}")
                else:
                    inserted_count = min((batch_idx + 1) * batch_size, total_count)
                    logger.info(f"表 {table_name} 已插入 {inserted_count} 条数据（批次 {batch_idx + 1}/{num_batches}）")
            
            logger.info(f"表 {table_name} 初始化数据插入完成，共 {total_count} 条")
        except Exception as e:
            logger.error(f"批量插入初始化数据异常: {e}")
    
    def setup_databases_and_tables(self) -> bool:
        """为每个租户设置数据库和表（只创建源数据库，sink数据库由CDC任务创建时创建）
        如果表已存在且有数据，跳过初始化
        """
        logger.info("为每个租户设置数据库和表（只创建源数据库）...")
        
        for tenant_name, tenant_info in self.tenants.items():
            logger.info(f"为租户 {tenant_name} 设置数据库和表...")
            
            for db_idx in range(1, NUM_DATABASES_PER_TENANT + 1):
                db_name = f"{tenant_name}_db{db_idx}"
                
                # 只创建源数据库，不创建sink数据库（sink数据库在创建CDC任务时创建）
                # 创建数据库
                if not self.create_database(tenant_name, db_name):
                    logger.error(f"创建数据库 {db_name} 失败")
                    continue
                
                # 创建表
                for tbl_idx in range(1, NUM_TABLES_PER_DATABASE + 1):
                    table_name = f"table{tbl_idx}"
                    
                    # 检查表是否已存在且有数据
                    if self.check_table_has_data(tenant_name, db_name, table_name):
                        logger.info(f"表 {tenant_name}.{db_name}.{table_name} 已存在且有数据，跳过初始化")
                        continue
                    
                    # 创建表（如果不存在）并初始化数据
                    if not self.create_table(tenant_name, db_name, table_name):
                        logger.error(f"创建表 {table_name} 失败")
        
        logger.info("数据库和表设置完成（只创建源数据库）")
        return True
    
    def create_cdc_task(self, tenant_name: str, task_name: str, source_db: str, 
                       sink_db: str, level: str = "database", table_name: str = None) -> bool:
        """创建CDC任务"""
        tenant = self.tenants.get(tenant_name)
        if not tenant:
            return False
        
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS
        )
        
        if not tenant_conn.connect():
            return False
        
        source_uri = f"mysql://{tenant_name}#{ADMIN_USER}:{ADMIN_PASS}@{DB_HOST}:{DB_PORT}"
        sink_uri = f"mysql://{tenant_name}#{ADMIN_USER}:{ADMIN_PASS}@{DB_HOST}:{DB_PORT}"
        
        # CDC任务的sink格式：
        # 数据库级别: source_db:sink_db
        # 表级别: source_db.table_name:sink_db.table_name
        if level == "table" and table_name:
            sink_db_spec = f"{source_db}.{table_name}:{sink_db}.{table_name}"
        else:
            sink_db_spec = f"{source_db}:{sink_db}"
        
        sql = f"CREATE CDC {task_name} '{source_uri}' 'matrixone' '{sink_uri}' '{sink_db_spec}' {{'Level'='{level}'}};"
        logger.info(f"租户 {tenant_name} 创建CDC任务: {task_name} (级别: {level})")
        result = tenant_conn.execute_sql(sql)
        tenant_conn.close()
        
        if result is not None:
            tenant['cdc_tasks'].append({
                'task_name': task_name,
                'tenant_name': tenant_name,
                'source_db': source_db,
                'sink_db': sink_db,
                'level': level,
                'table_name': table_name,
                'paused': False
            })
            logger.info(f"CDC任务 {task_name} 创建成功")
            return True
        return False
    
    def check_existing_tasks(self) -> Dict[str, Dict]:
        """检查现有的CDC任务状态"""
        logger.info("检查现有CDC任务状态...")
        existing_tasks = {}
        
        # 使用系统租户连接查询所有任务
        sys_conn = CDCConnection(
            DB_HOST, DB_PORT,
            DB_USER,  # dump用户
            DB_PASS
        )
        
        if not sys_conn.connect():
            logger.warning("无法连接到系统租户，跳过任务检查")
            return existing_tasks
        
        try:
            # 查询所有CDC任务
            sql = "SELECT account_id, task_id, task_name, status FROM mo_catalog.mo_cdc_task"
            result = sys_conn.execute_sql(sql, fetch=True)
            
            if result:
                for row in result:
                    account_id = row['account_id']
                    task_id = row['task_id']
                    task_name = row['task_name']
                    status = row.get('status', '')
                    
                    # 找到对应的租户
                    tenant_name = None
                    for tname, tinfo in self.tenants.items():
                        if tinfo.get('account_id') == account_id:
                            tenant_name = tname
                            break
                    
                    if tenant_name:
                        existing_tasks[task_name] = {
                            'tenant_name': tenant_name,
                            'task_id': task_id,
                            'status': status,
                            'account_id': account_id
                        }
                        logger.info(f"发现现有任务: {task_name} (租户: {tenant_name}, 状态: {status})")
        except Exception as e:
            logger.error(f"检查现有任务失败: {e}")
        finally:
            sys_conn.close()
        
        return existing_tasks
    
    def resume_all_paused_tasks(self) -> int:
        """恢复所有暂停的任务"""
        logger.info("检查并恢复所有暂停的任务...")
        resumed_count = 0
        
        existing_tasks = self.check_existing_tasks()
        
        for task_name, task_info in existing_tasks.items():
            tenant_name = task_info['tenant_name']
            status = task_info.get('status', '').upper()
            
            # 检查任务是否暂停
            if 'PAUSE' in status or 'PAUSED' in status:
                logger.info(f"恢复暂停的任务: {task_name} (租户: {tenant_name})")
                if self.resume_cdc_task(tenant_name, task_name):
                    resumed_count += 1
                    # 更新本地任务状态
                    tenant = self.tenants.get(tenant_name)
                    if tenant:
                        for task in tenant.get('cdc_tasks', []):
                            if task['task_name'] == task_name:
                                task['paused'] = False
                                break
        
        if resumed_count > 0:
            logger.info(f"已恢复 {resumed_count} 个暂停的任务")
        else:
            logger.info("没有发现暂停的任务")
        
        return resumed_count
    
    def check_table_has_data(self, tenant_name: str, db_name: str, table_name: str) -> bool:
        """检查表是否有数据"""
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS,
            db_name
        )
        
        if not tenant_conn.connect():
            return False
        
        try:
            # 检查表是否存在且有数据
            sql = f"SELECT COUNT(*) as cnt FROM {table_name} LIMIT 1"
            result = tenant_conn.execute_sql(sql, fetch=True)
            
            if result and len(result) > 0:
                count = result[0].get('cnt', 0)
                return count > 0
            return False
        except Exception as e:
            logger.debug(f"检查表数据失败: {tenant_name}.{db_name}.{table_name}, {e}")
            return False
        finally:
            tenant_conn.close()
    
    def check_all_tables_have_data(self) -> bool:
        """检查所有表是否都有数据"""
        logger.info("检查所有表是否已有数据...")
        tables_with_data = 0
        total_tables = 0
        
        for tenant_name, tenant_info in self.tenants.items():
            # 只检查源数据库
            source_databases = [db for db in tenant_info['databases'] 
                              if not db.endswith("_bak") and not db.endswith("_bak_table")]
            
            for db_name in source_databases:
                for tbl_idx in range(1, NUM_TABLES_PER_DATABASE + 1):
                    table_name = f"table{tbl_idx}"
                    total_tables += 1
                    
                    if self.check_table_has_data(tenant_name, db_name, table_name):
                        tables_with_data += 1
                        logger.debug(f"表 {tenant_name}.{db_name}.{table_name} 已有数据")
        
        logger.info(f"数据检查完成: {tables_with_data}/{total_tables} 个表有数据")
        
        # 如果所有表都有数据，返回True
        return tables_with_data == total_tables and total_tables > 0
    
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
    
    def check_row_count_consistency(self, tenant_name: str, task_name: str) -> Dict[str, Dict]:
        """检查上游表和下游表的行数一致性"""
        results = {}
        
        # 获取任务信息
        task_info = None
        for tname, tinfo in self.tenants.items():
            if tname == tenant_name:
                for task in tinfo.get('cdc_tasks', []):
                    if task['task_name'] == task_name:
                        task_info = task
                        break
                break
        
        if not task_info:
            return results
        
        source_db = task_info.get('source_db')
        sink_db = task_info.get('sink_db')
        level = task_info.get('level', 'database')
        table_name = task_info.get('table_name')
        
        if not source_db or not sink_db:
            return results
        
        # 根据任务级别检查表
        if level == 'database':
            # 数据库级别：检查所有表
            for tbl_idx in range(1, NUM_TABLES_PER_DATABASE + 1):
                table_name = f"table{tbl_idx}"
                source_count = self.get_table_row_count(tenant_name, source_db, table_name)
                sink_count = self.get_table_row_count(tenant_name, sink_db, table_name)
                
                if source_count >= 0 and sink_count >= 0:
                    diff = abs(source_count - sink_count)
                    diff_percent = (diff / source_count * 100) if source_count > 0 else 0
                    
                    results[table_name] = {
                        'source_count': source_count,
                        'sink_count': sink_count,
                        'diff': diff,
                        'diff_percent': diff_percent,
                        'within_threshold': diff_percent <= (ROW_COUNT_DIFF_THRESHOLD * 100)
                    }
        else:
            # 表级别：只检查指定的表
            if table_name:
                source_count = self.get_table_row_count(tenant_name, source_db, table_name)
                sink_count = self.get_table_row_count(tenant_name, sink_db, table_name)
                
                if source_count >= 0 and sink_count >= 0:
                    diff = abs(source_count - sink_count)
                    diff_percent = (diff / source_count * 100) if source_count > 0 else 0
                    
                    results[table_name] = {
                        'source_count': source_count,
                        'sink_count': sink_count,
                        'diff': diff,
                        'diff_percent': diff_percent,
                        'within_threshold': diff_percent <= (ROW_COUNT_DIFF_THRESHOLD * 100)
                    }
        
        return results
    
    def random_check_row_counts(self) -> List[str]:
        """随机检查行数一致性"""
        issues = []
        
        # 随机选择一些任务进行检查
        all_tasks = []
        for tenant_name, tenant_info in self.tenants.items():
            for task in tenant_info.get('cdc_tasks', []):
                if not task.get('paused', False):
                    all_tasks.append((tenant_name, task['task_name']))
        
        if not all_tasks:
            return issues
        
        # 随机选择30%的任务进行检查
        num_to_check = max(1, int(len(all_tasks) * 0.3))
        selected_tasks = random.sample(all_tasks, min(num_to_check, len(all_tasks)))
        
        for tenant_name, task_name in selected_tasks:
            results = self.check_row_count_consistency(tenant_name, task_name)
            
            for table_name, result in results.items():
                if not result['within_threshold']:
                    issue = f"{tenant_name}.{task_name}.{table_name}: 上游={result['source_count']}, 下游={result['sink_count']}, 差异={result['diff']} ({result['diff_percent']:.2f}%)"
                    issues.append(issue)
                    logger.warning(f"⚠️ 行数差异过大: {issue}")
                else:
                    logger.debug(f"✓ {tenant_name}.{task_name}.{table_name}: 上游={result['source_count']}, 下游={result['sink_count']}, 差异={result['diff']} ({result['diff_percent']:.2f}%)")
        
        return issues
    
    def load_existing_tasks(self) -> bool:
        """加载现有的CDC任务到本地状态"""
        logger.info("加载现有CDC任务...")
        existing_tasks = self.check_existing_tasks()
        
        loaded_count = 0
        for task_name, task_info in existing_tasks.items():
            tenant_name = task_info['tenant_name']
            tenant = self.tenants.get(tenant_name)
            
            if not tenant:
                continue
            
            # 检查任务是否已经在本地状态中
            task_exists = False
            for task in tenant.get('cdc_tasks', []):
                if task['task_name'] == task_name:
                    task_exists = True
                    # 更新状态
                    task['paused'] = 'PAUSE' not in task_info.get('status', '').upper()
                    break
            
            if not task_exists:
                # 从任务名推断任务信息
                # 任务名格式: cdc_task_{tenant_name}_{db_name} 或 cdc_task_{tenant_name}_{db_name}_{table_name}
                parts = task_name.split('_')
                if len(parts) >= 4:
                    # 尝试解析任务信息
                    db_name = None
                    sink_db = None
                    level = "database"
                    table_name = None
                    
                    # 简化处理：从任务名中提取信息
                    # 这里需要根据实际任务名格式来解析
                    # 暂时使用一个通用的方式
                    tenant['cdc_tasks'].append({
                        'task_name': task_name,
                        'tenant_name': tenant_name,
                        'source_db': 'unknown',  # 需要从任务配置中获取
                        'sink_db': 'unknown',
                        'level': level,
                        'table_name': table_name,
                        'paused': 'PAUSE' not in task_info.get('status', '').upper()
                    })
                    loaded_count += 1
        
        if loaded_count > 0:
            logger.info(f"已加载 {loaded_count} 个现有任务到本地状态")
        
        return True
    
    def task_exists(self, tenant_name: str, task_name: str) -> bool:
        """检查任务是否已存在"""
        existing_tasks = self.check_existing_tasks()
        return task_name in existing_tasks
    
    def setup_cdc_tasks(self) -> bool:
        """设置CDC任务（如果任务已存在则跳过）"""
        logger.info("设置CDC任务...")
        
        # 获取现有任务列表
        existing_tasks = self.check_existing_tasks()
        existing_task_names = set(existing_tasks.keys())
        
        # 随机选择一些数据库创建数据库级别的CDC任务
        # 只选择源数据库（不包含_bak后缀的数据库）
        all_databases = []
        for tenant_name, tenant_info in self.tenants.items():
            # 只选择源数据库
            source_databases = [db for db in tenant_info['databases'] 
                              if not db.endswith("_bak") and not db.endswith("_bak_table")]
            for db_name in source_databases:
                all_databases.append((tenant_name, db_name))
        
        # 随机选择70%的数据库创建CDC任务
        num_cdc_databases = max(1, int(len(all_databases) * 0.7))
        selected_databases = random.sample(all_databases, min(num_cdc_databases, len(all_databases)))
        
        created_count = 0
        skipped_count = 0
        
        for tenant_name, db_name in selected_databases:
            sink_db = f"{db_name}_bak"
            task_name = f"cdc_task_{tenant_name}_{db_name}"
            
            # 检查任务是否已存在
            if task_name in existing_task_names:
                logger.info(f"任务 {task_name} 已存在，跳过创建")
                skipped_count += 1
                # 加载到本地状态
                tenant = self.tenants.get(tenant_name)
                if tenant:
                    task_exists = False
                    for task in tenant.get('cdc_tasks', []):
                        if task['task_name'] == task_name:
                            task_exists = True
                            break
                    if not task_exists:
                        tenant['cdc_tasks'].append({
                            'task_name': task_name,
                            'tenant_name': tenant_name,
                            'source_db': db_name,
                            'sink_db': sink_db,
                            'level': 'database',
                            'table_name': None,
                            'paused': False
                        })
                continue
            
            # 创建sink数据库（sink数据库不需要创建表，CDC会自动同步）
            if not self.create_database(tenant_name, sink_db):
                continue
            
            if self.create_cdc_task(tenant_name, task_name, db_name, sink_db, "database"):
                created_count += 1
        
        # 选择一个没有CDC任务的数据库，创建表级别的CDC任务
        cdc_databases = {(t, d) for t, d in selected_databases}
        non_cdc_databases = [(t, d) for t, d in all_databases if (t, d) not in cdc_databases]
        
        if non_cdc_databases:
            tenant_name, db_name = random.choice(non_cdc_databases)
            sink_db = f"{db_name}_bak_table"
            table_name = "table1"  # 使用第一个表
            task_name = f"cdc_task_{tenant_name}_{db_name}_{table_name}"
            
            # 检查任务是否已存在
            if task_name in existing_task_names:
                logger.info(f"任务 {task_name} 已存在，跳过创建")
                skipped_count += 1
            else:
                # 创建sink数据库（sink数据库不需要创建表，CDC会自动同步）
                if self.create_database(tenant_name, sink_db):
                    if self.create_cdc_task(tenant_name, task_name, db_name, sink_db, "table", table_name):
                        created_count += 1
        
        logger.info(f"CDC任务设置完成: 创建 {created_count} 个新任务, 跳过 {skipped_count} 个已存在任务")
        logger.info(f"当前共有 {sum(len(t['cdc_tasks']) for t in self.tenants.values())} 个任务")
        return True
    
    def pause_cdc_task(self, tenant_name: str, task_name: str) -> bool:
        """暂停CDC任务"""
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS
        )
        
        if not tenant_conn.connect():
            return False
        
        sql = f"PAUSE CDC TASK {task_name};"
        logger.info(f"暂停CDC任务: {task_name}")
        result = tenant_conn.execute_sql(sql)
        tenant_conn.close()
        
        if result is not None:
            # 更新任务状态
            for tenant_info in self.tenants.values():
                for task in tenant_info['cdc_tasks']:
                    if task['task_name'] == task_name:
                        task['paused'] = True
                        break
            return True
        return False
    
    def resume_cdc_task(self, tenant_name: str, task_name: str) -> bool:
        """恢复CDC任务"""
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS
        )
        
        if not tenant_conn.connect():
            return False
        
        sql = f"RESUME CDC TASK {task_name};"
        logger.info(f"恢复CDC任务: {task_name}")
        result = tenant_conn.execute_sql(sql)
        tenant_conn.close()
        
        if result is not None:
            # 更新任务状态
            for tenant_info in self.tenants.values():
                for task in tenant_info['cdc_tasks']:
                    if task['task_name'] == task_name:
                        task['paused'] = False
                        break
            return True
        return False
    
    def get_task_id_by_name(self, tenant_name: str, task_name: str) -> Optional[str]:
        """根据task_name获取task_id（使用系统租户连接）"""
        tenant = self.tenants.get(tenant_name)
        if not tenant:
            return None
        
        # mo_cdc_task表在系统租户下，需要使用dump用户连接
        sys_conn = CDCConnection(
            DB_HOST, DB_PORT,
            DB_USER,  # dump用户
            DB_PASS
        )
        
        if not sys_conn.connect():
            return None
        
        account_id = tenant['account_id']
        sql = f"SELECT task_id FROM mo_catalog.mo_cdc_task WHERE account_id={account_id} AND task_name='{task_name}' LIMIT 1"
        task_id = sys_conn.execute_sql_silent(sql)
        sys_conn.close()
        return task_id
    
    def check_watermark(self, tenant_name: str, task_name: str) -> Dict[str, str]:
        """检查watermark（使用系统租户连接）"""
        task_id = self.get_task_id_by_name(tenant_name, task_name)
        if not task_id:
            return {}
        
        tenant = self.tenants.get(tenant_name)
        if not tenant:
            return {}
        
        # mo_cdc_watermark表在系统租户下，需要使用dump用户连接
        sys_conn = CDCConnection(
            DB_HOST, DB_PORT,
            DB_USER,  # dump用户
            DB_PASS
        )
        
        if not sys_conn.connect():
            return {}
        
        account_id = tenant['account_id']
        sql = f"""SELECT db_name, table_name, watermark, err_msg 
                  FROM mo_catalog.mo_cdc_watermark 
                  WHERE account_id={account_id} AND task_id='{task_id}'"""
        result = sys_conn.execute_sql(sql, fetch=True)
        sys_conn.close()
        
        watermarks = {}
        if result:
            for row in result:
                key = f"{row['db_name']}.{row['table_name']}"
                watermarks[key] = row['watermark'] or ""
        
        return watermarks
    
    def compare_timestamps(self, ts1: str, ts2: str) -> int:
        """比较两个时间戳字符串
        支持格式：
        - TS格式: "1762763441896327755-1" (physical-logical)
          格式说明: physical time (纳秒) - logical time
        - 时间格式: "2024-01-01 12:00:00.000000"
        返回: -1 if ts1 < ts2, 0 if ts1 == ts2, 1 if ts1 > ts2
        """
        try:
            # 首先尝试TS格式 (physical-logical)
            # 格式: "1762763441896327755-1"
            if '-' in ts1 and '-' in ts2:
                parts1 = ts1.split('-', 1)  # 只分割第一个'-'，避免logical time中有'-'
                parts2 = ts2.split('-', 1)
                if len(parts1) == 2 and len(parts2) == 2:
                    try:
                        # physical time 是纳秒级时间戳（大整数）
                        p1 = int(parts1[0])
                        l1 = int(parts1[1])
                        p2 = int(parts2[0])
                        l2 = int(parts2[1])
                        
                        # 先比较 physical time
                        if p1 < p2:
                            return -1
                        elif p1 > p2:
                            return 1
                        else:
                            # physical time相同，比较logical time
                            if l1 < l2:
                                return -1
                            elif l1 > l2:
                                return 1
                            else:
                                return 0
                    except ValueError as e:
                        logger.debug(f"TS格式解析失败: {ts1} 或 {ts2}, {e}")
                        pass  # 不是TS格式，继续尝试其他格式
            
            # 尝试解析时间戳字符串
            # 格式可能是: "2024-01-01 12:00:00.000000" 或 "2024-01-01T12:00:00.000000Z"
            from datetime import datetime
            
            formats = [
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
            ]
            
            dt1 = None
            dt2 = None
            
            for fmt in formats:
                try:
                    dt1 = datetime.strptime(ts1, fmt)
                    break
                except ValueError:
                    continue
            
            for fmt in formats:
                try:
                    dt2 = datetime.strptime(ts2, fmt)
                    break
                except ValueError:
                    continue
            
            if dt1 is not None and dt2 is not None:
                if dt1 < dt2:
                    return -1
                elif dt1 > dt2:
                    return 1
                else:
                    return 0
            
            # 如果无法解析，使用字符串比较
            if ts1 < ts2:
                return -1
            elif ts1 > ts2:
                return 1
            else:
                return 0
        except Exception as e:
            logger.error(f"比较时间戳失败: {ts1} vs {ts2}, {e}")
            # 如果解析失败，使用字符串比较
            if ts1 < ts2:
                return -1
            elif ts1 > ts2:
                return 1
            else:
                return 0
    
    def check_watermark_stall(self) -> Tuple[List[str], bool]:
        """检查水位停滞和回退的任务
        返回: (停滞任务列表, 是否发现严重问题需要终止)
        """
        stalled_tasks = []
        has_critical_issue = False
        current_time = time.time()
        
        for tenant_name, tenant_info in self.tenants.items():
            for task in tenant_info['cdc_tasks']:
                if task['paused']:
                    continue
                
                task_name = task['task_name']
                watermarks = self.check_watermark(tenant_name, task_name)
                
                for table_key, watermark in watermarks.items():
                    if not watermark:
                        continue
                    
                    # 检查是否有更新
                    last_watermark = self.watermark_history.get(task_name, {}).get(table_key)
                    last_update_time = self.watermark_last_update.get(task_name, {}).get(table_key, 0)
                    
                    if last_watermark:
                        # 检查水位是否回退
                        comparison = self.compare_timestamps(watermark, last_watermark)
                        if comparison < 0:
                            # 水位回退了！
                            has_critical_issue = True
                            stalled_tasks.append(f"{task_name} ({table_key}) - 水位回退: {last_watermark} -> {watermark}")
                            logger.error(f"❌ 严重错误: 任务 {task_name} 表 {table_key} 水位回退!")
                            logger.error(f"   之前水位: {last_watermark}")
                            logger.error(f"   当前水位: {watermark}")
                        elif comparison > 0:
                            # 水位正常前进
                            self.watermark_history[task_name][table_key] = watermark
                            self.watermark_last_update[task_name][table_key] = current_time
                            logger.info(f"✓ 任务 {task_name} 表 {table_key} 水位更新: {watermark}")
                        else:
                            # 水位没有变化
                            if current_time - last_update_time > WATERMARK_STALL_TIMEOUT:
                                has_critical_issue = True
                                stalled_tasks.append(f"{task_name} ({table_key}) - 水位停滞超过 {WATERMARK_STALL_TIMEOUT/60} 分钟")
                                logger.error(f"❌ 严重错误: 任务 {task_name} 表 {table_key} 水位停滞超过 {WATERMARK_STALL_TIMEOUT/60} 分钟!")
                                logger.error(f"   最后水位: {last_watermark}")
                                logger.error(f"   停滞时间: {(current_time - last_update_time)/60:.1f} 分钟")
                    else:
                        # 第一次记录水位
                        if task_name not in self.watermark_history:
                            self.watermark_history[task_name] = {}
                        if task_name not in self.watermark_last_update:
                            self.watermark_last_update[task_name] = {}
                        
                        self.watermark_history[task_name][table_key] = watermark
                        self.watermark_last_update[task_name][table_key] = current_time
                        logger.info(f"✓ 任务 {task_name} 表 {table_key} 首次记录水位: {watermark}")
        
        return stalled_tasks, has_critical_issue
    
    def insert_data_worker(self, tenant_name: str, db_name: str, table_name: str):
        """数据插入和删除工作线程（只操作源数据库，不操作sink数据库）
        持续运行，不会因为其他操作而停止
        批量插入1万行，批量删除3000行
        """
        # 只操作源数据库，不操作sink数据库（sink数据库的表由CDC自动同步）
        if db_name.endswith("_bak") or db_name.endswith("_bak_table"):
            logger.debug(f"跳过sink数据库 {db_name}，只操作源数据库")
            return
        
        tenant = self.tenants.get(tenant_name)
        if not tenant:
            return
        
        tenant_conn = CDCConnection(
            DB_HOST, DB_PORT,
            f"{tenant_name}#{ADMIN_USER}",
            ADMIN_PASS,
            db_name
        )
        
        if not tenant_conn.connect():
            logger.error(f"无法连接到租户 {tenant_name} 数据库 {db_name}")
            return
        
        # 记录操作计数
        insert_rows = 0
        delete_rows = 0
        error_count = 0
        
        try:
            while self.running:
                try:
                    # 随机决定是插入还是删除
                    if random.random() < DELETE_PROBABILITY:
                        # 执行批量删除操作（一次删除3000行）
                        delete_type = random.choice(['by_condition', 'oldest'])
                        
                        if delete_type == 'by_condition':
                            # 按条件批量删除（删除value在某个范围内的记录）
                            value_min = random.randint(1, 500)
                            value_max = value_min + random.randint(1, 100)
                            sql = f"DELETE FROM {table_name} WHERE value >= {value_min} AND value <= {value_max} LIMIT {BATCH_DELETE_SIZE}"
                        else:  # oldest
                            # 删除最旧的记录（按ts排序）
                            sql = f"DELETE FROM {table_name} ORDER BY ts ASC LIMIT {BATCH_DELETE_SIZE}"
                        
                        result = tenant_conn.execute_sql(sql)
                        if result is None:
                            error_count += 1
                            if error_count % 10 == 0:  # 每10次错误才记录一次
                                logger.debug(f"数据删除失败或没有数据可删除: {table_name}")
                        else:
                            delete_rows += BATCH_DELETE_SIZE
                            if delete_rows % 30000 == 0:  # 每删除3万行记录一次
                                logger.debug(f"表 {table_name} 已删除 {delete_rows} 行数据")
                    else:
                        # 执行批量插入操作（一次插入1万行）
                        values = []
                        for i in range(BATCH_INSERT_SIZE):
                            value = random.randint(1, 1000)
                            values.append(f"('name_{value}', 'data_{value}', {value})")
                        
                        sql = f"INSERT INTO {table_name} (name, data, value) VALUES {','.join(values)}"
                        result = tenant_conn.execute_sql(sql)
                        if result is None:
                            error_count += 1
                            if error_count % 10 == 0:  # 每10次错误才记录一次
                                logger.error(f"数据插入失败: {table_name}")
                        else:
                            insert_rows += BATCH_INSERT_SIZE
                            if insert_rows % 100000 == 0:  # 每插入10万行记录一次
                                logger.debug(f"表 {table_name} 已插入 {insert_rows} 行数据")
                    
                    time.sleep(DATA_INSERT_INTERVAL)
                except Exception as e:
                    error_count += 1
                    if error_count % 10 == 0:
                        logger.error(f"数据操作异常: {table_name}, {e}")
                    # 连接可能断开，尝试重连
                    if not tenant_conn._connection or not tenant_conn._connection.open:
                        logger.warning(f"连接断开，尝试重连: {tenant_name}.{db_name}")
                        if not tenant_conn.connect():
                            logger.error(f"重连失败，等待后重试: {tenant_name}.{db_name}")
                            time.sleep(5)
                            continue
        except Exception as e:
            logger.error(f"数据插入线程异常退出: {tenant_name}.{db_name}.{table_name}, {e}")
        finally:
            tenant_conn.close()
            logger.info(f"数据插入线程结束: {tenant_name}.{db_name}.{table_name} (插入: {insert_rows}行, 删除: {delete_rows}行, 错误: {error_count})")
    
    def start_data_insertion(self):
        """启动数据插入和删除线程（只操作源数据库，不操作sink数据库）"""
        logger.info("启动数据插入和删除线程（只操作源数据库）...")
        logger.info(f"数据操作策略: 70%插入（一次{BATCH_INSERT_SIZE}行），30%删除（一次{BATCH_DELETE_SIZE}行）")
        logger.info(f"操作间隔: {DATA_INSERT_INTERVAL}秒")
        
        threads = []
        for tenant_name, tenant_info in self.tenants.items():
            # 只操作源数据库，不操作sink数据库（带_bak后缀的数据库）
            source_databases = [db for db in tenant_info['databases'] 
                              if not db.endswith("_bak") and not db.endswith("_bak_table")]
            for db_name in source_databases:
                for table_idx in range(1, NUM_TABLES_PER_DATABASE + 1):
                    table_name = f"table{table_idx}"
                    thread = threading.Thread(
                        target=self.insert_data_worker,
                        args=(tenant_name, db_name, table_name),
                        daemon=True
                    )
                    thread.start()
                    threads.append(thread)
        
        logger.info(f"数据操作线程启动完成，共 {len(threads)} 个线程（只操作源数据库）")
        return threads
    
    def random_pause_resume_tasks(self):
        """随机暂停和恢复任务"""
        all_tasks = []
        for tenant_name, tenant_info in self.tenants.items():
            for task in tenant_info['cdc_tasks']:
                all_tasks.append((tenant_name, task))
        
        if not all_tasks:
            return
        
        # 随机选择一些任务暂停
        num_to_pause = max(1, len(all_tasks) // 3)
        tasks_to_pause = random.sample(all_tasks, min(num_to_pause, len(all_tasks)))
        
        for tenant_name, task in tasks_to_pause:
            if not task['paused']:
                self.pause_cdc_task(tenant_name, task['task_name'])
                time.sleep(1)
        
        # 等待一段时间
        logger.info(f"等待 {TASK_PAUSE_INTERVAL} 秒后恢复任务...")
        time.sleep(TASK_PAUSE_INTERVAL)
        
        # 恢复暂停的任务
        for tenant_name, task in tasks_to_pause:
            if task['paused']:
                self.resume_cdc_task(tenant_name, task['task_name'])
                time.sleep(1)
    
    def run_test_loop(self):
        """运行测试循环"""
        logger.info("=" * 80)
        logger.info("开始CDC GC高级测试循环")
        logger.info("=" * 80)
        
        # 启动数据插入线程（在独立线程中运行，不会阻塞）
        data_threads = self.start_data_insertion()
        
        # 等待任务启动
        logger.info("等待CDC任务启动...")
        time.sleep(10)
        
        cycle = 0
        while self.running:
            cycle += 1
            logger.info("=" * 80)
            logger.info(f"测试循环 #{cycle}")
            logger.info("=" * 80)
            
            # 检查数据插入线程是否还在运行
            alive_threads = sum(1 for t in data_threads if t.is_alive())
            if alive_threads < len(data_threads):
                logger.warning(f"数据插入线程异常: {alive_threads}/{len(data_threads)} 个线程还在运行")
                # 重新启动数据插入线程
                logger.info("重新启动数据插入线程...")
                data_threads = self.start_data_insertion()
            
            # 检查水位（在独立线程中运行，不阻塞数据插入）
            logger.info("检查水位...")
            stalled_tasks, has_critical_issue = self.check_watermark_stall()
            if stalled_tasks:
                logger.warning(f"发现 {len(stalled_tasks)} 个水位问题:")
                for task in stalled_tasks:
                    logger.warning(f"  - {task}")
            
            # 如果发现严重问题（水位回退或长时间停滞），终止程序
            if has_critical_issue:
                logger.error("=" * 80)
                logger.error("❌ 检测到严重问题（水位回退或长时间停滞），终止测试程序")
                logger.error("=" * 80)
                logger.error("问题详情:")
                for task in stalled_tasks:
                    logger.error(f"  - {task}")
                logger.error("=" * 80)
                self.stop()
                # 退出程序
                sys.exit(1)
            
            # 随机检查行数一致性
            if random.random() < ROW_COUNT_CHECK_PROBABILITY:
                logger.info("随机检查行数一致性...")
                row_count_issues = self.random_check_row_counts()
                if row_count_issues:
                    logger.warning(f"发现 {len(row_count_issues)} 个行数差异过大的问题:")
                    for issue in row_count_issues:
                        logger.warning(f"  - {issue}")
                else:
                    logger.info("✓ 行数一致性检查通过")
            
            # 随机暂停和恢复任务（在独立线程中运行，不阻塞数据插入）
            if cycle % 2 == 0:  # 每2个循环执行一次
                logger.info("随机暂停和恢复任务...")
                # 在独立线程中执行，不阻塞主循环
                pause_thread = threading.Thread(
                    target=self.random_pause_resume_tasks,
                    daemon=True
                )
                pause_thread.start()
            
            # 等待下一次检查（数据插入线程继续运行）
            logger.info(f"等待 {WATERMARK_CHECK_INTERVAL} 秒后进行下一次检查（数据插入持续进行）...")
            time.sleep(WATERMARK_CHECK_INTERVAL)
    
    def stop(self):
        """停止测试"""
        self.running = False
        logger.info("测试停止")


def main():
    """主函数"""
    logger.info("开始CDC GC高级测试...")
    logger.info(f"数据库连接: {DB_USER}@{DB_HOST}:{DB_PORT}")
    logger.info("")
    
    # 创建数据库连接（使用系统账户）
    conn = CDCConnection(DB_HOST, DB_PORT, DB_USER, DB_PASS)
    if not conn.connect():
        logger.error("无法连接到数据库")
        sys.exit(1)
    
    try:
        tester = AdvancedCDCTester(conn)
        
        # 1. 设置租户
        if not tester.setup_tenants():
            logger.error("设置租户失败")
            sys.exit(1)
        
        # 2. 检查集群状态
        logger.info("=" * 80)
        logger.info("检查集群状态...")
        logger.info("=" * 80)
        
        # 恢复所有暂停的任务
        tester.resume_all_paused_tasks()
        
        # 加载现有任务到本地状态
        tester.load_existing_tasks()
        
        # 检查表是否已有数据
        tables_have_data = tester.check_all_tables_have_data()
        
        if tables_have_data:
            logger.info("所有表已有数据，跳过初始化，直接开始测试")
            # 如果表已有数据，只检查CDC任务
            existing_tasks = tester.check_existing_tasks()
            if not existing_tasks:
                logger.warning("没有发现CDC任务，需要创建新任务")
                if not tester.setup_cdc_tasks():
                    logger.error("CDC任务设置失败")
                    sys.exit(1)
        else:
            logger.info("表数据不完整，执行完整初始化...")
            # 设置数据库和表（会检查表是否存在，只创建不存在的表）
            if not tester.setup_databases_and_tables():
                logger.error("设置数据库和表失败")
                sys.exit(1)
            
            # 设置CDC任务（会检查任务是否存在，只创建不存在的任务）
            if not tester.setup_cdc_tasks():
                logger.error("设置CDC任务失败")
                sys.exit(1)
        
        logger.info("=" * 80)
        logger.info("初始化完成，开始测试循环")
        logger.info("=" * 80)
        
        # 3. 运行测试循环
        try:
            tester.run_test_loop()
        except KeyboardInterrupt:
            logger.info("\n测试被用户中断")
            tester.stop()
        
    except Exception as e:
        logger.error(f"测试异常: {e}", exc_info=True)
    finally:
        conn.close()
        logger.info("测试结束")


if __name__ == "__main__":
    main()

