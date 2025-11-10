#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CDC GC 功能测试脚本 (Python版本)
支持并发操作、长时间运行和详细的错误处理
"""

import os
import sys
import time
import logging
import threading
from typing import Optional, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from pymysql import Error as PyMySQLError

# 配置参数
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "6001"))
DB_USER = os.getenv("DB_USER", "dump")
DB_PASS = os.getenv("DB_PASS", "111")
ACCOUNT_ID = int(os.getenv("ACCOUNT_ID", "0"))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class CDCConnection:
    """数据库连接管理类"""
    
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self._connection = None
    
    def connect(self):
        """建立数据库连接"""
        try:
            self._connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            logger.info(f"数据库连接成功: {self.user}@{self.host}:{self.port}")
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
            # 返回第一行第一列的值
            first_row = result[0]
            if first_row:
                return list(first_row.values())[0]
        return None
    
    def close(self):
        """关闭数据库连接"""
        if self._connection:
            self._connection.close()
            self._connection = None


class CDCTester:
    """CDC测试类"""
    
    def __init__(self, conn: CDCConnection):
        self.conn = conn
        self.account_id = ACCOUNT_ID
    
    def get_task_id_by_name(self, task_name: str) -> Optional[str]:
        """根据task_name获取task_id"""
        sql = f"SELECT task_id FROM mo_catalog.mo_cdc_task WHERE account_id={self.account_id} AND task_name='{task_name}' LIMIT 1"
        task_id = self.conn.execute_sql_silent(sql)
        if not task_id:
            logger.warning(f"未找到任务: {task_name}")
        return task_id
    
    def create_cdc_task(self, task_name: str, source_uri: str, source_db: str, 
                       sink_uri: str, sink_db: str, level: str = "database") -> bool:
        """创建CDC任务"""
        sql = f"CREATE CDC {task_name} '{source_uri}' 'matrixone' '{sink_uri}' '{source_db}:{sink_db}' {{'Level'='{level}'}};"
        logger.info(f"创建CDC任务: {task_name}")
        result = self.conn.execute_sql(sql)
        return result is not None
    
    def pause_cdc_task(self, task_name: str) -> bool:
        """暂停CDC任务"""
        sql = f"PAUSE CDC TASK {task_name};"
        logger.info(f"暂停CDC任务: {task_name}")
        result = self.conn.execute_sql(sql)
        return result is not None
    
    def resume_cdc_task(self, task_name: str) -> bool:
        """重启CDC任务"""
        sql = f"RESUME CDC TASK {task_name};"
        logger.info(f"重启CDC任务: {task_name}")
        result = self.conn.execute_sql(sql)
        return result is not None
    
    def query_watermark(self, task_name: str, db_name: Optional[str] = None, 
                       table_name: Optional[str] = None) -> Optional[List[Dict]]:
        """查询CDC watermark"""
        # 先获取task_id
        task_id = self.get_task_id_by_name(task_name)
        if not task_id:
            return None
        
        if table_name:
            # 查询特定表的watermark
            sql = f"SELECT watermark FROM mo_catalog.mo_cdc_watermark WHERE account_id={self.account_id} AND task_id='{task_id}' AND db_name='{db_name}' AND table_name='{table_name}' LIMIT 1"
            result = self.conn.execute_sql(sql, fetch=True)
            return result
        elif db_name:
            # 查询数据库所有表的watermark
            sql = f"SELECT db_name, table_name, watermark, err_msg FROM mo_catalog.mo_cdc_watermark WHERE account_id={self.account_id} AND task_id='{task_id}' AND db_name='{db_name}'"
            result = self.conn.execute_sql(sql, fetch=True)
            if result:
                logger.info(f"任务 {task_name} 在数据库 {db_name} 的watermark:")
                for row in result:
                    logger.info(f"  表 {row.get('table_name')}: watermark={row.get('watermark')}, err_msg={row.get('err_msg')}")
            return result
        else:
            # 查询任务所有watermark
            sql = f"SELECT db_name, table_name, watermark, err_msg FROM mo_catalog.mo_cdc_watermark WHERE account_id={self.account_id} AND task_id='{task_id}'"
            result = self.conn.execute_sql(sql, fetch=True)
            if result:
                logger.info(f"任务 {task_name} 的所有watermark:")
                for row in result:
                    logger.info(f"  {row.get('db_name')}.{row.get('table_name')}: watermark={row.get('watermark')}, err_msg={row.get('err_msg')}")
            return result
    
    def wait_for_watermark(self, task_name: str, db_name: str, table_name: str, 
                          timeout: int = 300, check_interval: int = 10) -> bool:
        """等待watermark更新（带超时）"""
        logger.info(f"等待任务 {task_name} 表 {db_name}.{table_name} 的watermark更新（最多{timeout}秒）...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            result = self.query_watermark(task_name, db_name, table_name)
            if result and len(result) > 0:
                watermark = result[0].get('watermark')
                if watermark:
                    logger.info(f"✓ watermark已更新: {watermark}")
                    return True
            
            time.sleep(check_interval)
            elapsed = int(time.time() - start_time)
            if elapsed % 30 == 0:  # 每30秒打印一次
                logger.info(f"已等待 {elapsed} 秒，继续等待...")
        
        logger.warning(f"⚠ watermark更新超时（{timeout}秒）")
        return False
    
    def insert_data(self, db_name: str, table_name: str, data: List[Tuple]) -> bool:
        """插入数据"""
        try:
            # 切换到目标数据库
            self.conn.execute_sql(f"USE {db_name}")
            
            for row in data:
                if len(row) == 2:
                    sql = f"INSERT INTO {table_name} (id, name) VALUES ({row[0]}, '{row[1]}')"
                elif len(row) == 3:
                    sql = f"INSERT INTO {table_name} (id, name, data) VALUES ({row[0]}, '{row[1]}', '{row[2]}')"
                else:
                    continue
                
                result = self.conn.execute_sql(sql)
                if result is None:
                    logger.error(f"插入数据失败: {sql}")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"插入数据异常: {e}")
            return False
    
    def concurrent_insert(self, db_name: str, table_name: str, num_threads: int = 5, 
                         inserts_per_thread: int = 10) -> bool:
        """并发插入数据"""
        logger.info(f"开始并发插入数据到 {db_name}.{table_name} ({num_threads}个线程，每个线程{inserts_per_thread}条)")
        
        def insert_worker(thread_id: int):
            """工作线程"""
            worker_conn = CDCConnection(DB_HOST, DB_PORT, DB_USER, DB_PASS)
            if not worker_conn.connect():
                return False
            
            try:
                worker_conn.execute_sql(f"USE {db_name}")
                for i in range(inserts_per_thread):
                    id_val = thread_id * 1000 + i
                    sql = f"INSERT INTO {table_name} (id, name, data) VALUES ({id_val}, 'thread{thread_id}_name{i}', 'thread{thread_id}_data{i}')"
                    result = worker_conn.execute_sql(sql)
                    if result is None:
                        logger.error(f"线程 {thread_id} 插入失败: {sql}")
                        return False
                    time.sleep(0.1)  # 稍微延迟
                logger.info(f"线程 {thread_id} 完成插入")
                return True
            except Exception as e:
                logger.error(f"线程 {thread_id} 异常: {e}")
                return False
            finally:
                worker_conn.close()
        
        # 使用线程池并发执行
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(insert_worker, i) for i in range(num_threads)]
            results = [f.result() for f in as_completed(futures)]
        
        success_count = sum(results)
        logger.info(f"并发插入完成: {success_count}/{num_threads} 个线程成功")
        return success_count == num_threads


def test_basic_cdc_protection(tester: CDCTester):
    """测试场景1: 基本CDC保护"""
    logger.info("=" * 60)
    logger.info("测试场景1: 基本CDC保护")
    logger.info("=" * 60)
    
    test_db = "test_cdc_db1"
    test_table = "test_table1"
    task_name = "test_cdc_task1"
    sink_db = f"{test_db}_bak"
    source_uri = f"mysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    sink_uri = f"mysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    
    # 创建测试数据库和表
    tester.conn.execute_sql(f"CREATE DATABASE IF NOT EXISTS {test_db}")
    tester.conn.execute_sql(f"CREATE DATABASE IF NOT EXISTS {sink_db}")
    tester.conn.execute_sql(f"USE {test_db}; CREATE TABLE IF NOT EXISTS {test_table} (id INT PRIMARY KEY, name VARCHAR(100), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    
    # 创建CDC任务
    if not tester.create_cdc_task(task_name, source_uri, test_db, sink_uri, sink_db):
        logger.error("创建CDC任务失败")
        return False
    
    # 等待任务启动
    logger.info("等待任务启动...")
    time.sleep(3)
    
    # 插入测试数据
    logger.info("插入测试数据...")
    data = [(i, f'data{i}') for i in range(1, 6)]
    tester.insert_data(test_db, test_table, data)
    
    # 等待watermark更新
    tester.wait_for_watermark(task_name, test_db, test_table, timeout=120)
    
    # 查询watermark
    tester.query_watermark(task_name, test_db)
    
    logger.info("场景1测试完成\n")
    return True


def test_concurrent_multi_table(tester: CDCTester):
    """测试场景2: 并发多表操作"""
    logger.info("=" * 60)
    logger.info("测试场景2: 并发多表操作")
    logger.info("=" * 60)
    
    test_db = "test_cdc_db2"
    task_name = "test_cdc_task2"
    sink_db = f"{test_db}_bak"
    source_uri = f"mysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    sink_uri = f"mysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    num_tables = 5
    
    # 创建测试数据库和表
    tester.conn.execute_sql(f"CREATE DATABASE IF NOT EXISTS {test_db}")
    tester.conn.execute_sql(f"CREATE DATABASE IF NOT EXISTS {sink_db}")
    
    for i in range(1, num_tables + 1):
        table_name = f"test_table{i}"
        tester.conn.execute_sql(f"USE {test_db}; CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, name VARCHAR(100), data VARCHAR(200))")
    
    # 创建CDC任务
    if not tester.create_cdc_task(task_name, source_uri, test_db, sink_uri, sink_db):
        logger.error("创建CDC任务失败")
        return False
    
    # 等待任务启动
    logger.info("等待任务启动...")
    time.sleep(3)
    
    # 并发插入数据到多个表
    logger.info("并发插入数据到多个表...")
    
    def insert_table_data(table_id: int):
        """为单个表插入数据"""
        table_name = f"test_table{table_id}"
        logger.info(f"开始为表 {table_name} 插入数据...")
        tester.concurrent_insert(test_db, table_name, num_threads=3, inserts_per_thread=5)
        logger.info(f"表 {table_name} 数据插入完成")
    
    # 并发操作多个表
    with ThreadPoolExecutor(max_workers=num_tables) as executor:
        futures = [executor.submit(insert_table_data, i) for i in range(1, num_tables + 1)]
        for f in as_completed(futures):
            f.result()
    
    # 等待watermark更新
    logger.info("等待watermark更新...")
    time.sleep(30)
    
    # 查询所有表的watermark
    tester.query_watermark(task_name, test_db)
    
    logger.info("场景2测试完成\n")
    return True


def test_long_running(tester: CDCTester, duration_minutes: int = 30):
    """测试场景3: 长时间运行测试"""
    logger.info("=" * 60)
    logger.info(f"测试场景3: 长时间运行测试（{duration_minutes}分钟）")
    logger.info("=" * 60)
    
    test_db = "test_cdc_db3"
    task_name = "test_cdc_task3"
    sink_db = f"{test_db}_bak"
    source_uri = f"mysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    sink_uri = f"mysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    
    # 创建测试数据库和表
    tester.conn.execute_sql(f"CREATE DATABASE IF NOT EXISTS {test_db}")
    tester.conn.execute_sql(f"CREATE DATABASE IF NOT EXISTS {sink_db}")
    tester.conn.execute_sql(f"USE {test_db}; CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, name VARCHAR(100), data VARCHAR(200), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    
    # 创建CDC任务
    if not tester.create_cdc_task(task_name, source_uri, test_db, sink_uri, sink_db):
        logger.error("创建CDC任务失败")
        return False
    
    # 等待任务启动
    logger.info("等待任务启动...")
    time.sleep(3)
    
    # 长时间运行：持续插入数据并监控watermark
    logger.info(f"开始长时间运行测试，将持续 {duration_minutes} 分钟...")
    logger.info("请查看CDC任务日志确认是否有错误")
    
    start_time = time.time()
    end_time = start_time + duration_minutes * 60
    insert_count = 0
    
    while time.time() < end_time:
        # 并发插入数据
        logger.info(f"第 {insert_count + 1} 轮数据插入...")
        tester.concurrent_insert(test_db, "test_table", num_threads=5, inserts_per_thread=10)
        insert_count += 1
        
        # 每5轮查询一次watermark
        if insert_count % 5 == 0:
            logger.info("查询watermark状态...")
            tester.query_watermark(task_name, test_db)
        
        # 等待一段时间再继续
        time.sleep(60)  # 每分钟一轮
    
    elapsed = int(time.time() - start_time)
    logger.info(f"长时间运行测试完成，共运行 {elapsed} 秒，插入 {insert_count} 轮数据")
    logger.info("场景3测试完成\n")
    return True


def main():
    """主函数"""
    logger.info("开始CDC GC功能测试...")
    logger.info(f"数据库连接: {DB_USER}@{DB_HOST}:{DB_PORT}")
    logger.info(f"账户ID: {ACCOUNT_ID}")
    logger.info("")
    
    # 创建数据库连接
    conn = CDCConnection(DB_HOST, DB_PORT, DB_USER, DB_PASS)
    if not conn.connect():
        logger.error("无法连接到数据库")
        sys.exit(1)
    
    # 检查必要的表是否存在
    result = conn.execute_sql("SELECT COUNT(*) as cnt FROM mo_catalog.mo_cdc_watermark LIMIT 1", fetch=True)
    if not result:
        logger.error("mo_cdc_watermark表不存在，请确保CDC功能已启用")
        sys.exit(1)
    
    try:
        tester = CDCTester(conn)
        
        # 运行测试场景
        logger.info("开始运行测试场景...\n")
        
        # 场景1: 基本CDC保护
        test_basic_cdc_protection(tester)
        
        # 场景2: 并发多表操作
        test_concurrent_multi_table(tester)
        
        # 场景3: 长时间运行测试（30分钟）
        # 可以通过环境变量设置运行时长，例如: export TEST_DURATION=60
        duration = int(os.getenv("TEST_DURATION", "30"))
        test_long_running(tester, duration_minutes=duration)
        
        logger.info("=" * 60)
        logger.info("所有测试完成！")
        logger.info("=" * 60)
        logger.info("注意: 测试任务已创建但未删除，可以继续使用或手动清理")
        logger.info("请查看CDC任务日志确认是否有错误")
        
    except KeyboardInterrupt:
        logger.info("\n测试被用户中断")
    except Exception as e:
        logger.error(f"测试异常: {e}", exc_info=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

