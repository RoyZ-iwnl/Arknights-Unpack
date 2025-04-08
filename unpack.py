import UnityPy
from pathlib import Path
import logging
import gc
import os
import time
from typing import Optional
from functools import wraps

# 设置明日方舟特定的环境变量
os.environ['UNITYPY_AK'] = '1'

# 设置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('unity_extractor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class AssetExtractionError(Exception):
    """自定义异常类，用于资源提取错误"""
    pass

def retry_on_error(max_attempts=3, delay=1):
    """
    装饰器：在发生错误时进行重试
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    logging.warning(f"第{attempt + 1}次尝试失败: {e}, {delay}秒后重试...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def check_file_validity(file_path: Path) -> bool:
    """
    检查文件有效性
    """
    try:
        if not file_path.exists():
            logging.error(f"文件不存在: {file_path}")
            return False
        
        if file_path.stat().st_size == 0:
            logging.error(f"文件为空: {file_path}")
            return False
        
        # 尝试打开文件检查是否可读
        with open(file_path, 'rb') as f:
            # 读取前几个字节检查文件头
            header = f.read(4)
            if len(header) < 4:
                logging.error(f"文件损坏或太小: {file_path}")
                return False
            
            # Unity资源文件通常以特定字节开始
            if not (header.startswith(b'Unit') or header.startswith(b'\x00\x00\x00\x00')):
                logging.warning(f"文件可能不是有效的Unity资源: {file_path}")
                # 不返回False，因为有些ab文件可能是加密的
        
        return True
        
    except Exception as e:
        logging.error(f"检查文件时出错 {file_path}: {e}")
        return False

def create_output_directory(output_dir: Path) -> bool:
    """
    创建输出目录
    """
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logging.error(f"创建输出目录失败: {output_dir}, 错误: {e}")
        return False

def safe_extract_asset(obj) -> bool:
    """
    安全地检查资源类型
    """
    try:
        if hasattr(obj, 'type'):
            return obj.type.name in ["Texture2D", "Sprite"]
        elif hasattr(obj, 'object_reader'):
            return obj.object_reader.type.name in ["Texture2D", "Sprite"]
        return False
    except Exception:
        return False

def get_asset_name(data, obj) -> str:
    """
    获取资源名称
    """
    try:
        if hasattr(data, 'name') and data.name:
            return "".join(c for c in data.name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        return f"unnamed_asset_{obj.path_id}"
    except:
        return f"unnamed_asset_{obj.path_id}"

@retry_on_error(max_attempts=3, delay=1)
def process_ab_file(file_path: Path, output_base: Path) -> bool:
    """
    处理单个.ab文件
    """
    try:
        if not check_file_validity(file_path):
            return False
            
        logging.info(f"开始处理文件: {file_path}")
        
        # 保持原始目录结构
        try:
            # 获取相对路径
            relative_path = file_path.relative_to(file_path.parent.parent)
            # 创建对应的输出目录
            asset_output_dir = output_base / relative_path.parent / relative_path.stem
            if not create_output_directory(asset_output_dir):
                return False

            # 加载文件
            env = UnityPy.load(str(file_path))
            
            # 遍历所有资源
            extracted = False
            for obj in env.objects:
                try:
                    if not safe_extract_asset(obj):
                        continue
                        
                    # 读取资源数据
                    data = obj.read()
                    
                    # 获取资源名称
                    safe_name = get_asset_name(data, obj)
                    
                    # 设置输出路径
                    output_path = asset_output_dir / f"{safe_name}.png"
                    counter = 1
                    while output_path.exists():
                        output_path = asset_output_dir / f"{safe_name}_{counter}.png"
                        counter += 1
                    
                    # 保存图像
                    if hasattr(data, 'image'):
                        data.image.save(str(output_path))
                        extracted = True
                        logging.info(f"已导出: {output_path}")
                
                except Exception as e:
                    logging.error(f"处理资源时出错 (path_id: {obj.path_id}): {e}")
                    continue
            
            return extracted
                
        except Exception as e:
            logging.error(f"导出资源时出错: {e}")
            return False
            
    except Exception as e:
        logging.error(f"处理文件 {file_path} 时出错: {e}")
        return False
    finally:
        gc.collect()

def process_ab_files(root_folder: str, output_folder: str = "output"):
    """
    处理目录下所有的.ab文件
    """
    try:
        root_path = Path(root_folder)
        output_base = Path(output_folder)
        
        if not root_path.exists():
            raise AssetExtractionError(f"输入目录不存在: {root_path}")
        
        # 获取所有.ab文件
        ab_files = list(root_path.rglob("*.ab"))
        total_files = len(ab_files)
        
        if total_files == 0:
            logging.warning(f"在 {root_folder} 中未找到.ab文件")
            return
        
        logging.info(f"找到 {total_files} 个.ab文件")
        
        # 分批处理文件以控制内存使用
        BATCH_SIZE = 5
        successful_files = 0
        failed_files = 0
        
        for i in range(0, total_files, BATCH_SIZE):
            batch = ab_files[i:i + BATCH_SIZE]
            current_batch = i // BATCH_SIZE + 1
            total_batches = (total_files + BATCH_SIZE - 1) // BATCH_SIZE
            
            logging.info(f"处理批次 {current_batch}/{total_batches}")
            
            for index, file_path in enumerate(batch, 1):
                try:
                    logging.info(f"处理进度: {i + index}/{total_files} ({((i + index)/total_files*100):.2f}%)")
                    if process_ab_file(file_path, output_base):
                        successful_files += 1
                    else:
                        failed_files += 1
                except Exception as e:
                    failed_files += 1
                    logging.error(f"处理文件失败 {file_path}: {e}")
                finally:
                    gc.collect()
            
            # 批次处理完后强制进行垃圾回收
            gc.collect()
            
            # 输出当前批次统计
            logging.info(f"当前批次 {current_batch}/{total_batches} 完成")
            logging.info(f"当前成功: {successful_files}, 失败: {failed_files}")
            
        # 输出最终统计
        logging.info("=" * 50)
        logging.info("处理完成！统计信息：")
        logging.info(f"总文件数: {total_files}")
        logging.info(f"成功处理: {successful_files}")
        logging.info(f"处理失败: {failed_files}")
        logging.info("=" * 50)
        
    except Exception as e:
        logging.error(f"处理过程中发生错误: {e}")
        raise

def main():
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='解包Unity AB文件工具')
    parser.add_argument('input', nargs='?', default='avg', help='输入目录路径 (默认: avg)')
    parser.add_argument('--output', '-o', default='output', help='输出目录路径 (默认: output)')
    parser.add_argument('--batch-size', '-b', type=int, default=5, help='批处理大小 (默认: 5)')
    
    args = parser.parse_args()
    
    try:
        process_ab_files(args.input, args.output)
    except KeyboardInterrupt:
        logging.info("用户中断处理")
        sys.exit(1)
    except Exception as e:
        logging.error(f"程序执行出错: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
