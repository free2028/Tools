import os
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from pathlib import Path
import time

class DuplicateImageFinder:
    def __init__(self, max_workers=None):
        """
        初始化重复图片查找器
        
        Args:
            max_workers: 最大工作线程数，默认为None（自动选择）
        """
        self.max_workers = max_workers
        self.image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'}
        self.md5_dict = defaultdict(list)
        self.lock = threading.Lock()
        
    def calculate_md5(self, file_path):
        """
        计算文件的MD5值
        
        Args:
            file_path: 文件路径
            
        Returns:
            tuple: (md5值, 文件路径)
        """
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                # 分块读取文件，避免大文件占用过多内存
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest(), file_path
        except Exception as e:
            print(f"计算MD5失败 {file_path}: {e}")
            return None, file_path
    
    def is_image_file(self, file_path):
        """
        判断文件是否为图片文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 是否为图片文件
        """
        return Path(file_path).suffix.lower() in self.image_extensions
    
    def collect_image_files(self, directories):
        """
        收集所有目录中的图片文件
        
        Args:
            directories: 目录列表
            
        Returns:
            list: 图片文件路径列表
        """
        image_files = []
        
        for directory in directories:
            if not os.path.exists(directory):
                print(f"目录不存在: {directory}")
                continue
                
            print(f"扫描目录: {directory}")
            
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    if self.is_image_file(file_path):
                        image_files.append(file_path)
        
        return image_files
    
    def process_images(self, image_files):
        """
        多线程处理图片文件，计算MD5值
        
        Args:
            image_files: 图片文件路径列表
        """
        print(f"开始处理 {len(image_files)} 个图片文件...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_file = {
                executor.submit(self.calculate_md5, file_path): file_path 
                for file_path in image_files
            }
            
            # 处理完成的任务
            completed = 0
            for future in as_completed(future_to_file):
                md5_value, file_path = future.result()
                
                if md5_value:
                    with self.lock:
                        self.md5_dict[md5_value].append(file_path)
                
                completed += 1
                if completed % 100 == 0:
                    print(f"已处理: {completed}/{len(image_files)} 个文件")
        
        print(f"所有文件处理完成！")
    
    def find_duplicates(self):
        """
        查找重复的图片
        
        Returns:
            dict: 重复图片的MD5值和对应的文件路径
        """
        duplicates = {
            md5_value: paths 
            for md5_value, paths in self.md5_dict.items() 
            if len(paths) > 1
        }
        return duplicates
    
    def get_sorted_md5_list(self):
        """
        获取排序后的MD5值列表
        
        Returns:
            list: 排序后的(md5值, 文件路径列表)元组列表
        """
        return sorted(self.md5_dict.items(), key=lambda x: x[0])
    
    def print_results(self):
        """
        打印结果
        """
        print(f"\n{'='*60}")
        print("MD5值统计:")
        print(f"{'='*60}")
        
        # 打印所有MD5值（排序后）
        sorted_md5_list = self.get_sorted_md5_list()
        for md5_value, paths in sorted_md5_list:
            print(f"MD5: {md5_value}")
            for path in paths:
                print(f"  -> {path}")
            print()
        
        # 查找并打印重复的图片
        duplicates = self.find_duplicates()
        
        print(f"\n{'='*60}")
        print("重复图片检测结果:")
        print(f"{'='*60}")
        
        if duplicates:
            print(f"发现 {len(duplicates)} 组重复图片:")
            print()
            
            for md5_value, paths in duplicates.items():
                print(f"MD5: {md5_value}")
                print(f"重复数量: {len(paths)}")
                for i, path in enumerate(paths, 1):
                    print(f"  {i}. {path}")
                print("-" * 40)
        else:
            print("没有发现重复的图片！")
    
    def run(self, directories):
        """
        运行重复图片查找
        
        Args:
            directories: 目录列表
        """
        start_time = time.time()
        
        # 收集图片文件
        image_files = self.collect_image_files(directories)
        
        if not image_files:
            print("没有找到图片文件！")
            return
        
        # 处理图片文件
        self.process_images(image_files)
        
        # 打印结果
        self.print_results()
        
        end_time = time.time()
        print(f"\n总耗时: {end_time - start_time:.2f} 秒")
        print(f"处理图片数量: {len(image_files)}")
        print(f"唯一MD5数量: {len(self.md5_dict)}")


def main():
    """
    主函数
    """
    # 配置要扫描的目录
    directories = [
        "path/to/directory1",
        "path/to/directory2",
        "path/to/directory3",
        # 添加更多目录...
    ]
    
    # 创建查找器实例（可以指定线程数）
    finder = DuplicateImageFinder(max_workers=8)  # 使用8个线程, 可修改
    
    # 运行查找
    finder.run(directories)


if __name__ == "__main__":
    main()
