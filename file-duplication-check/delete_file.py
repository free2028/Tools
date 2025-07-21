import json
import os

# --- 配置 ---
# 请将 'path/to/your/data.json' 替换为你实际的 JSON 文件路径
JSON_FILE_PATH = './midfile/duplicates.json'

# --- 核心逻辑 ---

try:
    # 1. 从指定路径读取 JSON 文件内容
    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        json_data_string = f.read()

    # 2. 加载 JSON 数据
    data = json.loads(json_data_string)

    # 3. 创建一个新的字典来存储处理后的结果 (仅包含保留的文件)
    kept_files_data = {}
    files_deleted_count = 0
    processed_md5_count = 0 # 统计实际处理的 MD5 组数

    # 4. 遍历 "duplicates" 字典
    if "duplicates" in data and isinstance(data["duplicates"], dict):
        for md5_hash, file_list in data["duplicates"].items():
            processed_md5_count += 1 # 增加处理的 MD5 计数

            if not file_list or not isinstance(file_list, list):
                # 如果文件列表为空或不是列表，跳过此 MD5
                print(f"警告: MD5 '{md5_hash}' 的文件列表无效或为空，跳过。")
                continue

            # 保留第一个文件
            first_file_path = file_list[0]
            kept_files_data[md5_hash] = [first_file_path] # 结构保持一致

            # 5. 遍历该 MD5 值下的所有其他文件（从第二个开始）并删除
            for file_to_delete in file_list[1:]:
                try:
                    if os.path.exists(file_to_delete):
                        os.remove(file_to_delete)
                        print(f"已删除: {file_to_delete}")
                        files_deleted_count += 1
                    else:
                        print(f"警告: 文件不存在，无法删除: {file_to_delete}")
                except OSError as e:
                    print(f"错误：删除文件 {file_to_delete} 时发生操作系统错误: {e}")
                except Exception as e:
                    print(f"错误：删除文件 {file_to_delete} 时发生未知错误: {e}")
    else:
        print("警告: JSON 数据中未找到 'duplicates' 键，或者其值不是一个字典。")


    # 6. 构建新的 JSON 结构，更新计数器
    new_json_structure = {
        "found_duplicates_count": len(kept_files_data), # 更新为实际保留的 MD5 条目数
        "duplicates": kept_files_data
    }

    # --- 输出结果 ---
    print("\n--- 操作完成 ---")
    print(f"已读取 JSON 文件: {JSON_FILE_PATH}")
    print(f"总共扫描了 {processed_md5_count} 个 MD5 组。")
    print(f"成功保留了 {len(kept_files_data)} 个 MD5 组（每个组只保留第一个文件）。")
    print(f"总共尝试删除 {files_deleted_count} 个文件。")

    # 打印处理后的 JSON 结构
    print("\n--- 处理后的 JSON 结构 ---")
    print(json.dumps(new_json_structure, indent=4))

    # 如果你想将处理后的 JSON 结构保存到新文件，可以取消下面这行的注释
    # 请指定你想要保存到的新文件路径
    # NEW_JSON_OUTPUT_PATH = 'path/to/your/processed_data.json'
    # with open(NEW_JSON_OUTPUT_PATH, 'w', encoding='utf-8') as outfile:
    #     json.dump(new_json_structure, outfile, indent=4)
    # print(f"\n处理后的 JSON 已保存到 {NEW_JSON_OUTPUT_PATH}")

except FileNotFoundError:
    print(f"错误: 指定的 JSON 文件未找到，请检查路径: {JSON_FILE_PATH}")
except json.JSONDecodeError:
    print(f"错误: 无法解析 JSON 数据。请检查文件 '{JSON_FILE_PATH}' 的格式是否正确。")
except Exception as e:
    print(f"发生了一个意外错误: {e}")
