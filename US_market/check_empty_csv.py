import os

def list_csv_files(directory):
    csv_files = []
    small_files = []

    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            csv_files.append(file_path)
            if os.path.getsize(file_path) < 2048:  # 檔案大小小於 2 KB
                small_files.append(file_path)

    tmp_csv_files = []
    for csv_file in csv_files:
        tmp_csv_files.append(csv_file.split("_")[3].strip(".csv"))

    tmp_small_files = []
    for small_file in small_files:
        tmp_small_files.append(small_file.split("_")[3].strip(".csv"))

    return tmp_csv_files, tmp_small_files

directory = 'C:\\Users\\kai\\Downloads'  # 將這裡替換為你的資料夾路徑
csv_files, small_files = list_csv_files(directory)

print("所有的 CSV 檔案:")
for file in csv_files:
    print(file)

print("\n大小小於 2 KB 的檔案:")
print(small_files)