import os

def list_files(dir_path):
    all_files = []
    
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            full_path = os.path.join(root, file)
            all_files.append(full_path)
            
    return all_files

dataset_path = 'dataset/dataset/Data/020/labels.txt'
all_files = list_files(dataset_path)

for file in all_files:
    print(file)
