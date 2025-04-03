
# wget https://dsi.ut-capitole.fr/blacklists/download/adult.tar.gz

# wget https://dsi.ut-capitole.fr/blacklists/download/phishing.tar.gz

# wget https://dsi.ut-capitole.fr/blacklists/download/dating.tar.gz

# wget https://dsi.ut-capitole.fr/blacklists/download/gambling.tar.gz

# wget https://dsi.ut-capitole.fr/blacklists/download/filehosting.tar.gz

# wget https://dsi.ut-capitole.fr/blacklists/download/ddos.tar.gz

# wget https://dsi.ut-capitole.fr/blacklists/download/agressif.tar.gz

# wget https://dsi.ut-capitole.fr/blacklists/download/chat.tar.gz

# wget https://dsi.ut-capitole.fr/blacklists/download/mixed_adult.tar.gz

# wget https://dsi.ut-capitole.fr/blacklists/download/arjel.tar.gz


#!/bin/bash

# 遍历当前目录中所有的 .tar.gz 文件
for file in *.tar.gz
do
    # 检查文件是否存在（以防止没有匹配的文件时的错误）
    if [ -f "$file" ]; then
        echo "Extracting $file..."
        
        # 获取文件名（不包括 .tar.gz 扩展名）
        filename="${file%.tar.gz}"
        
        # 创建一个与文件同名的目录
        # mkdir -p "$filename"
        
        # 解压文件到这个新目录
        tar -xzf "$file" 
        # -C "$filename"
        
        echo "Finished extracting $file"
    fi
done

echo "All .tar.gz files have been extracted."