aux_source_directory(. SRC_LIST)  # 定义变量 存储当前目录下所有源文件
add_library(kvdb STATIC ${SRC_LIST})#生成静态库