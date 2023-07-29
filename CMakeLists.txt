cmake_minimum_required (VERSION 3.10)  # 你不写，build时会提醒你写，你自己当前的cmake版本号
project (kvdb)  # 设置项目名称
set(CMAKE_BUILD_TYPE "Debug")  # 设置debug模式，如果没有这一行将不能调试设断点
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++14")  # 设置g++编译选项

include_directories(${PROJECT_SOURCE_DIR}/include)  # 设置头文件路径
link_directories(${PROJECT_SOURCE_DIR}/lib)#设置库文件搜索路径
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)#设置库文件输出路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)#设置可执行文件输出的路径


add_subdirectory(src)  
add_executable(store_server store_server.cc)#生成可执行程序main
target_link_libraries(store_server kvdb muduo_net muduo_base pthread)#库是由右到左链接的

# 进入子目录下执行CMakeLists.txt文件
# add_subdirectory(tests)