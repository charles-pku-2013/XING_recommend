基于 XING 的推荐系统实现

目前进度：实现了数据集的导入和查询

开发语言：C++11
运行环境：MacOS, Linux, (可根据需要添加Windows支持)
依赖库：boost, glog

MacOS下编译:
1. 安装依赖库:
安装 MacPorts:
https://www.macports.org/
用 MacPorts 安装boost库:
            sudo port install boost
用 MacPorts 安装glog库:
            sudo port install google-glog

2. 修改 compile.macos, 将 targetDir 变量内容改成数据文件*.csv存放目录。
3. 检查 compile.macos 是否有可执行权限，若无，通过 chmod a+x compile.macos 命令加上
4. 编译完成后的可执行文件在数据目录下， 启动命令:
    GLOG_log_dir="." ./test.bin


Linux下编译:
1. 安装依赖库:
sudo apt-get install libboost-dev libgoogle-glog-dev
2. 修改 compile.linux, 将 targetDir 变量内容改成数据文件*.csv存放目录。
3. 检查 compile.linux 是否有可执行权限，若无，通过 chmod a+x compile.linux 命令加上
4. 编译完成后的可执行文件在数据目录下， 启动命令:
    GLOG_log_dir="." ./test.bin


运行测试:
NOTE!!! 运行之前请先用 correct_items 程序更正 items.csv 中属性文本缺失问题, 并确保 items.csv 文件是更正过后的，方法参考 correct_items.cpp 内说明。
本程序目前实现将数据导入内存，并建立了便于快速查找的数据结构。并实现了一个简单的交互查询系统(见main.cpp:handle_command), 目前支持命令：
u $user_id      打印用户信息和交互历史
i $item_id      打印物品信息和交互历史
q               退出

截取小数据集的实现在 main.cpp:gen_small_dataset 函数中实现
