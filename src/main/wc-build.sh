# 将wc.go构建成为插件
# 后续可能遇到插件无法加载https://blog.csdn.net/dyq94310/article/details/125086718
go build -buildmode=plugin ../mrapps/wc.go

# 删除之前的输出文件
rm mr-out*

# 执行协调者，并传入输入文件作为参数
go run mrcoordinator.go pg-*.txt

# 执行工作者，并输入插件作为参数
go run mrworker.go wc.so

# 运行检测是否通过lab
bash test-mr.sh