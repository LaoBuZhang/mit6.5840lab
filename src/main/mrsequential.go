package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.5840/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//单机版的word count
func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	//通过输入的参数来获得插件wc.so
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//用来保存最终结果的键值对数组
	intermediate := []mr.KeyValue{}
	//读取传入的多个文件pg*.txt
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		//调用wc.go中的map方法得到单个文件的键值对数组
		kva := mapf(filename, string(content))
		//加入最终的键值对数组
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//按键值对结果进行排序
	sort.Sort(ByKey(intermediate))

	//创建输出文件
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		//value的长度即为当前单词的出现此书
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value) //出现一次长度加1
		}
		//调用reduce方法，返回的是单词出现的次数
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		//将 单词 出现次数\n 写入输出文件
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		//下一个单词
		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//返回值是两个函数
//第一个是传入两个string参数，返回键值对数组，对应wc.go中的map方法
//第二个是传入一个string，一个string数组作为参数，返回单词出现的次数,对应wc.go中的reduce方法
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	//加载插件
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}

	//加载map方法
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)

	//加载reduce方法
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
