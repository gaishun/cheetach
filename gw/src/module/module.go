package module

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

var Modules = []*Module{
	startAllDescription,
	version,
}

type Module struct {
	Run       func(mdl *Module, args []string) bool
	UsageLine string
	Short     string
	Long      string
	Flag      flag.FlagSet
	IsDebug   *bool
}

/*
 * name :			Name
 * Description :	获取模块名
 * input :
 * output :
 * return :
 * creator :		HuangChunhua
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func (m *Module) Name() string {
	fmt.Println(m.UsageLine)
	name := m.UsageLine
	i := strings.Index(name, " ")
	if i >= 0 {
		name = name[:i]
	}
	return name
}

/*
 * name :			Usage
 * Description :
 * input :
 * output :
 * return :
 * creator :		HuangChunhua
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func (m *Module) Usage() {
	fmt.Fprintf(os.Stderr, "Example: gproxy %s\n", m.UsageLine)
	fmt.Fprintf(os.Stderr, "Default Usage:\n")
	m.Flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Description:\n")
	fmt.Fprintf(os.Stderr, "  %s\n", strings.TrimSpace(m.Long))
	os.Exit(2)
}

/*
 * name :			Runable
 * Description :	判断模块是否可以运行
 * input :
 * output :
 * return :
 * creator :		HuangChunhua
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func (m *Module) Runnable() bool {
	return m.Run != nil
}
