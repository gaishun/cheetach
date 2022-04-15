package module

import (
	"fmt"
	"runtime"
	"../service-direct"
)

var version = &Module{
	Run:       runVersion,
	UsageLine: "version",
	Short:     "print Gift Proxy version",
	Long:      `Version prints the Gift Proxy version`,
}

/*
 * name :			runVersion
 * Description :    获取当前版本号
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
func runVersion(cmd *Module, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	fmt.Printf("version %s %s %s\n", service_direct.VERSION, runtime.GOOS, runtime.GOARCH)
	return true
}
