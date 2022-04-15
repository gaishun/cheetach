package main

import (
	"../config"
	"flag"
	"fmt"
	"io"
	"lib/glog"
	"math/rand"
	"../module"
	"os"
	"pkg/util"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"
)

var modules = module.Modules

var exitStatus = 0
var exitMu sync.Mutex

func setExitStatus(n int) {
	exitMu.Lock()
	if exitStatus < n {
		exitStatus = n
	}
	exitMu.Unlock()
}

var usageTemplate = `
Gift Proxy

Usage:

	gproxy module [arguments]

The module are:
{{range .}}{{if .Runnable}}
    {{.Name | printf "%-11s"}} {{.Short}}{{end}}{{end}}

Use "gproxy help [command]" for more information about a command.

`

var helpTemplate = `{{if .Runnable}}Usage: gproxy {{.UsageLine}}
{{end}}
  {{.Long}}
`

/*
 * name :			capitalize
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
func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}

/*
 * name :			tmpl
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
func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("gift")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, modules)
}

/*
 * name :			usage
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
func usage() {
	printUsage(os.Stderr)
	fmt.Fprintf(os.Stderr, "For Logging, use \"gproxy [logging_options] [command]\". The logging options are:\n")
	flag.PrintDefaults()
	os.Exit(2)
}

/*
 * name :			help
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
func help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		return
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: gproxy help command\n\nToo many arguments given.\n")
		os.Exit(2) // failed at 'gproxy help'
	}

	arg := args[0]

	for _, md := range modules {
		if md.Name() == arg {
			tmpl(os.Stdout, helpTemplate, md)
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic %#q.  Run 'gift-gproxy help'.\n", arg)
	os.Exit(2) // failed at 'gproxy help cmd'
}

var atexitFuncs []func()

func exit() {
	for _, f := range atexitFuncs {
		f()
	}
	glog.Flush()
	os.Exit(exitStatus)
}

/*
 * name :			main
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
func main() {
	glog.MaxSize = 1024 * 1024 * 1024
	rand.Seed(time.Now().UnixNano())

	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	if args[0] == "help" {
		help(args[1:])
		for _, md := range modules {
			if len(args) >= 2 && md.Name() == args[1] && md.Run != nil {
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				md.Flag.PrintDefaults()
			}
		}
		return
	}

	for _, md := range modules {
		if md.Name() == args[0] && md.Run != nil {
			md.Flag.Usage = func() { md.Usage() }
			md.Flag.Parse(args[1:])
			args = md.Flag.Args()
			config.QpsLimit = util.NewQpsLimit()

			if !md.Run(md, args) {
				fmt.Fprintf(os.Stderr, "\n")
				md.Flag.Usage()
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				md.Flag.PrintDefaults()
			}
			exit()
			return
		}
	}
	fmt.Fprintf(os.Stderr, "gproxy: unknown module %q\nRun 'gproxy help' for usage.\n", args[0])
	setExitStatus(2)
	exit()
}
