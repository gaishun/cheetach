package service_direct

import (
	"../config"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
)

const (
	DefaultMaxAge = 365 * 24 * 3600
)

/*
 * name :			getNamespace
 * Description :	获取ns配置
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func getNamespace(ns string) (conf *config.NSConf, has bool) {
	data := config.RequireConfigData()
	if data.Namespace == nil {
		return nil, false
	}
	nsObj, has := data.Namespace[ns]
	return &nsObj, has
}

func GetNamespace(ns string) (conf *config.NSConf, has bool) {
	return getNamespace(ns)
}

/*
 * name :			NamespaceExist
 * Description :	判断ns是否存在
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func NamespaceExist(ns string) bool {
	if _, has := getNamespace(ns); has {
		return true
	}
	return false
}

func IsLifeCycle(ns string) bool {
	nsObj, has := GetNamespace(ns)
	if !has {
		return false
	}
	if nsObj.LifeCycle == "" {
		return false
	}
	return true
}

func LifeCycle(ns string) int64 {
	nsObj, has := GetNamespace(ns)
	if !has {
		return 0
	}

	nslifecycle := NsLifeCycle{}
	if err := json.Unmarshal([]byte(nsObj.LifeCycle), &nslifecycle); err != nil {
		return 0
	}

	unit := nslifecycle.TTL[len(nslifecycle.TTL)-1]
	count, err := strconv.Atoi(nslifecycle.TTL[:len(nslifecycle.TTL)-1])
	if err != nil {
		return 0
	}

	return Second(string(unit), count)
}

func Second(unit string, count int) int64 {
	switch unit {
	case "":
		return 0
	case "m":
		return int64(count) * 60
	case "h":
		return int64(count) * 60 * 60
	case "d":
		return int64(count) * 24 * 60 * 60
	case "w":
		return int64(count) * 7 * 24 * 60 * 60
	case "M":
		return int64(count) * 30 * 24 * 60 * 60
	case "y":
		return int64(count) * 365 * 24 * 60 * 60
	}
	return 0
}

func CanPassWhiteList(ns, ip string) bool {
	nsObj, has := getNamespace(ns)
	if !has {
		return false
	}
	if !nsObj.WhiteListEnable {
		return true
	}
	if _, has := nsObj.WhiteList[ip]; !has {
		return false
	}

	return true
}

func IP(req *http.Request) string {
	ipsp := req.Header.Get("X-Forwarded-For")

	// not use client proxy, get RemoteAddr ip
	if ipsp == "" {
		if ip, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
			return ip
		}
	}

	// use client proxy, get from proxy client field.
	// X-Forwarded-For: client, proxy1, proxy2
	ips := strings.Split(ipsp, ",")
	if len(ips) > 0 && ips[0] != "" {
		ip, _, err := net.SplitHostPort(ips[0])
		if err != nil {
			ip = ips[0]
		}
		return ip
	}

	return req.RemoteAddr
}

/*
 * name :			NeedSecurity
 * Description :	判断ns是public|private
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func NeedSecurity(ns string) bool {
	if nsObj, has := getNamespace(ns); has {
		visibility := nsObj.Visibility
		if "public" == visibility {
			return false
		} else {
			return true
		}
	}
	return true
}

/*
 * name :			GetNSRegion
 * Description :	获取ns的region
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func GetNSRegion(ns string) string {
	if nsObj, has := getNamespace(ns); has {
		return nsObj.Region
	}
	return ""
}

/*
 * name :			GetMaxAge
 * Description :	获取CDN最大缓存时间
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func GetMaxAge(ns string) int64 {
	if nsObj, has := getNamespace(ns); has {
		maxAgeNum := nsObj.MaxAge
		return int64(maxAgeNum)
	}
	return DefaultMaxAge
}

/*
 * name :			GetNsSecretKey
 * Description :	获取ns的密钥，用于生成signature
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func GetNsSecretKey(ns string) string {
	if nsObj, has := getNamespace(ns); has {
		return nsObj.Secretkey
	}
	return ""
}

/*
 * name :			CanMimeAccept
 * Description :	mime类型检验
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func CanMimeAccept(ns string, mime string) bool {
	if nsObj, has := getNamespace(ns); has {
		mimeArray := nsObj.Accept
		if mimeArray == nil {
			return true
		} else {
			for _, value := range mimeArray {
				if mime == value {
					return true
				}
			}
			return false
		}

	}
	return false
}

/*
 * name :			GetDomain
 * Description :	获取下载域名
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func GetDomain(ns string) string {
	var nsObj *config.NSConf
	var has bool
	if nsObj, has = getNamespace(ns); has {
		if nsObj.Domain != "" {
			return nsObj.Domain
		}
	}
	defaultDomainMap := config.RequireConfigData().DefaultDomain
	if defaultDomainMap == nil {
		return ""
	}
	visibilityMap, has := defaultDomainMap[config.GetRegion()]
	if !has {
		return ""
	}
	domain, has := visibilityMap[nsObj.Visibility]
	if has {
		return domain
	}
	return ""
}

/*
 * name :			GetNsTimeout
 * Description :	获取ns的token时间(private)
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func GetNsTimeout(ns string) int64 {
	if nsObj, has := getNamespace(ns); has {
		return nsObj.Tokenexpire
	}
	return 0
}

/*
 * name :			GetDefaultSpec
 * Description :	获取ns的specs
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func GetDefaultSpec(ns string) map[string]string {
	if nsObj, has := getNamespace(ns); has {
		return nsObj.Spec
	}
	return map[string]string{}
}
