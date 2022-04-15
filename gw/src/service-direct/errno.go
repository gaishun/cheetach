package service_direct

type ErrorCode struct {
	StatusCode int
	StatusMsg  string
}

var (
	Success              = &ErrorCode{200, "Success"}
	NotModified          = &ErrorCode{304, "StatusNotModified"}
	InvalidArgument      = &ErrorCode{400, "InvalidArgument"}
	InvalidNamespace     = &ErrorCode{400, "InvalidNamespace"}
	InvalidOrigin        = &ErrorCode{400, "InvalidOrigin"}
	InvalidKey           = &ErrorCode{400, "InvalidKey"}
	InvalidFileSize      = &ErrorCode{400, "InvalidFileSize"}
	InvalidRegion        = &ErrorCode{400, "InvalidRegion"}
	InvalidRange         = &ErrorCode{400, "InvalidRange"}
	InvalidToken         = &ErrorCode{400, "InvalidToken"}
	InvalidDigest        = &ErrorCode{400, "InvalidDigest"}
	InvalidMimeType      = &ErrorCode{400, "InvalidMimeType"}
	NamespaceNotExist    = &ErrorCode{404, "NamespaceNotExist"}
	ResourceNotExist     = &ErrorCode{404, "ResourceNotExist"}
	InvalidMethod        = &ErrorCode{405, "HTTPMethodNotAllowed"}
	IOCopyFailure        = &ErrorCode{407, "IOCopyFailure"}
	ImgProcessing        = &ErrorCode{423, "ImgProcessing"}
	RequsetTooFrequently = &ErrorCode{429, "RequsetTooFrequently"}
	InternalError        = &ErrorCode{500, "InternalError"}
	FileSystemError      = &ErrorCode{500, "FileSystemError"}
	ImgProcessError      = &ErrorCode{501, "ImgProcessError"}
)
