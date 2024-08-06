package error

type ErrorCode int

const (
	MasterError ErrorCode = iota + 1
	WorkerError
	OtherError
)

const (
	SaveJobError ErrorCode = iota + MasterError*100
	DeleteJobError
	JobNoExitError
	KillJobError
)

const (
	GetLockFailed = iota + WorkerError*100
)

var ErrorMsg = map[ErrorCode]string{
	SaveJobError:   "save job failed",
	JobNoExitError: "job is not exit",
	KillJobError:   "job is not exit",
	GetLockFailed:  "get lock is fail",
}

type SystemError struct {
	code ErrorCode
	msg  string
}

func (systemError *SystemError) Error() string {
	return systemError.msg
}

func NewError(errorCode ErrorCode) *SystemError {
	if errorMsg, ok := ErrorMsg[errorCode]; ok {
		return &SystemError{
			code: errorCode,
			msg:  errorMsg,
		}
	} else {
		return &SystemError{
			code: errorCode,
			msg:  "unknow error",
		}
	}
}
