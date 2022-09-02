package halo

type Options struct {
	Handlers      map[string]JobHandler
	Weight        int
	Addr          string
	RetryCount    int
	RetryInterval int64
	CronMode      int
}

type Option func(opts *Options)

func loadOptions(options ...Option) *Options {
	opts := &Options{
		RetryCount:    3,
		RetryInterval: 120,
		Weight:        1,
		CronMode:      1,
	}
	for _, option := range options {
		option(opts)
	}
	return opts
}

func WtihDefaultOpts(addr string, handlers map[string]JobHandler) Option {
	return func(opts *Options) {
		opts.Handlers = handlers
		opts.Addr = addr
	}
}

func WtihCronOpts(cronMode int) Option {
	return func(opts *Options) {
		opts.CronMode = cronMode
	}
}

func WtihWeight(weight int) Option {
	return func(opts *Options) {
		if weight > 10 {
			weight = 10
		}
		opts.Weight = weight
	}
}

func WtihRetrySet(retryCount int, retryInterval int64) Option {
	return func(opts *Options) {
		opts.RetryCount = retryCount
		opts.RetryInterval = retryInterval
	}
}
