package config

import (
	"context"
	"path/filepath"

	"github.com/golang/protobuf/proto"

	"chain/database/sinkdb"
	"chain/errors"
)

const prefix = `/core/config/`

var (
	ErrBadConfigOp    = errors.New("unsupported operation")
	ErrBadConfigKey   = errors.New("bad config key")
	ErrBadConfigValue = errors.New("bad config value")
)

type Options struct {
	sdb    *sinkdb.DB
	schema map[string]option
}

type option struct {
	multiple       bool
	parseInputFunc func([]string) (proto.Message, error)
}

func (opts *Options) Add(ctx context.Context, key string, val []string) (sinkdb.Op, error) {
	opt, ok := opts.schema[key]
	if !ok {
		return sinkdb.Op{}, errors.WithData(ErrBadConfigKey, "key", key)
	}
	if !opt.multiple {
		return sinkdb.Op{}, errors.WithDetailf(ErrBadConfigOp, "%q is a scalar config option. Use corectl set instead.", key)
	}

	v, err := opt.parseInputFunc(val)
	if err != nil {
		return sinkdb.Op{}, errors.Sub(err, ErrBadConfigValue)
	}
	b, err := proto.Marshal(v)
	if err != nil {
		return sinkdb.Op{}, err
	}

	// read-modify-write cycle
	var values OptionSet
	ver, err := opts.sdb.Get(ctx, filepath.Join(prefix, key), &values)
	if err != nil {
		return sinkdb.Op{}, err
	}
	values.Values = append(values.Values, b)
	return sinkdb.All(
		sinkdb.IfNotModified(ver),
		sinkdb.Set(filepath.Join(prefix, key), &values),
	), nil
}
