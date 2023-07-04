// Code generated by Kitex v0.6.1. DO NOT EDIT.

package tdorisexternalservice

import (
	"context"
	client "github.com/cloudwego/kitex/client"
	callopt "github.com/cloudwego/kitex/client/callopt"
	dorisexternalservice "github.com/selectdb/ccr_syncer/rpc/kitex_gen/dorisexternalservice"
)

// Client is designed to provide IDL-compatible methods with call-option parameter for kitex framework.
type Client interface {
	OpenScanner(ctx context.Context, params *dorisexternalservice.TScanOpenParams, callOptions ...callopt.Option) (r *dorisexternalservice.TScanOpenResult_, err error)
	GetNext(ctx context.Context, params *dorisexternalservice.TScanNextBatchParams, callOptions ...callopt.Option) (r *dorisexternalservice.TScanBatchResult_, err error)
	CloseScanner(ctx context.Context, params *dorisexternalservice.TScanCloseParams, callOptions ...callopt.Option) (r *dorisexternalservice.TScanCloseResult_, err error)
}

// NewClient creates a client for the service defined in IDL.
func NewClient(destService string, opts ...client.Option) (Client, error) {
	var options []client.Option
	options = append(options, client.WithDestService(destService))

	options = append(options, opts...)

	kc, err := client.NewClient(serviceInfo(), options...)
	if err != nil {
		return nil, err
	}
	return &kTDorisExternalServiceClient{
		kClient: newServiceClient(kc),
	}, nil
}

// MustNewClient creates a client for the service defined in IDL. It panics if any error occurs.
func MustNewClient(destService string, opts ...client.Option) Client {
	kc, err := NewClient(destService, opts...)
	if err != nil {
		panic(err)
	}
	return kc
}

type kTDorisExternalServiceClient struct {
	*kClient
}

func (p *kTDorisExternalServiceClient) OpenScanner(ctx context.Context, params *dorisexternalservice.TScanOpenParams, callOptions ...callopt.Option) (r *dorisexternalservice.TScanOpenResult_, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.OpenScanner(ctx, params)
}

func (p *kTDorisExternalServiceClient) GetNext(ctx context.Context, params *dorisexternalservice.TScanNextBatchParams, callOptions ...callopt.Option) (r *dorisexternalservice.TScanBatchResult_, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.GetNext(ctx, params)
}

func (p *kTDorisExternalServiceClient) CloseScanner(ctx context.Context, params *dorisexternalservice.TScanCloseParams, callOptions ...callopt.Option) (r *dorisexternalservice.TScanCloseResult_, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.CloseScanner(ctx, params)
}
