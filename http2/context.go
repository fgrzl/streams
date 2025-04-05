package http2

import "context"

type JWTKey string

const jwt JWTKey = "jwt"

func WithJWT(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, jwt, token)
}

func JWTFromContext(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(jwt).(string)
	return value, ok
}

type StreamKey string

const streamKey StreamKey = "stream-endpoint"

func WithStreamEndpoint(ctx context.Context, endpoint string) context.Context {
	return context.WithValue(ctx, streamKey, endpoint)
}

func StreamEndpointFromContext(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(streamKey).(string)
	return value, ok
}
