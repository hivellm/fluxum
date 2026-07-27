// Published as its own module — the HiveLLM family convention for Go SDKs
// (hivellm/nexus-go, hivellm/synap-sdk-go): the monorepo's sdks/go tree is
// mirrored to the hivellm/fluxum-go repository and tagged there, so
// `go get github.com/hivellm/fluxum-go/fluxum` resolves without dragging
// the whole server repo through the module proxy.
module github.com/hivellm/fluxum-go

go 1.21
