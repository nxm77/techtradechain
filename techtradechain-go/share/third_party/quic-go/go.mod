module github.com/lucas-clemente/quic-go

go 1.15

require (
	github.com/cheekybits/genny v1.0.0
	github.com/francoispqt/gojay v1.2.13
	github.com/golang/mock v1.6.0
	github.com/marten-seemann/qpack v0.2.1
	github.com/marten-seemann/qtls-go1-16 v0.1.4
	github.com/marten-seemann/qtls-go1-17 v0.1.0
	github.com/marten-seemann/qtls-go1-18 v0.1.0
	github.com/marten-seemann/qtls-go1-19 v0.1.1
	github.com/onsi/ginkgo v1.16.4
	github.com/onsi/gomega v1.13.0
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a
)

replace (
	github.com/marten-seemann/qtls-go1-16 v0.1.4 => techtradechain.com/third_party/qtls-go1-16 v1.1.0
	github.com/marten-seemann/qtls-go1-17 v0.1.0 => techtradechain.com/third_party/qtls-go1-17 v1.1.0
	github.com/marten-seemann/qtls-go1-18 v0.1.0 => techtradechain.com/third_party/qtls-go1-18 v1.1.0
	github.com/marten-seemann/qtls-go1-19 v0.1.1 => techtradechain.com/third_party/qtls-go1-19 v1.0.0
)
