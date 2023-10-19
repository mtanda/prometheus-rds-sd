module github.com/rrreeeyyy/prometheus-rds-sd

go 1.14

require (
	github.com/aws/aws-sdk-go-v2 v1.21.2
	github.com/aws/aws-sdk-go-v2/config v1.19.0
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.13.13
	github.com/aws/aws-sdk-go-v2/service/rds v1.57.0
	github.com/go-kit/log v0.2.1
	github.com/prometheus/common v0.8.0
	github.com/prometheus/prometheus v1.8.2-0.20200213233353-b90be6f32a33
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
)
