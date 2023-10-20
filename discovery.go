package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	rgtTypes "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/util/strutil"
)

const (
	rdsLabel                = model.MetaLabelPrefix + "rds_"
	rdsLabelAZ              = rdsLabel + "availability_zone"
	rdsLabelInstanceID      = rdsLabel + "instance_id"
	rdsLabelResourceID      = rdsLabel + "resource_id"
	rdsLabelClusterID       = rdsLabel + "cluster_id"
	rdsLabelInstanceState   = rdsLabel + "instance_state"
	rdsLabelInstanceType    = rdsLabel + "instance_type"
	rdsLabelRole            = rdsLabel + "role"
	rdsLabelEngine          = rdsLabel + "engine"
	rdsLabelEngineVersion   = rdsLabel + "engine_version"
	rdsLabelTag             = rdsLabel + "tag_"
	rdsLabelVPCID           = rdsLabel + "vpc_id"
	rdsLabelEndpointAddress = rdsLabel + "endpoint_address"
	rdsLabelEndpointPort    = rdsLabel + "endpoint_port"
)

type discovery struct {
	refreshInterval int
	logger          log.Logger
	filters         []types.Filter
}

func newDiscovery(conf sdConfig, logger log.Logger) (*discovery, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	d := &discovery{
		logger:          logger,
		refreshInterval: conf.RefreshInterval,
		filters:         conf.Filters,
	}

	return d, nil
}

func (d *discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	var accountId string
	for accountId == "" {
		var err error
		accountId, err = d.getAccountId(ctx)
		if err != nil {
			level.Error(d.logger).Log("msg", "could not get account id", "err", err)
			time.Sleep(time.Duration(d.refreshInterval) * time.Second)
			continue
		}
	}

	var region string
	for region == "" {
		var err error
		region, err = d.getDefaultRegion(ctx)
		if err != nil {
			level.Error(d.logger).Log("msg", "could not get default region", "err", err)
			time.Sleep(time.Duration(d.refreshInterval) * time.Second)
			continue
		}
	}
	for c := time.Tick(time.Duration(d.refreshInterval) * time.Second); ; {
		var tgs []*targetgroup.Group

		sdkConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			level.Error(d.logger).Log("msg", "could not load config", "err", err)
			time.Sleep(time.Duration(d.refreshInterval) * time.Second)
			continue
		}
		client := rds.NewFromConfig(sdkConfig)

		tagsIndexByArn, err := getTags(ctx, sdkConfig)
		if err != nil {
			level.Error(d.logger).Log("msg", "could not get tags", "err", err)
			time.Sleep(time.Duration(d.refreshInterval) * time.Second)
			continue
		}

		memberMap := make(map[string]types.DBClusterMember)
		paginator := rds.NewDescribeDBClustersPaginator(client, &rds.DescribeDBClustersInput{})
		for paginator.HasMorePages() {
			out, err := paginator.NextPage(ctx)
			if err != nil {
				level.Error(d.logger).Log("msg", "could not describe db cluster", "err", err)
				time.Sleep(time.Duration(d.refreshInterval) * time.Second)
				continue
			}
			for _, cluster := range out.DBClusters {
				for _, member := range cluster.DBClusterMembers {
					memberMap[*member.DBInstanceIdentifier] = member
				}
			}
		}

		input := &rds.DescribeDBInstancesInput{
			Filters: d.filters,
		}

		paginator2 := rds.NewDescribeDBInstancesPaginator(client, input)
		for paginator2.HasMorePages() {
			out, err := paginator2.NextPage(ctx)
			if err != nil {
				level.Error(d.logger).Log("msg", "could not describe db instance", "err", err)
				time.Sleep(time.Duration(d.refreshInterval) * time.Second)
				continue
			}
			for _, dbi := range out.DBInstances {
				if dbi.Endpoint.Address == nil {
					continue // instance is not ready
				}

				labels := model.LabelSet{
					rdsLabelInstanceID: model.LabelValue(*dbi.DBInstanceIdentifier),
				}

				labels[rdsLabelResourceID] = model.LabelValue(*dbi.DbiResourceId)
				labels[rdsLabelAZ] = model.LabelValue(*dbi.AvailabilityZone)
				labels[rdsLabelInstanceState] = model.LabelValue(*dbi.DBInstanceStatus)
				labels[rdsLabelInstanceType] = model.LabelValue(*dbi.DBInstanceClass)

				addr := net.JoinHostPort(*dbi.Endpoint.Address, strconv.FormatInt(int64(dbi.Endpoint.Port), 10))
				labels[model.AddressLabel] = model.LabelValue(addr)

				labels[rdsLabelEngine] = model.LabelValue(*dbi.Engine)
				labels[rdsLabelEngineVersion] = model.LabelValue(*dbi.EngineVersion)

				labels[rdsLabelVPCID] = model.LabelValue(*dbi.DBSubnetGroup.VpcId)

				labels[rdsLabelEndpointAddress] = model.LabelValue(*dbi.Endpoint.Address)
				labels[rdsLabelEndpointPort] = model.LabelValue(strconv.FormatInt(int64(dbi.Endpoint.Port), 10))

				switch *dbi.Engine {
				case "aurora":
					fallthrough
				case "aurora-mysql":
					labels[rdsLabelClusterID] = model.LabelValue(*dbi.DBClusterIdentifier)
					if member, ok := memberMap[*dbi.DBInstanceIdentifier]; ok {
						if member.IsClusterWriter {
							labels[rdsLabelRole] = model.LabelValue("writer")
						} else {
							labels[rdsLabelRole] = model.LabelValue("reader")
						}
					}
				case "mysql":
					if dbi.ReadReplicaSourceDBInstanceIdentifier == nil {
						labels[rdsLabelRole] = model.LabelValue("master")
					} else {
						labels[rdsLabelRole] = model.LabelValue("slave")
					}
				}

				arn := fmt.Sprintf("arn:aws:rds:%s:%s:db:%s", region, accountId, *dbi.DBInstanceIdentifier)
				for _, t := range tagsIndexByArn[arn] {
					if t.Key == nil || t.Value == nil {
						continue
					}

					name := strutil.SanitizeLabelName(*t.Key)
					labels[rdsLabelTag+model.LabelName(name)] = model.LabelValue(*t.Value)
				}

				tgs = append(tgs, &targetgroup.Group{
					Source:  *dbi.DBInstanceIdentifier,
					Targets: []model.LabelSet{{model.AddressLabel: labels[model.AddressLabel]}},
					Labels:  labels,
				})
			}
		}

		ch <- tgs

		select {
		case <-c:
			continue
		case <-ctx.Done():
			return
		}
	}
}

func getTags(ctx context.Context, sdkConfig aws.Config) (map[string][]rgtTypes.Tag, error) {
	tags := make(map[string][]rgtTypes.Tag)

	client := resourcegroupstaggingapi.NewFromConfig(sdkConfig)
	input := &resourcegroupstaggingapi.GetResourcesInput{
		ResourceTypeFilters: []string{"rds:db"},
		TagsPerPage:         aws.Int32(500),
	}
	paginator := resourcegroupstaggingapi.NewGetResourcesPaginator(client, input)
	for paginator.HasMorePages() {
		out, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, mapping := range out.ResourceTagMappingList {
			if _, ok := tags[*mapping.ResourceARN]; !ok {
				tags[*mapping.ResourceARN] = mapping.Tags
			} else {
				tags[*mapping.ResourceARN] = append(tags[*mapping.ResourceARN], mapping.Tags...)
			}
		}
	}

	return tags, nil
}

func (d *discovery) getAccountId(ctx context.Context) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRetryMaxAttempts(0))
	if err != nil {
		return "", err
	}

	client := sts.NewFromConfig(cfg)
	response, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}

	return *response.Account, nil
}

func (d *discovery) getDefaultRegion(ctx context.Context) (string, error) {
	var region string

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRetryMaxAttempts(0))
	if err != nil {
		level.Error(d.logger).Log("err", err)
		return "", err
	}

	client := imds.NewFromConfig(cfg)
	response, err := client.GetRegion(ctx, &imds.GetRegionInput{})
	if err != nil {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
	} else {
		region = response.Region
	}

	return region, nil
}
