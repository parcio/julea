#!/bin/bash

docker buildx rm local_remote_builder || echo ""



echo "creating..."

docker buildx create --name local_remote_builder --node local_remote_build --platform linux/arm64,linux/riscv64,linux/ppc64le,linux/s390x,linux/mips64le,linux/mips64,linux/arm/v8,linux/arm/v7,linux/arm/v6  --driver-opt env.BUILDKIT_STEP_LOG_MAX_SIZE=10000000 --driver-opt env.BUILDKIT_STEP_LOG_MAX_SPEED=10000000

docker buildx create --name local_remote_builder --append --node intelarch --platform linux/amd64,linux/386 'ssh://finn@35.208.102.154' --driver-opt env.BUILDKIT_STEP_LOG_MAX_SIZE=10000000 --driver-opt env.BUILDKIT_STEP_LOG_MAX_SPEED=10000000
