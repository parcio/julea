target "docker-metadata-action" {
  platforms = ["linux/amd64"]
}

variable "BASE_TAG" {
  default = "ghcr.io/finnhering/julea"
}

variable "COMMIT_SHA" {
  default = "UNKNOWN"
}

group "ubuntu" {
  targets = ["ubuntu-spack", "ubuntu-system", "ubuntu-latest", "ubuntu-dev-container"]
}

# Target build docker images with prebuild julea + dependencies using spack method
target "ubuntu-spack" {
  name     = "julea-ubuntu-${versions}-04-spack-${compilers}"
  inherits = ["docker-metadata-action"]
  matrix = {
    versions  = ["24", "22", "20"]
    compilers = ["gcc", "clang"]
  }
  args = {
    UBUNTU_VERSION       = "${versions}.04"
    JULEA_SPACK_COMPILER = compilers
    CC                   = compilers
  }

  # Add -dependencies to the tag if the target is julea_dependencies
  tags       = ["${BASE_TAG}-spack:ubuntu-${versions}-04-${compilers}", "${BASE_TAG}-spack:ubuntu-${versions}-04-${compilers}-${COMMIT_SHA}"]
  target     = "julea"
  dockerfile = "Dockerfile.spack"
  cache-from = [
    "type=gha,scope=julea-ubuntu-${versions}-04-spack-${compilers}"
  ]
  cache-to = [
    "type=gha,scope=julea-ubuntu-${versions}-04-spack-${compilers},mode=max"
  ]
}

# Target build docker images with prebuild julea + dependencies using system method
target "ubuntu-system" {
  name     = "julea-ubuntu-${versions}-04-system-${compilers}"
  inherits = ["docker-metadata-action"]
  matrix = {
    versions  = ["24", "22"]
    compilers = ["gcc", "clang"]
  }
  args = {
    UBUNTU_VERSION = "${versions}.04"
    CC             = compilers
  }
  tags       = ["${BASE_TAG}-system:ubuntu-${versions}-04-${compilers}", "${BASE_TAG}-system:ubuntu-${versions}-04-${compilers}-${COMMIT_SHA}"]
  target     = "julea"
  dockerfile = "Dockerfile.system"
}

# Build latest image
target "ubuntu-latest" {
  inherits = ["docker-metadata-action"]
  args = {
    UBUNTU_VERSION = "24.04"
    CC             = "gcc"
  }
  tags = ["${BASE_TAG}:latest", "${BASE_TAG}:latest-${COMMIT_SHA}"]
  target     = "julea"
  dockerfile = "Dockerfile.system"
}

target "ubuntu-dev-container" {
  inherits = ["docker-metadata-action"]
  args = {
    UBUNTU_VERSION = "24.04"
    CC             = "gcc"
  }
  tags       = ["${BASE_TAG}-dev-container:latest", "${BASE_TAG}-dev-container:latest-${COMMIT_SHA}"]
  target     = "julea_dependencies"
  dockerfile = "Dockerfile.spack"

  cache-from = [
    "type=gha,scope=julea-ubuntu-24-04-spack-gcc"
  ]
  cache-to = [
    "type=gha,scope=julea-ubuntu-24-04-spack-gcc,mode=max"
  ]
}