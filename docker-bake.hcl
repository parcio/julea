target "base" { platforms = ["linux/amd64"] }

variable "BASE_IMAGE_NAME" { default = "ghcr.io/finnhering/julea" }

variable "COMMIT_SHA" { default = "UNKNOWN"}

group "ubuntu" { targets = ["ubuntu-spack", "ubuntu-system", "ubuntu-latest", "ubuntu-spack-dev-container", "ubuntu-system-dev-container", "ubuntu-dev-latest"] }

target "ubuntu-spack" {
  name = "julea-ubuntu-${versions}-04-spack-${compilers}"
  inherits = ["base"]
  matrix = {
    versions  = ["24", "22", "20"]
    compilers = ["gcc", "clang"]
  }
  args = {
    UBUNTU_VERSION = "${versions}.04"
    JULEA_SPACK_COMPILER = compilers
    CC = compilers
  }

  tags = ["${BASE_IMAGE_NAME}:${compilers}-spack-ubuntu-${versions}.04", "${BASE_IMAGE_NAME}:${compilers}-spack-ubuntu-${versions}.04-${COMMIT_SHA}"]
  target = "julea"
  dockerfile = "Dockerfile.spack"
  cache-from = [ "type=gha,scope=julea-ubuntu-${versions}-04-spack-${compilers}" ]
  cache-to = [ "type=gha,scope=julea-ubuntu-${versions}-04-spack-${compilers},mode=max" ]
}

# Target build docker images with prebuild julea + dependencies using system method
target "ubuntu-system" {
  name = "julea-ubuntu-${versions}-04-system-${compilers}"
  inherits = ["base"]
  matrix = {
    versions  = ["24", "22"]
    compilers = ["gcc", "clang"]
  }
  args = {
    UBUNTU_VERSION = "${versions}.04"
    CC = compilers
  }
  tags = ["${BASE_IMAGE_NAME}:${compilers}-system-ubuntu-${versions}.04", "${BASE_IMAGE_NAME}:${compilers}-system-ubuntu-${versions}.04-${COMMIT_SHA}"]
  target = "julea"
  dockerfile = "Dockerfile.system"
}

# Build latest image
target "ubuntu-latest" {
  inherits = ["base"]
  args = {
    UBUNTU_VERSION = "24.04"
    CC = "gcc"
  }
  tags = ["${BASE_IMAGE_NAME}:latest"]
  target = "julea"
  dockerfile = "Dockerfile.system"
}

target "ubuntu-spack-dev-container" {
  inherits = ["base"]
  name = "julea-dev-ubuntu-${versions}-04-spack-${compilers}"
  args = {
    UBUNTU_VERSION = "${versions}.04"
    CC = compilers
  }

  matrix = {
    versions  = ["24", "22", "20"]
    compilers = ["gcc", "clang"]
  }

  tags = ["${BASE_IMAGE_NAME}-dev:${compilers}-spack-ubuntu-${versions}.04", "${BASE_IMAGE_NAME}-dev:${compilers}-spack-ubuntu-${versions}.04-${COMMIT_SHA}"]
  target = "julea_dev"
  dockerfile = "Dockerfile.spack"

  cache-from = [ "type=gha,scope=julea-ubuntu-${versions}-04-spack-${compilers}" ]
  cache-to = [ "type=gha,scope=julea-ubuntu-${versions}-04-spack-${compilers},mode=max" ]
}


target "ubuntu-system-dev-container" {
  inherits = ["base"]
  name = "julea-dev-ubuntu-${versions}-04-system"
  args = {
    UBUNTU_VERSION = "${versions}.04"
  }

  matrix = {
    versions  = ["24", "22", "20"]
  }

  tags = ["${BASE_IMAGE_NAME}-dev:system-ubuntu-${versions}.04", "${BASE_IMAGE_NAME}-dev:system-ubuntu-${versions}.04-${COMMIT_SHA}"]
  target = "julea_dev"
  dockerfile = "Dockerfile.system"
}

target "ubuntu-dev-latest" {
  inherits = ["base"]
  args = {
    UBUNTU_VERSION = "24.04"
    CC = "gcc"
  }

  tags = ["${BASE_IMAGE_NAME}-dev:latest"]
  target = "julea_dev"
  dockerfile = "Dockerfile.spack"

  cache-from = [ "type=gha,scope=julea-ubuntu-24-04-spack-gcc" ]
  cache-to = [ "type=gha,scope=julea-ubuntu-24-04-spack-gcc,mode=max" ]
}