target "docker-metadata-action" {
  platforms = ["linux/amd64"]
}

variable "BASE_TAG" {
  default = "ghcr.io/finnhering/julea-prebuilt"
}

group "ubuntu" {
  targets = ["ubuntu-spack", "ubuntu-system"]
}

target "ubuntu-spack" {
  name     = "julea-ubuntu-spack-${compilers}-${versions}-04"
  inherits = ["docker-metadata-action"]
  matrix = {
    versions  = ["24", "22"]
    compilers = ["gcc", "clang"]
  }
  args = {
    UBUNTU_VERSION       = "${versions}.04"
    JULEA_SPACK_COMPILER = compilers
    CC                   = compilers
  }
  tags   = ["${BASE_TAG}:ubuntu-spack-${compilers}-${versions}.04"]
  target = "julea_spack"
}

target "ubuntu-system" {
  name     = "julea-ubuntu-system-${compilers}-${versions}-04"
  inherits = ["docker-metadata-action"]
  matrix = {
    versions  = ["24", "22"]
    compilers = ["gcc", "clang"]
  }
  args = {
    UBUNTU_VERSION       = "${versions}.04"
    JULEA_SPACK_COMPILER = compilers
    CC                   = compilers
  }
  tags   = ["${BASE_TAG}:ubuntu-system-${compilers}-${versions}.04"]
  target = "julea_system"
}