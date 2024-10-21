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
  name     = "julea-ubuntu-${versions}-04-spack-${compilers}"
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
  tags   = ["${BASE_TAG}:ubuntu-${versions}-04-spack-${compilers}"]
  target = "julea_spack"
}

target "ubuntu-system" {
  name     = "julea-ubuntu-${versions}-04-system-${compilers}"
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
  tags   = ["${BASE_TAG}:ubuntu-${versions}-04-system-${compilers}"]
  target = "julea_system"
}