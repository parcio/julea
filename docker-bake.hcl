target "docker-metadata-action" {}

variable "BASE_TAG" {
  default = "ghcr.io/finnhering/julea-prebuilt"
}




// Using ubuntu lts version
target "ci" {
     name="julea-ubuntu-${versions}-04"
     inherits = ["docker-metadata-action"]
     matrix={
         versions=["24", "22"]
     }
     args = {
         BASE_IMAGE = "ubuntu:${versions}.04"
     }
     tags = ["${BASE_TAG}:ubuntu-${versions}.04"]
     platforms=["linux/arm64", "linux/amd64"]
}



// Using image notation
// target "ci" {
//     name="julea-${regex_replace(base_images, "\\W", "-")}"
//     inherits = ["docker-metadata-action", "platforms"]
//     matrix={
//         base_images=["ubuntu:24.04", "ubuntu:22.04"]
//     }
//     args = {
//         BASE_IMAGE = "${base_images}"
//     }
//     tags = ["${BASE_TAG}:${replace(base_images, ":", "-")}"]
// }