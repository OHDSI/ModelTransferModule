createModelTransferModuleSpecifications <- function(
 s3Settings = NULL,
 githubSettings = NULL
) {
  
  specifications <- list(
    module = "ModelTransferModule",
    version = "0.0.1",
    remoteRepo = "github.com",
    remoteUsername = "ohdsi",
    settings = list(
      s3Settings = s3Settings,
      githubSettings = githubSettings
    )
  )
  class(specifications) <- c("ModelTransferModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}