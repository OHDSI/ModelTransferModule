createModelTransferModuleSpecifications <- function(
 s3Settings = NULL,
 githubSettings = NULL,
 localFileSettings = NULL
) {
  
  specifications <- list(
    module = "ModelTransferModule",
    version = "0.0.2",
    remoteRepo = "github.com",
    remoteUsername = "ohdsi",
    settings = list(
      s3Settings = s3Settings,
      githubSettings = githubSettings,
      localFileSettings =  localFileSettings
    )
  )
  class(specifications) <- c("ModelTransferModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}