# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of ModelTransferModule
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Module methods -------------------------
getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}


# Module methods -------------------------
execute <- function(jobContext) {
  rlang::inform("Validating inputs")
  inherits(jobContext, 'list')

  if (is.null(jobContext$settings)) {
    stop("Analysis settings not found in job context")
  }
  if (is.null(jobContext$sharedResources)) {
    stop("Shared resources not found in job context")
  }
  if (is.null(jobContext$moduleExecutionSettings)) {
    stop("Execution settings not found in job context")
  }
  
  workFolder <- jobContext$moduleExecutionSettings$workSubFolder
  resultsFolder <- jobContext$moduleExecutionSettings$resultsSubFolder
  
  rlang::inform("Transfering models")
  moduleInfo <- getModuleInfo()
  
  # finding S3 details
  s3Settings <- jobContext$settings$s3Settings
  # finding github details
  githubSettings <- jobContext$settings$githubSettings
  
  modelLocationsS3 <- tryCatch({getModelsFromS3(
    s3Settings = s3Settings$locations,
    saveFolder = s3Settings$modelSaveFolder
  )}, error = function(e){ParallelLogger::logInfo(e); return(NULL)}
  )
  if(!is.null(modelLocationsS3)){
    readr::write_csv(modelLocationsS3, file = file.path(resultsFolder, 's3_export.csv'))
  }
  
  modelLocationsGithub <- tryCatch({getModelsFromGithub(
    githubSettings = githubSettings$locations,
    saveFolder = githubSettings$modelSaveFolder
  )}, error = function(e){ParallelLogger::logInfo(e); return(NULL)}
  )
  if(!is.null(modelLocationsGithub)){
    readr::write_csv(modelLocationsGithub, file = file.path(resultsFolder, 'github_export.csv'))
  }
  
}


# code that takes s3 details and download the models and returns the locations plus details as data.frame
getModelsFromS3 <- function(
  s3Settings,
  saveFolder
){
  
  # need to have settings
  # AWS_ACCESS_KEY_ID=<my access key id>
  # AWS_SECRET_ACCESS_KEY=<my secret key>
  #  AWS_DEFAULT_REGION=ap-southeast-2
  
  if(is.null(s3Settings)){
    return(NULL)
  }
  
  info <- data.frame()
  
  for(i in 1:nrow(s3Settings)){
    
    modelSaved <- F
    saveToLoc <- ''
    
    validBucket <- aws.s3::bucket_exists(
      bucket = s3Settings$bucket[i], 
      region = s3Settings$region[i]
    )
    
    if(validBucket){
      
      modelExists <- aws.s3::object_exists(
        object = s3Settings$modelZipLocation[i],
        bucket = s3Settings$bucket[i], 
        region = s3Settings$region[i]
      )
      
      if(modelExists){
        if(!dir.exists(file.path(saveFolder, "models"))){
          dir.create(file.path(saveFolder, "models"), recursive = T)
        }
        saveToLoc <- file.path(saveFolder, "models", paste0("model_",i,))
        
        # move the model to a local file
        aws.s3::save_object(
          object = s3Settings$modelZipLocation[i],
          bucket = s3Settings$bucket[i], 
          region = s3Settings$region[i],
          file = file.path(tempdir(), paste0('model',i,'.zip'))
        )
        modelSaved <- T
        
        # unzip into the workFolder
        OhdsiSharing::decompressFolder(
          sourceFileName = file.path(tempdir(), paste0('model',i,'.zip')), 
          targetFolder = saveToLoc 
          )
        
      } else{
        ParallelLogger::logInfo(paste0("No ",s3Settings$modelZipLocation[i]," in bucket ", s3Settings$bucket[i], " in region ", s3Settings$region[i] ))
      } 
    }else{
      ParallelLogger::logInfo(paste0("No bucket ", s3Settings$bucket[i] ," in region ", s3Settings$region[i]))
    }
    
    info <- rbind(
      info,
      data.frame(
        originalLocation = s3Settings$modelZipLocation[i], 
        modelSavedLocally = modelSaved, 
        localLocation = saveToLoc
      )
    )
    
  }
  
  return(info)
}

# code that takes github details and download the models and returns the locations plus details as data.frame
getModelsFromGithub <- function(
  githubSettings,
  saveFolder
){
  
  if(is.null(githubSettings)){
    return(NULL)
  }
  
  info <- data.frame()
  
  for(i in 1:nrow(githubSettings)){
    
    modelSaved <- F
    saveToLoc <- ''
    
    githubUser <- githubSettings$githubUser[i] #'ohdsi-studies'
    githubRepository <- githubSettings$githubRepository[1] #'lungCancerPrognostic'
    githubBranch <- githubSettings$githubBranch[i] #'master'
    githubModelsFolder <- githubSettings$githubModelsFolder[i]  #'models'
    githubModelFolder <- githubSettings$githubModelFolder[i] #'full_model'
    
    downloadCheck <- tryCatch({
      download.file(
        url = file.path("https://github.com",githubUser,githubRepository, "archive", paste0(githubBranch,".zip")),
        destfile = file.path(tempdir(), "tempGitHub.zip")
      )}, error = function(e){ ParallelLogger::logInfo('GitHub repository download failed') ; return(NULL)}
    )
    if(!is.null(downloadCheck)){
      # unzip into the workFolder
      OhdsiSharing::decompressFolder(
        sourceFileName = file.path(tempdir(), "tempGitHub.zip"), 
        targetFolder = file.path(tempdir(), "tempGitHub")
      )
      
      tempModelLocation <- file.path(file.path(tempdir(), "tempGitHub"), dir(file.path(file.path(tempdir(), "tempGitHub"))), 'inst', githubModelsFolder, githubModelFolder )
      
      if(!dir.exists(file.path(saveFolder,githubModelFolder))){
        dir.create(file.path(saveFolder, githubModelFolder), recursive = T)
      }
      for(dirEntry in dir(tempModelLocation)){
        file.copy(
          from = file.path(tempModelLocation, dirEntry), 
          to = file.path(saveFolder,githubModelFolder), 
          recursive = TRUE
        )
      }
      
      modelSaved <- T
      saveToLoc <- 'file.path(saveFolder,githubModelFolder)'
    }
    
    info <- rbind(
      info,
      data.frame(
        githubLocation = file.path("https://github.com",githubUser,githubRepository, "archive", paste0(githubBranch,".zip")),
        githubPath = file.path('inst', githubModelsFolder, githubModelFolder),
        modelSavedLocally = modelSaved, 
        localLocation = saveToLoc
      )
    )
    
  }

  return(info)
}