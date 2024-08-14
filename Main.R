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

ModelTransferModule <- R6::R6Class(
  classname = "ModelTransferModule",
  inherit = Strategus::StrategusModule,
  public = list(
    # no results produced/uploaded from this module
    tablePrefix = "",
    initialize = function() {
      super$initialize()
    },
    #' @description Executes the PatientLevelPrediction package
    #' @template connectionDetails
    #' @template analysisSpecifications
    #' @template executionSettings
    execute = function(connectionDetails, analysisSpecifications, executionSettings) {
      super$execute(connectionDetails, analysisSpecifications, executionSettings)
      checkmate::assertClass(executionSettings, "CdmExecutionSettings")

      private$.message("Transferring models")
      jobContext <- private$jobContext

      workFolder <- jobContext$moduleExecutionSettings$workSubFolder
      resultsFolder <- jobContext$moduleExecutionSettings$resultsSubFolder
      modelSaveLocation <-  resultsFolder

      s3Settings <- jobContext$settings$s3Settings
      githubSettings <- jobContext$settings$githubSettings
      localFileSettings <- jobContext$settings$localFileSettings

      modelLocationsS3 <- tryCatch(private$getModelsFromS3(
        settings = s3Settings,
        saveFolder = modelSaveLocation
      ), error = function(e) {
        ParallelLogger::logInfo(e)
        return(NULL)
      })
      if (!is.null(modelLocationsS3)) {
        readr::write_csv(modelLocationsS3,
          file = file.path(resultsFolder, "s3_export.csv")
        )
      }
      
      modelLocationsGithub <- tryCatch(
        {
          private$getModelsFromGithub(
            settings = githubSettings,
            saveFolder = modelSaveLocation
          )
        },
        error = function(e) {
          ParallelLogger::logInfo(e)
          return(NULL)
        }
      )
      if (!is.null(modelLocationsGithub)) {
        readr::write_csv(modelLocationsGithub,
          file = file.path(resultsFolder, "github_export.csv"))
      }
      
      modelLocationsLocalFiles <- tryCatch(
        {
          private$getModelsFromLocalFiles(
            settings = localFileSettings$locations,
            saveFolder = modelSaveLocation
          )
        },
        error = function(e) {
          ParallelLogger::logInfo(e)
          return(NULL)
        }
      )

      if (!is.null(modelLocationsS3)) {
        readr::write_csv(modelLocationsLocalFiles,
          file = file.path(resultsFolder, "local_export.csv"))
      }
    },
    createModuleSpecifications = function(settings) {
      settings <- super$createModuleSpecifications(settings)
      return(specifications)
    }
  ),
  private = list(
    getModelsFromS3 = function(settings, saveFolder) {

      if (is.null(settings)) {
        return(NULL)
      }

      info <- data.frame()

      for (i in seq_len(nrow(settings))) {
        modelSaved <- FALSE
        saveToLoc <- ""

        validBucket <- aws.s3::bucket_exists(
          bucket = settings$bucket[i],
          region = settings$region[i]
        )

        if (validBucket) {
          subfolder <- settings$modelZipLocation[i]
          bucket <- settings$bucket[i]
          region <- settings$region[i]

          result <- aws.s3::get_bucket_df(bucket = bucket, region = region, max = Inf)
          paths <- fs::path(result$Key)

          workDir <- findWorkDir(bucket, subfolder, region)
          analyses <- findAnalysesNames(bucket, workDir, region)

          if (length(analyses) > 0) {
            if (!fs::dir_exists(fs::path(saveFolder, "models"))) {
              dir.create(fs::path(saveFolder, "models"), recursive = TRUE)
            }
            saveToLoc <- fs::path(saveFolder, "models")

            for (analysis in analyses) {
              analysisPaths <- paths[fs::path_has_parent(paths,
                fs::path(workDir, analysis))]

              for (obj in analysisPaths) {
                # split work directory from path
                relativePaths <- fs::path_rel(obj, start = workDir)
                # remove artifacts created by current path location
                filteredPaths <- relativePaths[relativePaths != "."]
                # Construct the file path where you want to save the file locally
                localFilePath <- fs::path(saveToLoc, filteredPaths)

                # Download the file from S3
                aws.s3::save_object(obj, bucket, file = localFilePath)
              }
              ParallelLogger::logInfo(paste0("Downloaded: ", analysis, " to ", saveToLoc))
            }
          } else {
            ParallelLogger::logInfo(paste0("No ", settings$modelZipLocation[i],
              " in bucket ", settings$bucket[i], " in region ", settings$region[i]))
          }
        } else {
          ParallelLogger::logInfo(paste0("No bucket ", settings$bucket[i],
            " in region ", settings$region[i]))
        }

        info <- rbind(
          info,
          data.frame(
            originalLocation = "PLACEHOLDER",
            modelSavedLocally = TRUE,
            localLocation = saveToLoc
          )
        )
      }

      return(info)
    },
    getModelsFromGithub = function(settings, saveFolder) {
      if (is.null(settings)) {
        return(NULL)
      }

      info <- data.frame()
      for (i in seq_len(nrow(settings))) {
        user <- githabSettings[i, ]$user #' ohdsi-studies'
        repository <- githubSettings[i, ]$repository #' lungCancerPrognostic'
        ref <- githubSettings[i, ]$ref #' master'

        downloadedRepo <- tryCatch(
          {
            utils::download.file(
              url = file.path(
                "https://github.com", user, repository, "archive",
                paste0(ref, ".zip")
              ),
              destfile = file.path(tempdir(), "tempGitHub.zip")
            )
          },
          error = function(e) {
            ParallelLogger::logInfo("GitHub repository download failed, because of the following error: ", e)
            return(NULL)
          }
        )

        if (!is.null(downloadedRepo)) {
          # unzip into the workFolder
          OhdsiSharing::decompressFolder(
            sourceFileName = file.path(tempdir(), "tempGitHub.zip"),
            targetFolder = file.path(tempdir(), "tempGitHub")
          )
          for (j in 1:length(settings[i, ]$modelsFolder)) {
            modelsFolder <- settings[i, ]$modelsFolder[j] #' models'
            modelFolder <- if (is.null(settings[i, ]$modelFolder[j])) "" else settings[i, ]$modelFolder[j] #' full_model'

            tempModelLocation <- file.path(tempdir(),
              "tempGitHub",
              paste0(repository, "-", ref),
              "inst",
              modelsFolder,
              modelFolder
            )

            if (!dir.exists(file.path(saveFolder, "models",
              paste0("model_github_", repository, "_", ref)))) {
              dir.create(file.path(saveFolder, "models",
                paste0("model_github_", repository, "_", ref)), recursive = TRUE)
            }
            for (dirEntry in dir(tempModelLocation)) {
              file.copy(
                from = file.path(tempModelLocation, dirEntry),
                to = file.path(saveFolder, "models", paste0("model_github_", repository, "_", ref)), # issues if same modelFolder name in different github repos
                recursive = TRUE
              )
            }

            modelSaved <- TRUE
            saveToLoc <- file.path(saveFolder, "models", paste0("model_github_", repository, "_", ref))

            info <- rbind(
              info,
              data.frame(
                githubLocation = file.path(
                  "https://github.com", user,
                  repository, "archive", paste0(ref, ".zip")
                ),
                githubPath = file.path("inst", modelsFolder, modelFolder),
                modelSavedLocally = modelSaved,
                localLocation = saveToLoc
              )
            )
          }
        } else {
          info <- rbind(
            info,
            data.frame(
              githubLocation = file.path(
                "https://github.com", user, repository,
                "archive", paste0(ref, ".zip")
              ),
              githubPath = file.path("inst", settings[i, ]$modelsFolder,
                githubSettings[i, ]$modelFolder),
              modelSavedLocally = FALSE,
              localLocation = ""
            )
          )
        }
      }
      return(info)
    },
    getModelsFromLocalFiles = function(settings, saveFolder) {
      if(is.null(settings)){
      return(NULL)
      }
  
      if(!fs::dir_exists(fs::path(saveFolder, "models"))){
        dir.create(fs::path(saveFolder, "models"), recursive = TRUE)
      }
  
      saveFolder <- fs::path(saveFolder, "models")
      
      localFileSettings <- fs::path_expand(settings)
      saveFolder <- fs::path_expand(saveFolder)
      
      contents <- fs::dir_ls(settings)
  
      for(item in contents){
        # Determine the target path in the destination folder
        targetPath <- fs::path(saveFolder, fs::path_file(item))
        # Copy the item to the destination
        if (fs::dir_exists(item)) {
          dir.create(targetPath) # Ensure the directory exists before copying into it
          fs::dir_copy(item, targetPath)
        } else {
          fs::file_copy(item, targetPath)
        }
      }
      info <- data.frame()
      return(info)
    },
    findWorkDir <- function(bucket, subfolder, region) {
      # list all content in the bucket
      result <- aws.s3::get_bucket_df(bucket = bucket, region = region, max = Inf)
      # extract paths of all content
      paths <- fs::path(result$Key)
      # split paths up for easier processing
      splitPath <- fs::path_split(paths)

      # find the full path of the subfolder with models for validation
      resultsSapply <- sapply(splitPath, function(x) {
        identical(tail(x, 1), subfolder)
      })
      subfolderPath <- paths[resultsSapply]

      return(subfolderPath)
    },
    findAnalysesNames <- function(bucket, workDir, region) {
      # list all content in the bucket
      result <- aws.s3::get_bucket_df(bucket = bucket, region = region, max = Inf)
      # extract paths of all content
      paths <- fs::path(result$Key)
      # filter for paths in work directory
      workDirPaths <- paths[fs::path_has_parent(paths, workDir)]
      # split work directory from path
      relativePaths <- fs::path_rel(workDirPaths, start = workDir)
      # remove artifacts created by current path location
      filteredPaths <- relativePaths[relativePaths != "."]
      # get only the top level directories
      topLevelDirs <- sapply(fs::path_split(filteredPaths), function(p) p[[1]])
      topLevelDirs <- unique(topLevelDirs)
      return(topLevelDirs)
    }
  )
)
